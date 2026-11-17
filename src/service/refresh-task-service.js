import { extractLinks, pageFingerprint } from "../feed/html.js";
import { normalizeCandidates } from "../feed/normalize.js";
import { assessCapture } from "../ingest/capture-policy.js";

/**
 * Error thrown by test hooks to simulate an abrupt process shutdown (power
 * loss, kill -9) at a durable checkpoint. The task runner treats it as "the
 * process died": it aborts execution immediately without touching task state,
 * exactly what a real crash would leave behind. A later process recovers the
 * task from the persisted stage pointer.
 */
export class SimulatedShutdown extends Error {
  constructor(message = "simulated process shutdown") {
    super(message);
    this.name = "SimulatedShutdown";
    this.simulatedShutdown = true;
  }
}

/**
 * Durable, resumable refresh pipeline. A task advances through the stages
 * fetch -> assess -> archive -> done, and every stage commits its output to
 * the local database before the stage pointer moves on:
 *
 * - fetch:   the full HTTP response (HTML, status, URLs) is stored on the
 *            task row. After a restart the stored page is reused as-is; the
 *            service never re-downloads a page that may have changed and
 *            passes it off as the original response.
 * - assess:  the quality assessment (counts, accept/reject advice) computed
 *            from the stored page is persisted for progress reporting.
 * - archive: the stored page is committed through ImportService.importHtml,
 *            which is idempotent per page fingerprint. The task then links
 *            the resulting capture and records its fetch attempt audit row.
 *
 * The archive commit and the task bookkeeping are separate durable writes, so
 * a crash can leave a committed capture behind while the task still reads
 * "running". Recovery reconciles that state by linking the already-archived
 * capture instead of importing the page again, which keeps article
 * observation counts exact.
 */
export class RefreshTaskService {
  constructor({ database, feeds, fetches, captures, imports, tasks, fetcher,
    clock = () => new Date(), hooks = {}, logger = console }) {
    this.database = database;
    this.feeds = feeds;
    this.fetches = fetches;
    this.captures = captures;
    this.imports = imports;
    this.tasks = tasks;
    this.fetcher = fetcher;
    this.clock = clock;
    this.hooks = hooks ?? {};
    this.logger = logger;
    this.executing = new Map();
    this.waiters = new Map();
  }

  submit(feedId, { taskKey = null } = {}) {
    const feed = this.feeds.get(feedId);
    if (!feed) throw new Error(`unknown feed: ${feedId}`);
    if (taskKey !== null && taskKey !== undefined) {
      if (typeof taskKey !== "string" || taskKey.length === 0 || taskKey.length > 100) {
        throw new TypeError("taskKey must be a string of 1 to 100 characters");
      }
      const existing = this.tasks.getByKey(feedId, taskKey);
      if (existing) return { task: existing, deduplicated: true };
    } else {
      taskKey = null;
      const active = this.tasks.activeForFeed(feedId);
      if (active) return { task: active, deduplicated: true };
    }
    const now = this.clock().toISOString();
    let taskId;
    try {
      taskId = this.tasks.create({ feedId, taskKey, now });
    } catch (error) {
      if (!/UNIQUE constraint failed/.test(error.message)) throw error;
      const existing = taskKey ? this.tasks.getByKey(feedId, taskKey) : this.tasks.activeForFeed(feedId);
      if (existing) return { task: existing, deduplicated: true };
      throw error;
    }
    this.launch(taskId);
    return { task: this.tasks.get(taskId), deduplicated: false };
  }

  get(taskId) {
    return this.tasks.get(taskId);
  }

  listForFeed(feedId, limit) {
    return this.tasks.listForFeed(feedId, limit);
  }

  awaitTask(taskId) {
    const current = this.tasks.get(taskId);
    if (!current) return Promise.resolve(null);
    if (current.status !== "running") return Promise.resolve(current);
    let waiters = this.waiters.get(taskId);
    if (!waiters) {
      waiters = [];
      this.waiters.set(taskId, waiters);
    }
    return new Promise((resolve) => {
      waiters.push(resolve);
    });
  }

  recoverInterrupted() {
    for (const task of this.tasks.interrupted()) this.launch(task.taskId);
    return this.whenIdle();
  }

  whenIdle() {
    return Promise.all([...this.executing.values()]).then(() => undefined);
  }

  launch(taskId) {
    const active = this.executing.get(taskId);
    if (active) return active;
    const promise = this.runStages(taskId)
      .catch((error) => {
        if (!error?.simulatedShutdown) {
          this.logger.error?.({ taskId, error: error?.message ?? String(error) });
        }
      })
      .finally(() => {
        if (this.executing.get(taskId) === promise) this.executing.delete(taskId);
      });
    this.executing.set(taskId, promise);
    return promise;
  }

  async runStages(taskId) {
    while (true) {
      const task = this.tasks.getForExecution(taskId);
      if (!task || task.status !== "running") return;
      if (task.stage === "fetch") await this.runFetchStage(task);
      else if (task.stage === "assess") await this.runAssessStage(task);
      else if (task.stage === "archive") await this.runArchiveStage(task);
      else return;
    }
  }

  async runFetchStage(task) {
    const feed = this.feeds.get(task.feedId);
    if (!feed) {
      this.tasks.fail(task.taskId, {
        errorCode: "feed_missing", errorMessage: `unknown feed: ${task.feedId}`,
        httpStatus: null, attemptCount: 0, attemptId: null, now: this.clock().toISOString()
      });
      this.notify(task.taskId);
      return;
    }
    const startedAt = this.clock().toISOString();
    this.tasks.markFetchStarted(task.taskId, startedAt);
    let fetched;
    try {
      fetched = await this.fetcher.fetchFeed(feed);
    } catch (error) {
      if (error?.simulatedShutdown) throw error;
      this.recordFailure(task, error, {
        startedAt,
        httpStatus: error.status ?? null,
        attemptCount: error.attemptCount ?? 1,
        requestedUrl: feed.baseUrl,
        finalUrl: null
      });
      return;
    }
    const finishedAt = this.clock().toISOString();
    if (fetched.status === 304) {
      this.completeNotModified(task, fetched, startedAt, finishedAt);
    } else {
      this.tasks.saveFetchedPage(task.taskId, {
        httpStatus: fetched.status,
        attemptCount: fetched.attemptCount ?? 1,
        requestedUrl: fetched.requestedUrl ?? feed.baseUrl,
        finalUrl: fetched.finalUrl ?? null,
        html: fetched.html ?? "",
        finishedAt
      });
    }
    await this.checkpoint("fetch", task.taskId);
  }

  async runAssessStage(task) {
    try {
      const feed = this.feeds.get(task.feedId);
      const capturedAt = task.fetchFinishedAt;
      const candidates = extractLinks(task.fetchedHtml, feed.baseUrl);
      const normalized = normalizeCandidates(candidates, {
        baseUrl: feed.baseUrl,
        allowedHosts: feed.allowedHosts
      });
      const policy = assessCapture({
        candidateCount: candidates.length,
        normalizedCount: normalized.records.length,
        rejectedCount: normalized.rejected.length,
        previousCount: feed.lastItemCount,
        capturedAt,
        feed
      });
      const assessment = {
        capturedAt,
        fingerprint: pageFingerprint(task.fetchedHtml),
        accepted: policy.accepted,
        reason: policy.reason,
        candidateCount: candidates.length,
        normalizedCount: normalized.records.length,
        rejectedCount: normalized.rejected.length,
        duplicateCount: normalized.duplicateCount,
        rejected: normalized.rejected.slice(0, 100)
      };
      this.tasks.saveAssessment(task.taskId, JSON.stringify(assessment), this.clock().toISOString());
    } catch (error) {
      if (error?.simulatedShutdown) throw error;
      this.recordFailure(task, error, {
        startedAt: task.fetchStartedAt,
        httpStatus: task.httpStatus,
        attemptCount: task.attemptCount ?? 1,
        requestedUrl: task.requestedUrl,
        finalUrl: task.finalUrl
      });
      return;
    }
    await this.checkpoint("assess", task.taskId);
  }

  async runArchiveStage(task) {
    let captureId = task.captureId;
    if (captureId == null) {
      let result;
      try {
        result = this.imports.importHtml(task.feedId, task.fetchedHtml, task.assessment.capturedAt);
      } catch (error) {
        if (error?.simulatedShutdown) throw error;
        this.recordFailure(task, error, {
          startedAt: task.fetchStartedAt,
          httpStatus: task.httpStatus,
          attemptCount: task.attemptCount ?? 1,
          requestedUrl: task.requestedUrl,
          finalUrl: task.finalUrl
        });
        return;
      }
      captureId = result.captureId;
      await this.checkpoint("archive.import", task.taskId);
      this.tasks.linkCapture(task.taskId, captureId, result.replay, this.clock().toISOString());
      await this.checkpoint("archive.capture", task.taskId);
    }
    const capture = this.captures.get(captureId);
    if (!capture) throw new Error(`linked capture is missing: ${captureId}`);
    if (task.attemptId == null) {
      const now = this.clock().toISOString();
      this.database.exec("BEGIN IMMEDIATE");
      let attemptId;
      try {
        attemptId = this.fetches.record({
          feedId: task.feedId,
          startedAt: task.fetchStartedAt,
          finishedAt: task.fetchFinishedAt ?? now,
          outcome: capture.accepted ? "accepted" : "rejected",
          httpStatus: task.httpStatus,
          attemptCount: task.attemptCount ?? 1,
          requestedUrl: task.requestedUrl,
          finalUrl: task.finalUrl,
          captureId,
          errorCode: capture.accepted ? null : "capture_rejected",
          errorMessage: capture.accepted ? null : capture.reason,
          taskId: task.taskId
        });
        this.tasks.linkAttempt(task.taskId, attemptId, now);
        this.database.exec("COMMIT");
      } catch (error) {
        this.database.exec("ROLLBACK");
        throw error;
      }
      await this.checkpoint("archive.attempt", task.taskId);
    }
    this.tasks.complete(task.taskId, {
      outcome: capture.accepted ? "accepted" : "rejected",
      now: this.clock().toISOString()
    });
    this.notify(task.taskId);
  }

  completeNotModified(task, fetched, startedAt, finishedAt) {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const attemptId = this.fetches.record({
        feedId: task.feedId,
        startedAt,
        finishedAt,
        outcome: "not_modified",
        httpStatus: 304,
        attemptCount: fetched.attemptCount ?? 1,
        requestedUrl: fetched.requestedUrl,
        finalUrl: fetched.finalUrl ?? null,
        captureId: null,
        errorCode: null,
        errorMessage: null,
        taskId: task.taskId
      });
      this.tasks.completeNotModified(task.taskId, {
        httpStatus: 304,
        attemptCount: fetched.attemptCount ?? 1,
        requestedUrl: fetched.requestedUrl,
        finalUrl: fetched.finalUrl ?? null,
        attemptId,
        now: finishedAt
      });
      this.database.exec("COMMIT");
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
    this.notify(task.taskId);
  }

  recordFailure(task, error, { startedAt, httpStatus, attemptCount, requestedUrl, finalUrl }) {
    const now = this.clock().toISOString();
    const errorCode = error.code ?? "refresh_failed";
    const errorMessage = (error.message ?? "refresh failed").slice(0, 500);
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const attemptId = this.fetches.record({
        feedId: task.feedId,
        startedAt: startedAt ?? now,
        finishedAt: now,
        outcome: "failed",
        httpStatus,
        attemptCount,
        requestedUrl: requestedUrl ?? task.requestedUrl ?? "",
        finalUrl: finalUrl ?? null,
        captureId: null,
        errorCode,
        errorMessage,
        taskId: task.taskId
      });
      this.tasks.fail(task.taskId, {
        errorCode, errorMessage, httpStatus, attemptCount, attemptId, now
      });
      this.database.exec("COMMIT");
    } catch (failure) {
      this.database.exec("ROLLBACK");
      throw failure;
    }
    this.notify(task.taskId);
  }

  async checkpoint(name, taskId) {
    await this.hooks.afterCheckpoint?.(name, taskId, this);
  }

  notify(taskId) {
    const waiters = this.waiters.get(taskId);
    if (!waiters) return;
    this.waiters.delete(taskId);
    const task = this.tasks.get(taskId);
    for (const resolve of waiters) resolve(task);
  }
}
