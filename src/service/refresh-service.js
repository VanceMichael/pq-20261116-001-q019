/**
 * Backwards-compatible synchronous refresh. The manual refresh path now runs
 * through the durable refresh-task pipeline: it submits (or attaches to) the
 * feed's active task, waits for the terminal state and maps it to the result
 * shape the original RefreshService returned. Callers keep their contract
 * while gaining crash recovery and cross-request deduplication.
 */
export class RefreshService {
  constructor({ feeds, fetches, captures, articles, tasks }) {
    this.feeds = feeds;
    this.fetches = fetches;
    this.captures = captures;
    this.articles = articles;
    this.tasks = tasks;
    this.inFlight = new Map();
  }

  refresh(feedId) {
    const active = this.inFlight.get(feedId);
    if (active) return active;
    const task = this.perform(feedId).finally(() => {
      if (this.inFlight.get(feedId) === task) this.inFlight.delete(feedId);
    });
    this.inFlight.set(feedId, task);
    return task;
  }

  async perform(feedId) {
    const feed = this.feeds.get(feedId);
    if (!feed) throw new Error(`unknown feed: ${feedId}`);
    const { task } = this.tasks.submit(feedId, {});
    const terminal = await this.tasks.awaitTask(task.taskId);
    return this.toLegacyResult(terminal);
  }

  toLegacyResult(task) {
    if (task.status === "failed") {
      throw Object.assign(new Error(task.errorMessage ?? "refresh failed"), {
        code: task.errorCode,
        status: task.httpStatus ?? undefined,
        attemptCount: task.attemptCount ?? undefined,
        attemptId: task.attemptId,
        statusCode: 502
      });
    }
    if (task.outcome === "not_modified") {
      return { attemptId: task.attemptId, feedId: task.feedId, outcome: "not_modified", httpStatus: 304, capture: null };
    }
    const stored = task.captureId != null ? this.captures.get(task.captureId) : null;
    const assessment = task.assessment ?? {};
    const capture = {
      captureId: task.captureId,
      feedId: task.feedId,
      fingerprint: stored?.fingerprint ?? assessment.fingerprint ?? null,
      capturedAt: stored?.capturedAt ?? assessment.capturedAt ?? null,
      accepted: task.outcome === "accepted",
      replay: Boolean(task.captureReplay),
      candidateCount: stored?.candidateCount ?? assessment.candidateCount ?? null,
      normalizedCount: stored?.normalizedCount ?? assessment.normalizedCount ?? null,
      rejectedCount: stored?.rejectedCount ?? assessment.rejectedCount ?? null,
      duplicateCount: stored?.duplicateCount ?? assessment.duplicateCount ?? null,
      reason: stored?.reason ?? assessment.reason ?? null,
      rejected: assessment.rejected ?? [],
      stats: this.articles.stats(task.feedId)
    };
    return {
      attemptId: task.attemptId,
      feedId: task.feedId,
      outcome: task.outcome,
      httpStatus: task.httpStatus,
      capture
    };
  }

  recent(feedId, limit) {
    if (!this.feeds.get(feedId)) return null;
    return this.fetches.recent(feedId, limit);
  }

  pending(feedId) {
    return this.inFlight.has(feedId) || this.tasks.activeForFeed(feedId) != null;
  }
}
