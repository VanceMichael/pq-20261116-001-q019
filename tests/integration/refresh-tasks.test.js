import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { NewsArchive } from "../../src/service/archive.js";
import { SimulatedShutdown } from "../../src/service/refresh-task-service.js";
import { createHttpServer } from "../../src/http/server.js";
import { makeArchive, page, standardItems, baseFeed } from "../helpers.js";

function cannedFetcher(result) {
  let calls = 0;
  return {
    async fetchFeed(feed) {
      calls += 1;
      if (typeof result === "function") return result(feed, calls);
      return { ...result, requestedUrl: feed.baseUrl, finalUrl: feed.baseUrl, attemptCount: 1 };
    },
    count: () => calls
  };
}

function manualClock(iso = "2026-11-16T02:00:00.000Z") {
  let current = Date.parse(iso);
  return {
    clock: () => new Date(current),
    advance(ms = 1000) {
      current += ms;
    }
  };
}

async function makeHarness(t) {
  const directory = await mkdtemp(join(tmpdir(), "news-archive-tasks-"));
  const filename = join(directory, "archive.sqlite");
  const archives = [];
  t.after(async () => {
    for (const archive of archives.splice(0)) {
      try {
        archive.close();
      } catch {
        // already closed by the test
      }
    }
    await rm(directory, { recursive: true, force: true });
  });
  return {
    filename,
    open(options = {}) {
      const archive = new NewsArchive(filename, options);
      archives.push(archive);
      return archive;
    }
  };
}

test("runs a background task through fetch, assess and archive with linked audit records", async (t) => {
  const time = manualClock();
  const html = page(standardItems(4));
  const fetcher = cannedFetcher({ status: 200, html });
  const { archive } = await makeArchive(t, { fetcher, clock: time.clock });
  archive.registerFeed(baseFeed);
  const { task, deduplicated } = archive.submitRefreshTask("daily-main", {});
  assert.equal(deduplicated, false);
  assert.equal(task.status, "running");
  const terminal = await archive.refreshTasks.awaitTask(task.taskId);
  assert.equal(terminal.status, "completed");
  assert.equal(terminal.stage, "done");
  assert.equal(terminal.outcome, "accepted");
  assert.equal(terminal.httpStatus, 200);
  assert.equal(terminal.fetchedBytes, Buffer.byteLength(html, "utf8"));
  assert.equal(terminal.fetchedHtml, undefined);
  assert.equal(terminal.assessment.accepted, true);
  assert.equal(terminal.assessment.normalizedCount, 4);
  assert.ok(terminal.captureId);
  assert.ok(terminal.attemptId);
  assert.equal(terminal.captureReplay, false);
  assert.ok(terminal.fetchStartedAt <= terminal.fetchFinishedAt);
  assert.ok(terminal.finishedAt);
  const [attempt] = archive.listFetchAttempts("daily-main");
  assert.equal(attempt.taskId, task.taskId);
  assert.equal(attempt.captureId, terminal.captureId);
  assert.equal(attempt.outcome, "accepted");
  const [capture] = archive.listCaptures("daily-main");
  assert.equal(capture.captureId, terminal.captureId);
  assert.equal(capture.accepted, true);
  assert.equal(archive.articles.stats("daily-main").total, 4);
  assert.equal(archive.listRefreshTasks("daily-main").length, 1);
  assert.equal(archive.getRefreshTask("daily-main", task.taskId).outcome, "accepted");
  assert.equal(archive.auditIntegrity().status, "consistent");
});

test("returns the same logical task for repeated submissions with or without a key", async (t) => {
  const time = manualClock();
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const fetcher = cannedFetcher(async (feed) => {
    await gate;
    return { status: 200, html: page(standardItems(4)), requestedUrl: feed.baseUrl, finalUrl: feed.baseUrl, attemptCount: 1 };
  });
  const { archive } = await makeArchive(t, { fetcher, clock: time.clock });
  archive.registerFeed(baseFeed);
  const first = archive.submitRefreshTask("daily-main", { taskKey: "op-1" });
  const keyedReplay = archive.submitRefreshTask("daily-main", { taskKey: "op-1" });
  const anonymous = archive.submitRefreshTask("daily-main", {});
  assert.equal(first.deduplicated, false);
  assert.equal(keyedReplay.deduplicated, true);
  assert.equal(anonymous.deduplicated, true);
  assert.equal(keyedReplay.task.taskId, first.task.taskId);
  assert.equal(anonymous.task.taskId, first.task.taskId);
  assert.equal(fetcher.count(), 1);
  release();
  await archive.whenIdle();
  const afterCompletion = archive.submitRefreshTask("daily-main", { taskKey: "op-1" });
  assert.equal(afterCompletion.deduplicated, true);
  assert.equal(afterCompletion.task.taskId, first.task.taskId);
  assert.equal(afterCompletion.task.status, "completed");
  assert.equal(fetcher.count(), 1);
  time.advance();
  const fresh = archive.submitRefreshTask("daily-main", {});
  assert.equal(fresh.deduplicated, false);
  assert.notEqual(fresh.task.taskId, first.task.taskId);
  await archive.whenIdle();
  assert.equal(fetcher.count(), 2);
  assert.throws(() => archive.submitRefreshTask("daily-main", { taskKey: "" }), TypeError);
  assert.throws(() => archive.submitRefreshTask("unknown-feed", {}), /unknown feed/);
});

test("keeps the task key mapping across a restart instead of starting a new fetch", async (t) => {
  const harness = await makeHarness(t);
  const time = manualClock();
  const first = harness.open({ fetcher: cannedFetcher({ status: 200, html: page(standardItems(4)) }), clock: time.clock });
  first.registerFeed(baseFeed);
  const submitted = first.submitRefreshTask("daily-main", { taskKey: "nightly-7" });
  await first.whenIdle();
  assert.equal(first.getRefreshTask("daily-main", submitted.task.taskId).status, "completed");
  first.close();
  const second = harness.open({
    fetcher: cannedFetcher(() => {
      throw new Error("must not be called");
    }),
    clock: time.clock
  });
  await second.recovery;
  const replayed = second.submitRefreshTask("daily-main", { taskKey: "nightly-7" });
  assert.equal(replayed.deduplicated, true);
  assert.equal(replayed.task.taskId, submitted.task.taskId);
  assert.equal(replayed.task.status, "completed");
  assert.equal(second.listFetchAttempts("daily-main").length, 1);
});

for (const crashAt of ["fetch", "assess"]) {
  test(`recovers the stored page after a shutdown at ${crashAt} without re-fetching a changed source`, async (t) => {
    const harness = await makeHarness(t);
    const time = manualClock();
    const originalHtml = page(standardItems(5));
    const hooks = {
      afterCheckpoint(name) {
        if (name === crashAt) throw new SimulatedShutdown();
      }
    };
    const first = harness.open({
      fetcher: cannedFetcher({ status: 200, html: originalHtml }),
      taskHooks: hooks,
      clock: time.clock
    });
    first.registerFeed(baseFeed);
    const { task } = first.submitRefreshTask("daily-main", {});
    await first.whenIdle();
    const interrupted = first.getRefreshTask("daily-main", task.taskId);
    assert.equal(interrupted.status, "running");
    assert.equal(interrupted.captureId, null);
    assert.ok(interrupted.fetchedBytes > 0);
    assert.equal(first.listCaptures("daily-main").length, 0);
    assert.equal(first.articles.stats("daily-main").total, 0);
    first.close();

    const changedFetcher = cannedFetcher({ status: 200, html: page(standardItems(3, 90)) });
    const second = harness.open({ fetcher: changedFetcher, clock: time.clock });
    await second.recovery;
    assert.equal(changedFetcher.count(), 0);
    const recovered = second.getRefreshTask("daily-main", task.taskId);
    assert.equal(recovered.status, "completed");
    assert.equal(recovered.outcome, "accepted");
    const items = second.listItems("daily-main").items;
    assert.deepEqual(items.map((item) => item.title).sort(), ["标题1", "标题2", "标题3", "标题4", "标题5"]);
    assert.ok(items.every((item) => item.seenCount === 1));
    assert.equal(second.listCaptures("daily-main").length, 1);
    const [attempt] = second.listFetchAttempts("daily-main");
    assert.equal(attempt.taskId, task.taskId);
    assert.equal(attempt.captureId, recovered.captureId);
    assert.equal(second.auditIntegrity().status, "consistent");
  });
}

test("links the already-committed capture after a shutdown between archive commit and task bookkeeping", async (t) => {
  const harness = await makeHarness(t);
  const time = manualClock();
  const html = page(standardItems(4));
  const hooks = {
    afterCheckpoint(name) {
      if (name === "archive.import") throw new SimulatedShutdown();
    }
  };
  const first = harness.open({
    fetcher: cannedFetcher({ status: 200, html }),
    taskHooks: hooks,
    clock: time.clock
  });
  first.registerFeed(baseFeed);
  const { task } = first.submitRefreshTask("daily-main", {});
  await first.whenIdle();
  const [capture] = first.listCaptures("daily-main");
  assert.equal(capture.accepted, true);
  assert.equal(first.articles.stats("daily-main").total, 4);
  assert.ok(first.listItems("daily-main").items.every((item) => item.seenCount === 1));
  const interrupted = first.getRefreshTask("daily-main", task.taskId);
  assert.equal(interrupted.status, "running");
  assert.equal(interrupted.stage, "archive");
  assert.equal(interrupted.captureId, null);
  first.close();

  const second = harness.open({
    fetcher: cannedFetcher({ status: 200, html: page(standardItems(3, 90)) }),
    clock: time.clock
  });
  await second.recovery;
  const recovered = second.getRefreshTask("daily-main", task.taskId);
  assert.equal(recovered.status, "completed");
  assert.equal(recovered.outcome, "accepted");
  assert.equal(recovered.captureId, capture.captureId);
  assert.equal(recovered.captureReplay, true);
  assert.equal(second.listCaptures("daily-main").length, 1);
  assert.ok(second.listItems("daily-main").items.every((item) => item.seenCount === 1));
  assert.equal(second.listFetchAttempts("daily-main").length, 1);
  assert.equal(second.auditIntegrity().status, "consistent");
});

test("writes only the missing completion marker after a shutdown that follows the attempt audit", async (t) => {
  const harness = await makeHarness(t);
  const time = manualClock();
  const hooks = {
    afterCheckpoint(name) {
      if (name === "archive.attempt") throw new SimulatedShutdown();
    }
  };
  const first = harness.open({
    fetcher: cannedFetcher({ status: 200, html: page(standardItems(4)) }),
    taskHooks: hooks,
    clock: time.clock
  });
  first.registerFeed(baseFeed);
  const { task } = first.submitRefreshTask("daily-main", {});
  await first.whenIdle();
  const interrupted = first.getRefreshTask("daily-main", task.taskId);
  assert.equal(interrupted.status, "running");
  assert.ok(interrupted.captureId);
  assert.ok(interrupted.attemptId);
  assert.equal(first.listFetchAttempts("daily-main").length, 1);
  first.close();

  const second = harness.open({
    fetcher: cannedFetcher({ status: 200, html: page(standardItems(3, 90)) }),
    clock: time.clock
  });
  await second.recovery;
  const recovered = second.getRefreshTask("daily-main", task.taskId);
  assert.equal(recovered.status, "completed");
  assert.equal(recovered.outcome, "accepted");
  assert.equal(recovered.attemptId, interrupted.attemptId);
  assert.equal(recovered.captureId, interrupted.captureId);
  assert.equal(second.listFetchAttempts("daily-main").length, 1);
  assert.equal(second.listCaptures("daily-main").length, 1);
  assert.ok(second.listItems("daily-main").items.every((item) => item.seenCount === 1));
  assert.equal(second.auditIntegrity().status, "consistent");
});

test("re-fetches only when the shutdown happened before any response was stored", async (t) => {
  const harness = await makeHarness(t);
  const time = manualClock();
  const dyingFetcher = {
    calls: 0,
    async fetchFeed() {
      dyingFetcher.calls += 1;
      throw new SimulatedShutdown();
    }
  };
  const first = harness.open({ fetcher: dyingFetcher, clock: time.clock });
  first.registerFeed(baseFeed);
  const { task } = first.submitRefreshTask("daily-main", {});
  await first.whenIdle();
  const interrupted = first.getRefreshTask("daily-main", task.taskId);
  assert.equal(interrupted.status, "running");
  assert.equal(interrupted.stage, "fetch");
  assert.equal(interrupted.fetchedBytes, null);
  first.close();

  const goodFetcher = cannedFetcher({ status: 200, html: page(standardItems(4)) });
  const second = harness.open({ fetcher: goodFetcher, clock: time.clock });
  await second.recovery;
  assert.equal(goodFetcher.count(), 1);
  const recovered = second.getRefreshTask("daily-main", task.taskId);
  assert.equal(recovered.status, "completed");
  assert.equal(recovered.outcome, "accepted");
  assert.equal(second.listItems("daily-main").items.length, 4);
  assert.equal(second.listFetchAttempts("daily-main").length, 1);
});

test("tracks stage failures and page rejections as distinct task outcomes", async (t) => {
  const time = manualClock();
  const fetcher = {
    calls: 0,
    impl: null,
    async fetchFeed(feed) {
      fetcher.calls += 1;
      return fetcher.impl(feed);
    }
  };
  fetcher.impl = () => {
    throw Object.assign(new Error("upstream timeout"), { code: "fetch_failed", attemptCount: 3 });
  };
  const { archive } = await makeArchive(t, { fetcher, clock: time.clock });
  archive.registerFeed(baseFeed);
  const failedSubmission = archive.submitRefreshTask("daily-main", {});
  const failedTask = await archive.refreshTasks.awaitTask(failedSubmission.task.taskId);
  assert.equal(failedTask.status, "failed");
  assert.equal(failedTask.stage, "fetch");
  assert.equal(failedTask.errorCode, "fetch_failed");
  assert.equal(failedTask.errorMessage, "upstream timeout");
  assert.equal(failedTask.outcome, null);
  assert.equal(failedTask.captureId, null);
  time.advance();
  fetcher.impl = (feed) => ({
    status: 200, html: page(standardItems(2)), requestedUrl: feed.baseUrl, finalUrl: feed.baseUrl, attemptCount: 1
  });
  const rejectedSubmission = archive.submitRefreshTask("daily-main", {});
  const rejectedTask = await archive.refreshTasks.awaitTask(rejectedSubmission.task.taskId);
  assert.equal(rejectedTask.status, "completed");
  assert.equal(rejectedTask.stage, "done");
  assert.equal(rejectedTask.outcome, "rejected");
  assert.equal(rejectedTask.errorCode, null);
  assert.match(rejectedTask.assessment.reason, /too_few/);
  assert.ok(rejectedTask.captureId);
  const attempts = archive.listFetchAttempts("daily-main");
  assert.deepEqual(attempts.map((attempt) => attempt.outcome), ["rejected", "failed"]);
  assert.equal(attempts[0].errorCode, "capture_rejected");
  assert.equal(attempts[1].errorCode, "fetch_failed");
  const captures = archive.listCaptures("daily-main");
  assert.equal(captures.length, 1);
  assert.equal(captures[0].accepted, false);
  assert.equal(archive.articles.stats("daily-main").total, 0);
  assert.equal(archive.listItems("daily-main").items.length, 0);
  const tasks = archive.listRefreshTasks("daily-main");
  assert.deepEqual(tasks.map((task) => [task.status, task.outcome]), [
    ["completed", "rejected"],
    ["failed", null]
  ]);
});

test("keeps a fetched but unarchived page out of the accepted item list", async (t) => {
  const time = manualClock();
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const hooks = {
    async afterCheckpoint(name) {
      if (name === "fetch") await gate;
    }
  };
  const { archive } = await makeArchive(t, {
    fetcher: cannedFetcher({ status: 200, html: page(standardItems(4)) }),
    taskHooks: hooks,
    clock: time.clock
  });
  archive.registerFeed(baseFeed);
  const { task } = archive.submitRefreshTask("daily-main", {});
  let parked = archive.getRefreshTask("daily-main", task.taskId);
  for (let spins = 0; parked.stage === "fetch" && spins < 1000; spins += 1) {
    await new Promise((resolve) => {
      setImmediate(resolve);
    });
    parked = archive.getRefreshTask("daily-main", task.taskId);
  }
  assert.equal(parked.status, "running");
  assert.equal(parked.stage, "assess");
  assert.ok(parked.fetchedBytes > 0);
  assert.equal(archive.listItems("daily-main").items.length, 0);
  assert.equal(archive.listCaptures("daily-main").length, 0);
  assert.equal(archive.feedStatus("daily-main").health, "awaiting_capture");
  release();
  const terminal = await archive.refreshTasks.awaitTask(task.taskId);
  assert.equal(terminal.outcome, "accepted");
  assert.equal(archive.listItems("daily-main").items.length, 4);
  assert.equal(archive.listCaptures("daily-main").length, 1);
});

test("records a not-modified upstream response as a completed task without a capture", async (t) => {
  const { archive } = await makeArchive(t, {
    fetcher: cannedFetcher({ status: 304, html: null, attemptCount: 1 })
  });
  archive.registerFeed(baseFeed);
  const { task } = archive.submitRefreshTask("daily-main", {});
  const terminal = await archive.refreshTasks.awaitTask(task.taskId);
  assert.equal(terminal.status, "completed");
  assert.equal(terminal.outcome, "not_modified");
  assert.equal(terminal.captureId, null);
  assert.ok(terminal.attemptId);
  const [attempt] = archive.listFetchAttempts("daily-main");
  assert.equal(attempt.outcome, "not_modified");
  assert.equal(attempt.httpStatus, 304);
  assert.equal(attempt.taskId, task.taskId);
  assert.equal(archive.listCaptures("daily-main").length, 0);
});

test("links the existing capture when another path already archived the same page", async (t) => {
  const time = manualClock("2026-11-16T02:00:10.000Z");
  const html = page(standardItems(4));
  const { archive } = await makeArchive(t, {
    fetcher: cannedFetcher({ status: 200, html }),
    clock: time.clock
  });
  archive.registerFeed(baseFeed);
  const manual = archive.importSnapshot("daily-main", html, "2026-11-16T02:00:01.000Z");
  assert.equal(manual.accepted, true);
  const { task } = archive.submitRefreshTask("daily-main", {});
  const terminal = await archive.refreshTasks.awaitTask(task.taskId);
  assert.equal(terminal.status, "completed");
  assert.equal(terminal.outcome, "accepted");
  assert.equal(terminal.captureId, manual.captureId);
  assert.equal(terminal.captureReplay, true);
  assert.equal(archive.listCaptures("daily-main").length, 1);
  assert.ok(archive.listItems("daily-main").items.every((item) => item.seenCount === 1));
  const [attempt] = archive.listFetchAttempts("daily-main");
  assert.equal(attempt.captureId, manual.captureId);
  assert.equal(attempt.taskId, task.taskId);
  assert.equal(archive.auditIntegrity().status, "consistent");
});

test("serves task submission, progress and detail over HTTP next to the legacy refresh", async (t) => {
  const time = manualClock();
  const fetcher = cannedFetcher((feed, calls) => ({
    status: 200,
    html: page(standardItems(4, calls * 10)),
    requestedUrl: feed.baseUrl,
    finalUrl: feed.baseUrl,
    attemptCount: 1
  }));
  const { archive } = await makeArchive(t, { fetcher, clock: time.clock });
  archive.registerFeed(baseFeed);
  const app = createHttpServer(archive, { host: "127.0.0.1", port: 0, logger: { error() {} } });
  const address = await app.listen();
  t.after(() => app.close());
  const root = `http://127.0.0.1:${address.port}/v1/feeds/daily-main`;

  const created = await fetch(`${root}/refresh-tasks`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ taskKey: "op-1" })
  });
  assert.equal(created.status, 202);
  const createdBody = await created.json();
  assert.equal(createdBody.deduplicated, false);
  assert.equal(createdBody.task.status, "running");
  assert.equal(createdBody.task.stage, "fetch");
  assert.equal(createdBody.task.taskKey, "op-1");

  const replay = await fetch(`${root}/refresh-tasks`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ taskKey: "op-1" })
  });
  assert.equal(replay.status, 200);
  const replayBody = await replay.json();
  assert.equal(replayBody.deduplicated, true);
  assert.equal(replayBody.task.taskId, createdBody.task.taskId);

  await archive.whenIdle();
  const detail = await fetch(`${root}/refresh-tasks/${createdBody.task.taskId}`);
  assert.equal(detail.status, 200);
  const detailBody = (await detail.json()).task;
  assert.equal(detailBody.status, "completed");
  assert.equal(detailBody.outcome, "accepted");
  assert.equal(detailBody.fetchedHtml, undefined);
  assert.ok(detailBody.fetchedBytes > 0);
  assert.ok(detailBody.captureId);
  assert.ok(detailBody.attemptId);

  const listed = await fetch(`${root}/refresh-tasks`);
  const listedBody = await listed.json();
  assert.equal(listedBody.tasks.length, 1);
  assert.equal(listedBody.tasks[0].taskId, createdBody.task.taskId);

  const missingTask = await fetch(`${root}/refresh-tasks/9999`);
  assert.equal(missingTask.status, 404);
  const missingFeed = await fetch("http://127.0.0.1:" + address.port + "/v1/feeds/nope/refresh-tasks", { method: "POST" });
  assert.equal(missingFeed.status, 404);
  const invalidBody = await fetch(`${root}/refresh-tasks`, { method: "POST", body: "{not-json" });
  assert.equal(invalidBody.status, 400);

  time.advance();
  const legacy = await fetch(`${root}/refresh`, { method: "POST" });
  assert.equal(legacy.status, 202);
  const legacyBody = await legacy.json();
  assert.equal(legacyBody.outcome, "accepted");
  assert.equal(legacyBody.capture.accepted, true);
  const attempts = await (await fetch(`${root}/attempts`)).json();
  assert.equal(attempts.attempts.length, 2);
  assert.ok(attempts.attempts.every((attempt) => attempt.taskId));
  const allTasks = await (await fetch(`${root}/refresh-tasks`)).json();
  assert.equal(allTasks.tasks.length, 2);
});
