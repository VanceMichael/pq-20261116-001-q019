export class RefreshTaskRepository {
  constructor(database) {
    this.database = database;
    this.insertTask = database.prepare(`INSERT INTO refresh_tasks
      (feed_id, task_key, status, stage, created_at, updated_at)
      VALUES (?, ?, 'running', 'fetch', ?, ?)`);
    this.byId = database.prepare("SELECT * FROM refresh_tasks WHERE task_id=?");
    this.byKey = database.prepare(`SELECT * FROM refresh_tasks
      WHERE feed_id=? AND task_key=? ORDER BY task_id DESC LIMIT 1`);
    this.activeQuery = database.prepare(`SELECT * FROM refresh_tasks
      WHERE feed_id=? AND status='running' ORDER BY task_id LIMIT 1`);
    this.interruptedQuery = database.prepare(`SELECT * FROM refresh_tasks
      WHERE status='running' ORDER BY task_id`);
    this.listQuery = database.prepare(`SELECT * FROM refresh_tasks
      WHERE feed_id=? ORDER BY task_id DESC LIMIT ?`);
    this.startFetch = database.prepare(`UPDATE refresh_tasks
      SET fetch_started_at=?, updated_at=? WHERE task_id=? AND status='running'`);
    this.savePage = database.prepare(`UPDATE refresh_tasks SET stage='assess',
      http_status=?, attempt_count=?, requested_url=?, final_url=?, fetched_html=?,
      fetch_finished_at=?, updated_at=? WHERE task_id=? AND status='running'`);
    this.saveAssessmentStmt = database.prepare(`UPDATE refresh_tasks
      SET stage='archive', assessment_json=?, updated_at=? WHERE task_id=? AND status='running'`);
    this.linkCaptureStmt = database.prepare(`UPDATE refresh_tasks
      SET capture_id=?, capture_replay=?, updated_at=? WHERE task_id=? AND status='running'`);
    this.linkAttemptStmt = database.prepare(`UPDATE refresh_tasks
      SET attempt_id=?, updated_at=? WHERE task_id=? AND status='running'`);
    this.completeStmt = database.prepare(`UPDATE refresh_tasks
      SET status='completed', stage='done', outcome=?, finished_at=?, updated_at=?
      WHERE task_id=? AND status='running'`);
    this.completeNotModifiedStmt = database.prepare(`UPDATE refresh_tasks
      SET status='completed', stage='done', outcome='not_modified',
      http_status=?, attempt_count=?, requested_url=?, final_url=?, attempt_id=?,
      finished_at=?, updated_at=? WHERE task_id=? AND status='running'`);
    this.failStmt = database.prepare(`UPDATE refresh_tasks
      SET status='failed', error_code=?, error_message=?, http_status=?, attempt_count=?,
      attempt_id=?, finished_at=?, updated_at=? WHERE task_id=? AND status='running'`);
  }

  create({ feedId, taskKey, now }) {
    const result = this.insertTask.run(feedId, taskKey, now, now);
    return Number(result.lastInsertRowid);
  }

  get(taskId) {
    const row = this.byId.get(taskId);
    return row ? mapTask(row) : null;
  }

  getForExecution(taskId) {
    const row = this.byId.get(taskId);
    return row ? mapTask(row, { includePage: true }) : null;
  }

  getByKey(feedId, taskKey) {
    const row = this.byKey.get(feedId, taskKey);
    return row ? mapTask(row) : null;
  }

  activeForFeed(feedId) {
    const row = this.activeQuery.get(feedId);
    return row ? mapTask(row) : null;
  }

  interrupted() {
    return this.interruptedQuery.all().map((row) => mapTask(row));
  }

  listForFeed(feedId, limit = 20) {
    const safeLimit = Math.max(1, Math.min(100, Number(limit) || 20));
    return this.listQuery.all(feedId, safeLimit).map((row) => mapTask(row));
  }

  markFetchStarted(taskId, startedAt) {
    this.startFetch.run(startedAt, startedAt, taskId);
  }

  saveFetchedPage(taskId, { httpStatus, attemptCount, requestedUrl, finalUrl, html, finishedAt }) {
    this.savePage.run(httpStatus, attemptCount, requestedUrl, finalUrl, html, finishedAt, finishedAt, taskId);
  }

  saveAssessment(taskId, assessmentJson, now) {
    this.saveAssessmentStmt.run(assessmentJson, now, taskId);
  }

  linkCapture(taskId, captureId, replay, now) {
    this.linkCaptureStmt.run(captureId, replay ? 1 : 0, now, taskId);
  }

  linkAttempt(taskId, attemptId, now) {
    this.linkAttemptStmt.run(attemptId, now, taskId);
  }

  complete(taskId, { outcome, now }) {
    this.completeStmt.run(outcome, now, now, taskId);
  }

  completeNotModified(taskId, { httpStatus, attemptCount, requestedUrl, finalUrl, attemptId, now }) {
    this.completeNotModifiedStmt.run(httpStatus, attemptCount, requestedUrl, finalUrl, attemptId, now, now, taskId);
  }

  fail(taskId, { errorCode, errorMessage, httpStatus, attemptCount, attemptId, now }) {
    this.failStmt.run(errorCode, errorMessage, httpStatus, attemptCount, attemptId, now, now, taskId);
  }
}

function mapTask(row, { includePage = false } = {}) {
  const task = {
    taskId: row.task_id,
    feedId: row.feed_id,
    taskKey: row.task_key,
    status: row.status,
    stage: row.stage,
    outcome: row.outcome,
    httpStatus: row.http_status,
    attemptCount: row.attempt_count,
    requestedUrl: row.requested_url,
    finalUrl: row.final_url,
    fetchStartedAt: row.fetch_started_at,
    fetchFinishedAt: row.fetch_finished_at,
    fetchedBytes: row.fetched_html == null ? null : Buffer.byteLength(row.fetched_html, "utf8"),
    assessment: row.assessment_json ? JSON.parse(row.assessment_json) : null,
    captureId: row.capture_id,
    captureReplay: row.capture_id == null ? null : Boolean(row.capture_replay),
    attemptId: row.attempt_id,
    errorCode: row.error_code,
    errorMessage: row.error_message,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    finishedAt: row.finished_at
  };
  if (includePage) task.fetchedHtml = row.fetched_html;
  return task;
}
