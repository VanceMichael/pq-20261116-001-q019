export class FetchRepository {
  constructor(database) {
    this.database = database;
    this.insert = database.prepare(`INSERT INTO fetch_attempts
      (feed_id, started_at, finished_at, outcome, http_status, attempt_count, requested_url,
       final_url, capture_id, error_code, error_message, task_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    this.listQuery = database.prepare(`SELECT attempt_id, feed_id, started_at, finished_at, outcome,
      http_status, attempt_count, requested_url, final_url, capture_id, error_code, error_message, task_id
      FROM fetch_attempts WHERE feed_id=? ORDER BY attempt_id DESC LIMIT ?`);
    this.summaryQuery = database.prepare(`SELECT COUNT(*) AS total,
      SUM(CASE WHEN outcome='accepted' THEN 1 ELSE 0 END) AS accepted,
      SUM(CASE WHEN outcome='rejected' THEN 1 ELSE 0 END) AS rejected,
      SUM(CASE WHEN outcome='failed' THEN 1 ELSE 0 END) AS failed,
      SUM(CASE WHEN outcome='not_modified' THEN 1 ELSE 0 END) AS not_modified,
      MAX(CASE WHEN outcome='accepted' THEN finished_at END) AS last_success_at,
      MAX(finished_at) AS last_attempt_at
      FROM fetch_attempts WHERE feed_id=? AND finished_at>=?`);
  }

  record(attempt) {
    const result = this.insert.run(attempt.feedId, attempt.startedAt, attempt.finishedAt, attempt.outcome,
      attempt.httpStatus, attempt.attemptCount, attempt.requestedUrl, attempt.finalUrl,
      attempt.captureId, attempt.errorCode, attempt.errorMessage, attempt.taskId ?? null);
    return Number(result.lastInsertRowid);
  }

  recent(feedId, limit = 20) {
    const safeLimit = Math.max(1, Math.min(100, Number(limit) || 20));
    return this.listQuery.all(feedId, safeLimit).map(mapAttempt);
  }

  summarySince(feedId, since) {
    const result = this.summaryQuery.get(feedId, since);
    return {
      total: result.total,
      accepted: result.accepted ?? 0,
      rejected: result.rejected ?? 0,
      failed: result.failed ?? 0,
      notModified: result.not_modified ?? 0,
      lastSuccessAt: result.last_success_at,
      lastAttemptAt: result.last_attempt_at
    };
  }
}

function mapAttempt(row) {
  return {
    attemptId: row.attempt_id,
    feedId: row.feed_id,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
    outcome: row.outcome,
    httpStatus: row.http_status,
    attemptCount: row.attempt_count,
    requestedUrl: row.requested_url,
    finalUrl: row.final_url,
    captureId: row.capture_id,
    errorCode: row.error_code,
    errorMessage: row.error_message,
    taskId: row.task_id ?? null
  };
}
