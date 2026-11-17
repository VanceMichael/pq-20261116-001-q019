import { openDatabase, closeDatabase } from "../store/database.js";
import { FeedRepository } from "../store/feed-repository.js";
import { CaptureRepository } from "../store/capture-repository.js";
import { ArticleRepository } from "../store/article-repository.js";
import { FetchRepository } from "../store/fetch-repository.js";
import { RefreshTaskRepository } from "../store/refresh-task-repository.js";
import { ImportService } from "../ingest/import-service.js";
import { PageFetcher } from "../acquisition/page-fetcher.js";
import { RefreshService } from "./refresh-service.js";
import { RefreshTaskService } from "./refresh-task-service.js";
import { ChangeReportService } from "./change-report.js";
import { HealthReportService } from "./health-report.js";
import { ReviewRepository } from "../store/review-repository.js";
import { ReviewService } from "./review-service.js";
import { CsvExporter } from "../export/csv-exporter.js";
import { ArticleTimelineService } from "./article-timeline.js";
import { IntegrityAuditService } from "./integrity-audit.js";

export class NewsArchive {
  constructor(filename, options = {}) {
    this.clock = options.clock ?? (() => new Date());
    this.database = openDatabase(filename);
    this.feeds = new FeedRepository(this.database);
    this.captures = new CaptureRepository(this.database);
    this.articles = new ArticleRepository(this.database);
    this.fetches = new FetchRepository(this.database);
    this.reviews = new ReviewRepository(this.database);
    this.tasks = new RefreshTaskRepository(this.database);
    this.imports = new ImportService({
      database: this.database,
      feeds: this.feeds,
      captures: this.captures,
      articles: this.articles,
      clock: this.clock
    });
    this.refreshTasks = new RefreshTaskService({
      database: this.database,
      feeds: this.feeds,
      fetches: this.fetches,
      captures: this.captures,
      imports: this.imports,
      tasks: this.tasks,
      fetcher: options.fetcher ?? new PageFetcher(),
      clock: this.clock,
      hooks: options.taskHooks,
      logger: options.logger ?? console
    });
    this.refreshes = new RefreshService({
      feeds: this.feeds,
      fetches: this.fetches,
      captures: this.captures,
      articles: this.articles,
      tasks: this.refreshTasks
    });
    this.changes = new ChangeReportService({ feeds: this.feeds, captures: this.captures, articles: this.articles });
    this.health = new HealthReportService({ feeds: this.feeds, fetches: this.fetches, clock: this.clock });
    this.reviewService = new ReviewService({ feeds: this.feeds, reviews: this.reviews, clock: this.clock });
    this.csvExporter = new CsvExporter({ database: this.database, feeds: this.feeds, articles: this.articles });
    this.timelines = new ArticleTimelineService({ feeds: this.feeds, reviews: this.reviews, articles: this.articles });
    this.integrityAudit = new IntegrityAuditService(this.database);
    this.recovery = this.refreshTasks.recoverInterrupted();
  }

  registerFeed(input) {
    return this.feeds.register(input, this.now());
  }

  importSnapshot(feedId, html, capturedAt) {
    return this.imports.importHtml(feedId, html, capturedAt);
  }

  refreshFeed(feedId) {
    return this.refreshes.refresh(feedId);
  }

  submitRefreshTask(feedId, options = {}) {
    return this.refreshTasks.submit(feedId, options);
  }

  getRefreshTask(feedId, taskId) {
    if (!this.feeds.get(feedId)) return null;
    const task = this.refreshTasks.get(taskId);
    return task && task.feedId === feedId ? task : null;
  }

  listRefreshTasks(feedId, limit) {
    if (!this.feeds.get(feedId)) return null;
    return this.refreshTasks.listForFeed(feedId, limit);
  }

  whenIdle() {
    return this.refreshTasks.whenIdle();
  }

  feedStatus(feedId) {
    const feed = this.feeds.get(feedId);
    if (!feed) return null;
    const latest = this.captures.recent(feedId, 1)[0] ?? null;
    return {
      ...feed,
      health: latest ? latest.accepted ? "healthy" : "degraded" : "awaiting_capture",
      latestAttempt: latest,
      statistics: this.articles.stats(feedId),
      collectionHealth: this.health.report(feedId)
    };
  }

  listItems(feedId, options) {
    if (!this.feeds.get(feedId)) return null;
    return { items: this.articles.list(feedId, options), statistics: this.articles.stats(feedId) };
  }

  listCaptures(feedId, limit) {
    if (!this.feeds.get(feedId)) return null;
    return this.captures.recent(feedId, limit);
  }

  listFetchAttempts(feedId, limit) {
    return this.refreshes.recent(feedId, limit);
  }

  feedChanges(feedId) {
    return this.changes.report(feedId);
  }

  feedHealth(feedId) {
    return this.health.report(feedId);
  }

  reviewArticle(feedId, articleId, input) {
    return this.reviewService.change(feedId, articleId, input);
  }

  reviewEvents(feedId, articleId, limit) {
    return this.reviewService.events(feedId, articleId, limit);
  }

  exportCsv(feedId, writable, options) {
    return this.csvExporter.write(feedId, writable, options);
  }

  articleTimeline(feedId, articleId, limit) {
    return this.timelines.report(feedId, articleId, limit);
  }

  auditIntegrity() {
    return this.integrityAudit.run();
  }

  close() {
    closeDatabase(this.database);
  }

  now() {
    return this.clock().toISOString();
  }
}
