/**
 * 共享取回（回热）任务管理器。
 *
 *  - 同一数据集/同一固定版本的整分区回热只会有一个任务；多个查询加入同一任务
 *    （sharedJoins 计数），避免一个冷分区被十个查询重复下载。
 *  - 每个文件先校验再登记；校验失败按设置重试，重试仍失败则该文件保持原可读位置
 *    （通常是冷层），不会把坏字节登记成热副本。
 *  - 查询取消只退订任务；任务是否继续由 cancelPolicy 决定：
 *      continue       —— 继续把整份数据取回并缓存（默认）；
 *      cancel-if-idle —— 没有订阅者时立即中止。
 *    两种情况下查询都会得到明确的“共享任务是否继续”标记。
 */
import path from 'node:path';
import { readFile, stat } from 'node:fs/promises';
import { nowIso, randomId, sha256Hex } from './util.js';

export class RetrievalManager {
  constructor({
    catalog, cold, hot, pins = null, log = () => {},
  }) {
    this.catalog = catalog;
    this.cold = cold;
    this.hot = hot;
    this.pins = pins;
    this.log = log;
    this.tasks = new Map();
  }

  keyFor(datasetId, { scope, fileId, partitionId, versionId }) {
    return scope === 'file'
      ? `${datasetId}|file|${fileId}|${partitionId}|${versionId}`
      : `${datasetId}|dataset|${partitionId}|${versionId}`;
  }

  list() {
    return [...this.tasks.values()].map((task) => this.#publicView(task));
  }

  get(id) {
    return this.tasks.get(id) ?? null;
  }

  findByKey(key) {
    for (const task of this.tasks.values()) {
      if (task.key === key && task.status === 'running') return task;
    }
    return null;
  }

  #publicView(task) {
    return {
      id: task.id,
      datasetId: task.datasetId,
      scope: task.scope,
      fileId: task.fileId,
      partitionId: task.partitionId,
      versionId: task.versionId,
      status: task.status,
      reason: task.reason,
      startedAt: task.startedAt,
      finishedAt: task.finishedAt,
      totalFiles: task.totalFiles,
      filesDone: task.filesDone,
      filesReused: task.filesReused,
      filesFailed: task.filesFailed,
      bytesDownloaded: task.bytesDownloaded,
      bytesTotal: task.bytesTotal,
      subscribers: task.subscribers.size,
      detached: task.subscribers.size === 0 && task.status === 'running',
      detachedAt: task.detachedAt,
      cancelRequested: task.controller.signal.aborted,
      cancelPolicy: this.catalog.settings().cancelPolicy,
      error: task.error,
      retries: task.retries,
      currentFile: task.currentFile,
      firstByteAt: task.firstByteAt,
      failures: task.failures,
    };
  }

  subscribe(taskId, onProgress = null) {
    const task = this.tasks.get(taskId);
    if (!task) return null;
    const subscription = {
      taskId,
      onProgress,
      joinedAt: nowIso(),
    };
    task.subscribers.add(subscription);
    task.detachedAt = null;
    return {
      subscription,
      done: task.promise,
      unsubscribe: () => this.unsubscribe(taskId, subscription),
    };
  }

  unsubscribe(taskId, subscription) {
    const task = this.tasks.get(taskId);
    if (!task) return { taskContinues: false };
    if (!task.subscribers.delete(subscription)) return { taskContinues: task.status === 'running' };
    const policy = this.catalog.settings().cancelPolicy;
    if (task.subscribers.size === 0 && task.status === 'running') {
      task.detachedAt = nowIso();
      this.catalog.addEvent({
        kind: 'retrieval.detached',
        datasetId: task.datasetId,
        fileId: task.fileId,
        reason: `最后一个查询退订；策略=${policy}`,
        evidence: { taskId },
      });
      if (policy === 'cancel-if-idle') {
        this.catalog.countDetached(false);
        task.controller.abort();
        return { taskContinues: false, policy };
      }
      this.catalog.countDetached(true);
      return { taskContinues: true, policy };
    }
    return { taskContinues: task.status === 'running', policy };
  }

  cancel(taskId, reason = '用户取消取回') {
    const task = this.tasks.get(taskId);
    if (!task) return false;
    task.cancelReason = reason;
    task.controller.abort();
    return true;
  }

  /** 创建或加入同键的共享任务。 */
  ensure({ datasetId, scope = 'dataset', fileId = null, partitionId, versionId, reason = '按需回热' }) {
    const key = this.keyFor(datasetId, { scope, fileId, partitionId, versionId });
    const existing = this.findByKey(key);
    if (existing) {
      return { task: existing, shared: true };
    }
    const dataset = this.catalog.requireDataset(datasetId);
    const controller = new AbortController();
    const task = {
      id: `rt_${randomId('')}`,
      key,
      datasetId,
      scope,
      fileId,
      partitionId,
      versionId,
      reason,
      status: 'running',
      startedAt: nowIso(),
      finishedAt: null,
      controller,
      subscribers: new Set(),
      detachedAt: null,
      cancelReason: null,
      totalFiles: 0,
      filesDone: 0,
      filesReused: 0,
      filesFailed: 0,
      bytesDownloaded: 0,
      bytesTotal: 0,
      retries: 0,
      currentFile: null,
      firstByteAt: null,
      error: null,
      failures: [],
      promise: null,
      datasetName: dataset.name,
      releasePin: this.pins ? this.pins.pin(partitionId, versionId) : null,
    };
    task.promise = this.#run(task).catch((error) => {
      task.status = error.code === 'ABORTED' ? 'cancelled' : 'failed';
      task.error = error.message;
      task.finishedAt = nowIso();
      this.catalog.addEvent({
        kind: 'retrieval.failed',
        datasetId,
        fileId,
        reason: error.message,
        evidence: { taskId: task.id, scope, versionId },
      });
      throw error;
    }).finally(() => {
      task.releasePin?.();
      task.releasePin = null;
      // 保留完成的任务一小段时间，便于网页展示；之后从内存表清理。
      setTimeout(() => {
        if (this.tasks.get(task.id) === task) this.tasks.delete(task.id);
      }, 10 * 60_000).unref?.();
    });
    task.promise.catch(() => {});
    this.tasks.set(task.id, task);
    this.catalog.countColdTask();
    this.catalog.addEvent({
      kind: 'retrieval.started',
      datasetId,
      fileId,
      reason: `${reason}（${scope === 'file' ? '单文件' : '整分区'}，版本 ${versionId}）`,
      evidence: { taskId: task.id, versionId, scope },
    });
    return { task, shared: false };
  }

  async #targets(task) {
    const manifest = await this.cold.readManifest(task.partitionId, task.versionId);
    const entries = task.scope === 'file'
      ? manifest.files.filter((entry) => entry.fileId === task.fileId)
      : manifest.files;
    if (!entries.length) {
      const error = new Error('冷版本中没有要回热的文件');
      error.code = 'COLD_FILE_NOT_FOUND';
      error.statusCode = 404;
      throw error;
    }
    return { manifest, entries };
  }

  async #run(task) {
    const signal = task.controller.signal;
    const { entries } = await this.#targets(task);
    task.totalFiles = entries.length;
    task.bytesTotal = entries.reduce((sum, entry) => sum + entry.size, 0);
    const maxRetries = Math.max(0, Number(this.catalog.settings().maxVerifyRetries ?? 1));
    for (const entry of entries) {
      this.catalog.setPendingReheat(task.datasetId, entry.fileId, true, { taskId: task.id });
    }
    // 让“回热中”标记先落盘：进程若在此刻崩溃，恢复扫描能看到中断痕迹。
    await this.catalog.flush();
    for (const entry of entries) {
      if (signal.aborted) throw this.#abort(task);
      task.currentFile = entry.name;
      task.firstByteAt = null;
      const { file } = this.catalog.findFile(task.datasetId, entry.fileId);

      // 已经有一份摘要一致的热副本：直接复用，不产生冷存储流量。
      // 先把路径固定到局部变量：回收线程可能在任何 await 之间把 file.hot 置空，
      // 固定后即使回收发生，我们也只是对一个旧路径做只读校验，失败就当作缓存未命中。
      const reusablePath = file.hot?.path ?? null;
      const reusableSha = file.hot?.sha256 ?? null;
      if (reusablePath && reusableSha === entry.sha256) {
        const exists = await stat(reusablePath).then(() => true, () => false);
        if (exists) {
          let actual = null;
          try {
            actual = sha256Hex(await readFile(reusablePath));
          } catch {
            // 文件在上面的 stat 之后被并发回收删除：按缓存未命中处理，走冷取回。
            actual = null;
          }
          if (actual === entry.sha256) {
            task.filesReused += 1;
            task.filesDone += 1;
            this.catalog.commitHotReplicas(task.datasetId, {
              files: [{ fileId: entry.fileId, path: reusablePath, sha256: entry.sha256, size: entry.size }],
              evidence: '热盘已有摘要一致副本，回热直接复用，无冷存储流量',
              actor: 'system',
            });
            this.#progress(task);
            continue;
          }
        }
      }

      let attempt = 0;
      let lastError = null;
      let completedBytes = task.bytesDownloaded;
      while (attempt <= maxRetries) {
        if (signal.aborted) throw this.#abort(task);
        attempt += 1;
        const staged = await this.hot.stage(task.id, entry.fileId);
        let attemptBytes = 0;
        try {
          const result = await this.cold.retrieveFile({
            partitionId: task.partitionId,
            versionId: task.versionId,
            fileId: entry.fileId,
            destPath: staged,
            signal,
            onProgress: (progress) => {
              task.currentFile = entry.name;
              if (!task.firstByteAt && progress.bytes > 0) task.firstByteAt = nowIso();
              attemptBytes = progress.bytes;
              // 重试时回退本次尝试的字节数，避免同一份数据在网络失败后重复计费。
              task.bytesDownloaded = completedBytes + attemptBytes;
              this.#progress(task, progress);
            },
          });
          if (task.firstByteAt === null && result.firstByteAt) task.firstByteAt = result.firstByteAt;
          if (result.sha256 !== entry.sha256) {
            this.catalog.countVerifyFailure();
            task.failures.push({
              at: nowIso(),
              fileId: entry.fileId,
              fileName: entry.name,
              attempt,
              expectedSha256: entry.sha256,
              actualSha256: result.sha256,
              reason: '取回内容摘要与 manifest 不一致',
            });
            lastError = new Error(`回热校验失败：${entry.name} 的 SHA-256 与冷 manifest 不一致`);
            lastError.code = 'REHEAT_VERIFY_FAILED';
            if (attempt <= maxRetries) {
              task.retries += 1;
              this.catalog.addEvent({
                kind: 'reheat.verify-retry',
                datasetId: task.datasetId,
                fileId: entry.fileId,
                reason: `第 ${attempt} 次取回校验失败，重试`,
                evidence: {
                  expectedSha256: entry.sha256,
                  actualSha256: result.sha256,
                  taskId: task.id,
                },
              });
              continue;
            }
            throw lastError;
          }
          // 写内容路径 + 登记目录必须与回收互斥：先拿租约（若此刻正在回收该
          // 路径就等回收结束），再 promote。否则可能出现“文件刚写到内容路径，
          // 就被孤儿清理当成未引用文件删掉”。
          const targetPath = this.hot.contentPath(task.datasetId, entry.fileId, entry.sha256);
          const releaseHotLease = await this.hot.leases.acquireWhenOpen(targetPath);
          let hotPath;
          try {
            hotPath = await this.hot.promoteStaged({
              taskId: task.id,
              fileId: entry.fileId,
              datasetId: task.datasetId,
              sha256: entry.sha256,
            });
            task.bytesDownloaded = completedBytes + result.bytes;
            this.catalog.addColdBytes(result.bytes);
            completedBytes = task.bytesDownloaded;
            task.filesDone += 1;
            const commit = this.catalog.commitHotReplicas(task.datasetId, {
              files: [{ fileId: entry.fileId, path: hotPath, sha256: entry.sha256, size: entry.size }],
              evidence: `取回 ${result.bytes} 字节，复算 SHA-256 与冷 manifest 一致`,
              actor: 'system',
            });
            if (commit.rejected.length) {
              // 取回期间版本已被替换：字节留在内容路径上但不登记，交给 GC。
              task.failures.push({
                at: nowIso(),
                fileId: entry.fileId,
                fileName: entry.name,
                reason: '回热期间版本被替换，旧内容未登记为当前热副本',
              });
            }
          } finally {
            releaseHotLease();
          }
          await this.catalog.flush();
          lastError = null;
          break;
        } catch (error) {
          lastError = error;
          await this.hot.discardStaging(task.id).catch(() => {});
          if (error.code === 'ABORTED') throw error;
          if (attempt > maxRetries) break;
          task.retries += 1;
        }
      }
      this.catalog.setPendingReheat(task.datasetId, entry.fileId, false);
      if (lastError) {
        task.filesFailed += 1;
        task.failures.push({
          at: nowIso(),
          fileId: entry.fileId,
          fileName: entry.name,
          reason: lastError.message,
          code: lastError.code ?? 'FETCH_FAILED',
        });
        this.catalog.markFileError(task.datasetId, entry.fileId, `回热失败：${lastError.message}`, {
          evidence: { taskId: task.id, versionId: task.versionId, code: lastError.code },
          kind: 'reheat.verify-failed',
        });
      }
      this.#progress(task);
    }
    for (const entry of entries) {
      this.catalog.setPendingReheat(task.datasetId, entry.fileId, false);
    }
    await this.hot.discardStaging(task.id).catch(() => {});
    if (task.filesFailed > 0) {
      task.status = 'failed';
      task.error = `${task.filesFailed}/${task.totalFiles} 个文件回热失败，原可读位置保持不变`;
    } else {
      task.status = 'completed';
    }
    task.finishedAt = nowIso();
    this.catalog.addEvent({
      kind: task.status === 'completed' ? 'retrieval.completed' : 'retrieval.partial-failure',
      datasetId: task.datasetId,
      fileId: task.fileId,
      reason: task.status === 'completed'
        ? `共享取回完成：下载 ${task.bytesDownloaded} 字节，复用 ${task.filesReused} 个热副本`
        : task.error,
      evidence: {
        taskId: task.id,
        detached: task.subscribers.size === 0,
        retries: task.retries,
        versionId: task.versionId,
      },
    });
    task.subscribers.clear();
  }

  #progress(task, progress = null) {
    if (progress) task.lastProgressAt = nowIso();
  }

  #abort(task) {
    const error = new Error(task.cancelReason ?? '取回任务已取消');
    error.code = 'ABORTED';
    error.statusCode = 499;
    return error;
  }
}

export { path };
