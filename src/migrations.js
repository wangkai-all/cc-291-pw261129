/**
 * 迁移执行器：把热副本搬到冷分区。
 *
 * 不变量（按执行顺序保证）：
 *  1. 先在冷存储构建不可见的新版本（tmp 目录 + manifest 最后写），逐对象校验 SHA-256。
 *  2. 全部通过后，才在目录库登记冷副本（readable 变成 hot+cold），并 fsync 目录库。
 *  3. 登记持久化完成，才开始等待读者租约并删除热副本。
 *  4. 任何一步失败：清除未登记的冷临时版本；热副本原样保留，readable 不变。
 *  5. 取消同样走 4，绝不在只有冷副本时丢热数据。
 *
 * 因此“迁到一半”时每个文件至少有一个已校验副本，可读位置始终可解析。
 */
import { atomicWriteJson, nowIso, randomId, readJson, sha256File } from './util.js';

export class MigrationRunner {
  constructor({
    catalog, cold, hot, log = () => {},
  }) {
    this.catalog = catalog;
    this.cold = cold;
    this.hot = hot;
    this.log = log;
    this.jobs = new Map();
  }

  active() {
    return [...this.jobs.values()].map((job) => ({
      datasetId: job.datasetId,
      migrationId: job.migrationId,
      versionId: job.versionId,
      partitionId: job.partitionId,
      startedAt: job.startedAt,
      cancelRequested: job.controller.signal.aborted,
    }));
  }

  isActive(datasetId) {
    return this.jobs.has(datasetId);
  }

  cancel(datasetId, reason = '用户取消') {
    const job = this.jobs.get(datasetId);
    if (!job) return false;
    job.cancelReason = reason;
    job.controller.abort();
    return true;
  }

  /** 异步启动迁移，立即返回迁移描述；迁移在独立任务中执行。 */
  start(datasetId, { partitionId, createdBy = 'user' } = {}) {
    if (this.jobs.has(datasetId)) {
      const error = new Error('该数据集已有迁移在进行');
      error.code = 'MIGRATION_RUNNING';
      error.statusCode = 409;
      throw error;
    }
    const dataset = this.catalog.requireDataset(datasetId);
    this.catalog.requirePartition(partitionId);
    if (dataset.files.some((file) => !file.hot)) {
      const error = new Error('数据集并非全部位于热盘，无法再次迁移');
      error.code = 'NOT_ALL_HOT';
      error.statusCode = 409;
      throw error;
    }
    if (dataset.files.some((file) => !file.cold)) {
      // 允许：冷版本是“附加”的第二副本。迁移会把当前热内容写成新冷版本。
    }
    const migrationId = `mg_${randomId('')}`;
    const versionId = `cv_${randomId('')}`;
    const controller = new AbortController();
    const migration = {
      id: migrationId,
      partitionId,
      versionId,
      status: 'running',
      phase: 'preparing',
      startedAt: nowIso(),
      heartbeatAt: nowIso(),
      finishedAt: null,
      totalFiles: dataset.files.length,
      totalBytes: dataset.totalBytes,
      verifiedFiles: 0,
      committedFiles: 0,
      coldWrittenBytes: 0,
      deletedHotFiles: 0,
      freedBytes: 0,
      note: '',
      cancelRequested: false,
      error: null,
      createdBy,
    };
    this.catalog.setMigration(datasetId, migration);
    this.catalog.addEvent({
      kind: 'migration.started',
      datasetId,
      reason: `开始迁移到冷分区 ${partitionId}`,
      evidence: { migrationId, versionId, files: migration.totalFiles, bytes: migration.totalBytes },
      actor: createdBy,
    });
    const job = {
      datasetId,
      migrationId,
      versionId,
      partitionId,
      startedAt: migration.startedAt,
      controller,
      cancelReason: null,
      done: null,
      result: null,
    };
    job.done = this.#run(job)
      .then((result) => { job.result = result; return result; })
      .catch((error) => {
        const migrationState = this.catalog.getDataset(datasetId)?.migration;
        job.result = {
          status: migrationState?.status ?? 'failed',
          error: error.message,
          code: error.code,
          versionId: migrationState?.versionId ?? null,
        };
        throw error;
      })
      .finally(() => { this.jobs.delete(datasetId); });
    // 后台任务失败不应产生未处理的 rejection；状态已写入目录库。
    job.done.catch(() => {});
    this.jobs.set(datasetId, job);
    return structuredClone(migration);
  }

  async waitFor(datasetId) {
    const job = this.jobs.get(datasetId);
    if (!job) {
      // 任务可能在 waitFor 之前已收尾：从目录库读出最终状态，避免误报 null。
      const migration = this.catalog.getDataset(datasetId)?.migration;
      if (!migration) return null;
      return {
        status: migration.status,
        versionId: migration.versionId ?? null,
        error: migration.error ?? null,
        note: migration.note ?? null,
      };
    }
    try {
      await job.done;
    } catch {
      // 失败/取消已写入 job.result；这里不再把取消误报成失败。
    }
    return job.result;
  }

  async #update(datasetId, patch, { persist = false } = {}) {
    this.catalog.updateMigrationProgress(datasetId, { ...patch, heartbeatAt: nowIso() }, { persist });
  }

  async #run(job) {
    const {
      datasetId, migrationId, partitionId, versionId, controller,
    } = job;
    const signal = controller.signal;
    const tmpVersion = `${versionId}_tmp`;
    let coldCommitted = false;
    let finalVersion = versionId;
    try {
      const dataset = this.catalog.requireDataset(datasetId);
      const files = [];
      for (const file of dataset.files) {
        if (signal.aborted) throw this.#abort(job);
        const sourcePath = file.hot?.path ?? null;
        if (!sourcePath) {
          throw Object.assign(new Error(`缺少热副本：${file.name}`), { code: 'MIGRATION_SOURCE_MISSING' });
        }
        const actual = await sha256File(sourcePath).catch(() => null);
        if (actual !== file.sha256) {
          throw Object.assign(new Error(`热副本摘要与目录记录不一致：${file.name}`), {
            code: 'MIGRATION_SOURCE_CHANGED', statusCode: 409,
          });
        }
        files.push({
          fileId: file.id,
          name: file.name,
          size: file.size,
          sha256: file.sha256,
          sourcePath,
        });
      }

      await this.#update(datasetId, { phase: 'cold-writing', coldWrittenBytes: 0 }, { persist: true });
      await this.cold.putVersion({
        partitionId,
        versionId: tmpVersion,
        files,
        signal,
        temporary: true,
        onProgress: (progress) => {
          this.#update(datasetId, { coldWrittenBytes: progress.bytes });
        },
      });
      if (signal.aborted) throw this.#abort(job);
      await this.#update(datasetId, {
        phase: 'verifying', coldWrittenBytes: files.reduce((sum, f) => sum + f.size, 0),
      });

      const verified = [];
      for (const file of files) {
        if (signal.aborted) throw this.#abort(job);
        const result = await this.cold.verifyObject(partitionId, tmpVersion, file.fileId);
        if (!result.ok) {
          throw Object.assign(new Error(`冷对象校验失败：${file.name}`), {
            code: 'COLD_VERIFY_FAILED', statusCode: 502,
          });
        }
        verified.push({ ...file, coldVerifiedAt: result.verifiedAt });
        await this.#update(datasetId, { verifiedFiles: verified.length });
      }

      const manifest = await this.cold.readManifest(partitionId, tmpVersion);
      // 先把临时版本转正，使目录库中的 versionId 与实际可查询的版本一致。
      finalVersion = await this.#finalizeVersion(partitionId, tmpVersion, versionId);
      this.catalog.notePartitionVersion(partitionId, finalVersion);

      // 关键切换点：冷副本全部校验通过才登记；登记录盘后才允许删热。
      this.catalog.commitColdReplicas(datasetId, {
        partitionId,
        versionId: finalVersion,
        checksum: manifest.checksum,
        files: verified.map((file) => ({
          fileId: file.fileId,
          sha256: file.sha256,
          size: file.size,
        })),
        evidence: '冷版本构建后逐对象复算 SHA-256，并与目录记录比对一致',
        actor: job.createdBy ?? 'user',
      });
      coldCommitted = true;
      this.#update(datasetId, { phase: 'cold-committed', committedFiles: verified.length });
      await this.catalog.flush();
      if (signal.aborted) throw this.#abort(job);

      await this.#update(datasetId, { phase: 'hot-reclaiming', status: 'switching' }, { persist: true });
      const current = this.catalog.requireDataset(datasetId);
      // 只删除那些已经有冷副本、且内容未变的文件。
      const removable = current.files
        .filter((file) => file.hot && file.cold && file.hot.sha256 === file.cold.sha256)
        .map((file) => ({ id: file.id, path: file.hot.path, size: file.hot.size ?? 0 }));
      let freed = 0;
      let deleted = 0;
      const droppedIds = [];
      for (const target of removable) {
        if (signal.aborted) break;
        // 删除前再确认一次：候选列表生成后，并发回收可能已把这份热副本降级。
        const live = this.catalog.getDataset(datasetId)?.files.find((file) => file.id === target.id);
        if (!live?.hot || live.hot.path !== target.path) continue;
        // 删除前等待读者租约排空：不会删掉正在被查询读取的文件。
        freed += await this.hot.removeFile(target.path);
        deleted += 1;
        droppedIds.push(target.id);
        this.#update(datasetId, { deletedHotFiles: deleted, freedBytes: freed });
      }
      if (deleted > 0) this.catalog.addReclaimed(freed);
      if (signal.aborted) {
        // 部分热副本已回收、其余保留。无论哪种情况每文件仍有冷副本。
        this.catalog.dropHotReplicas(datasetId, {
          evidence: '取消后回收已完成的部分热副本，所有文件仍有已校验冷副本',
          reason: '迁移取消，仅回收已完成切换部分的冗余热副本',
          fileIds: droppedIds,
        });
        this.#update(datasetId, {
          status: 'cancelled',
          phase: 'cancelled',
          finishedAt: nowIso(),
          cancelRequested: true,
          note: job.cancelReason ?? '迁移取消；冷副本已登记，热副本按完成度回收',
        });
        this.catalog.addEvent({
          kind: 'migration.cancelled',
          datasetId,
          reason: job.cancelReason ?? '迁移取消',
          evidence: { versionId: finalVersion, committedFiles: verified.length, hotRemoved: deleted },
        });
        await this.catalog.flush();
        return { status: 'cancelled', versionId: finalVersion };
      }

      const committedIds = droppedIds;
      const dropped = this.catalog.dropHotReplicas(datasetId, {
        evidence: '全部冷对象读回复算一致，热副本租约已排空',
        actor: job.createdBy ?? 'user',
        reason: '迁移完成，冷副本可读，回收热盘空间',
        fileIds: committedIds,
      });
      const remainingHot = this.catalog.requireDataset(datasetId).files
        .filter((file) => file.hot).length;
      const partial = remainingHot > 0;
      this.#update(datasetId, {
        status: partial ? 'partial' : 'completed',
        phase: 'done',
        finishedAt: nowIso(),
        deletedHotFiles: dropped,
        freedBytes: freed,
        note: partial
          ? `迁移完成但保留 ${remainingHot} 个热副本（迁移期间版本/内容变化或文件未提交）`
          : `迁移完成，回收热盘 ${freed} 字节`,
      });
      this.catalog.addEvent({
        kind: partial ? 'migration.partial' : 'migration.completed',
        datasetId,
        from: 'hot',
        to: partial ? 'hot+cold' : 'cold',
        reason: partial
          ? `冷版本 ${finalVersion} 已提交；${remainingHot} 个文件保留热副本`
          : `冷版本 ${finalVersion} 校验通过，热副本已回收`,
        evidence: { versionId: finalVersion, freedBytes: freed, files: verified.length },
        actor: job.createdBy ?? 'user',
      });
      await this.catalog.flush();
      return { status: 'completed', versionId: finalVersion };
    } catch (error) {
      const cancelled = error.code === 'ABORTED';
      // 无论失败在哪一步，先按磁盘实况修正“可读位置”：已删掉的热副本不能继续
      // 标记为可读，仍有冷副本的文件回退到冷层。
      await this.#reconcileHotAfterFailure(datasetId);
      await this.cold.deleteVersion(partitionId, tmpVersion).catch(() => {});
      if (!coldCommitted) {
        // 转正过但未登记到目录库：这份冷版本无人引用，可以删。
        await this.cold.deleteVersion(partitionId, finalVersion).catch(() => {});
        this.catalog.forgetPartitionVersion(partitionId, finalVersion);
      }
      this.#update(datasetId, {
        status: cancelled ? 'cancelled' : 'failed',
        phase: cancelled ? 'cancelled' : 'failed',
        finishedAt: nowIso(),
        cancelRequested: cancelled,
        error: error.message,
        note: coldCommitted
          ? (cancelled ? '迁移取消；冷副本已登记并保留，热副本未回收' : '迁移失败；冷副本已登记，热副本保留')
          : (cancelled ? '迁移取消，热副本保留，冷版本已清除' : '迁移失败，热副本保留，冷版本已清除'),
      });
      this.catalog.addEvent({
        kind: cancelled ? 'migration.cancelled' : 'migration.failed',
        datasetId,
        reason: error.message,
        evidence: { coldCommitted, code: error.code },
      });
      await this.catalog.flush();
      throw error;
    } finally {
      this.jobs.delete(datasetId);
    }
  }

  async #reconcileHotAfterFailure(datasetId) {
    const { pathExists } = await import('./util.js');
    const dataset = this.catalog.getDataset(datasetId);
    if (!dataset) return;
    for (const file of [...dataset.files]) {
      if (!file.hot?.path) continue;
      if (await pathExists(file.hot.path)) continue;
      this.catalog.markHotMissing(datasetId, file.id, {
        coldExists: Boolean(file.cold),
        detail: '（迁移失败后磁盘上已不存在）',
      });
    }
  }

  #abort(job) {
    const error = new Error(job.cancelReason ?? '迁移已取消');
    error.code = 'ABORTED';
    error.statusCode = 499;
    return error;
  }

  /** 把 tmp 版本目录转正；manifest 的临时标记与 versionId 一并更新。 */
  async #finalizeVersion(partitionId, tmpVersionId, finalVersionId) {
    const { rename } = await import('node:fs/promises');
    const tmpRoot = this.cold.versionRoot(partitionId, tmpVersionId);
    const finalRoot = this.cold.versionRoot(partitionId, finalVersionId);
    const manifest = await readJson(`${tmpRoot}/manifest.json`, null);
    if (manifest) {
      manifest.versionId = finalVersionId;
      manifest.temporary = false;
      await atomicWriteJson(`${tmpRoot}/manifest.json`, manifest);
    }
    if (tmpRoot !== finalRoot) {
      try {
        await rename(tmpRoot, finalRoot);
      } catch (error) {
        if (!['ENOENT', 'EEXIST', 'ENOTEMPTY'].includes(error.code)) throw error;
        // 目标已存在：保留旧目标，丢弃临时版本（重试路径）。
        await this.cold.deleteVersion(partitionId, tmpVersionId).catch(() => {});
      }
    }
    return finalVersionId;
  }
}
