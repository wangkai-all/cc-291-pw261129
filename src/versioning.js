/**
 * 慢存储版本替换。
 *
 *  - 新版本先在冷存储构建并逐对象校验，再一次性提交到目录库。
 *  - 旧冷版本不立即删除：目录不再引用它，但进行中的定版查询靠 VersionPins
 *    继续读；空间回收要等租约释放且宽限期结束。
 *  - 替换失败/取消时旧版本与当前可读位置完全不变。
 */
import { writeFile } from 'node:fs/promises';
import { nowIso, randomId, sha256Hex } from './util.js';
import { generateFiles } from './samples.js';

export class VersionReplacer {
  constructor({
    catalog, cold, hot, pins, log = () => {},
  }) {
    this.catalog = catalog;
    this.cold = cold;
    this.hot = hot;
    this.pins = pins;
    this.log = log;
    this.jobs = new Map();
  }

  active() {
    return [...this.jobs.values()].map((job) => ({
      datasetId: job.datasetId,
      versionId: job.versionId,
      startedAt: job.startedAt,
      cancelRequested: job.controller.signal.aborted,
    }));
  }

  cancel(datasetId, reason = '用户取消版本替换') {
    const job = this.jobs.get(datasetId);
    if (!job) return false;
    job.cancelReason = reason;
    job.controller.abort();
    return true;
  }

  /**
   * 用新生成的数据替换整个数据集的冷版本。
   * seed 相同则内容相同——可用于演示“替换但内容未变”，不同则每份文件摘要都变。
   */
  start(datasetId, {
    partitionId = null, seed = null, actor = 'user', sizeDelta = 0,
  } = {}) {
    if (this.jobs.has(datasetId)) {
      const error = new Error('该数据集已有版本替换在进行');
      error.code = 'VERSION_REPLACE_RUNNING';
      error.statusCode = 409;
      throw error;
    }
    const dataset = this.catalog.requireDataset(datasetId);
    const targetPartition = partitionId ?? dataset.coldPartition;
    if (!targetPartition) {
      const error = new Error('数据集没有冷分区，无法替换冷版本');
      error.code = 'NO_COLD_PARTITION';
      error.statusCode = 409;
      throw error;
    }
    const controller = new AbortController();
    const job = {
      datasetId,
      versionId: `cv_${randomId('')}`,
      partitionId: targetPartition,
      seed: seed ?? `${dataset.id}-v${dataset.currentVersion + 1}`,
      sizeDelta,
      startedAt: nowIso(),
      controller,
      cancelReason: null,
      done: null,
      result: null,
      actor,
    };
    const pending = {
      id: `vp_${randomId('')}`,
      versionId: job.versionId,
      partitionId: targetPartition,
      status: 'running',
      startedAt: job.startedAt,
      phase: 'cold-writing',
      writtenBytes: 0,
      totalBytes: 0,
      note: '',
      error: null,
    };
    dataset.pendingVersion = pending;
    this.catalog.addEvent({
      kind: 'version.replacement-started',
      datasetId,
      reason: `开始构建新冷版本 ${job.versionId}`,
      evidence: { partitionId: targetPartition, seed: job.seed },
      actor,
    });
    job.done = this.#run(job, pending)
      .then((result) => { job.result = result; return result; })
      .catch((error) => {
        const dataset = this.catalog.getDataset(datasetId);
        job.result = {
          status: dataset?.pendingVersion?.status ?? 'failed',
          error: error.message,
          code: error.code,
          versionId: job.versionId,
        };
        throw error;
      })
      .finally(() => { this.jobs.delete(datasetId); });
    job.done.catch(() => {});
    this.jobs.set(datasetId, job);
    return structuredClone(pending);
  }

  async waitFor(datasetId) {
    const job = this.jobs.get(datasetId);
    if (!job) {
      const pending = this.catalog.getDataset(datasetId)?.pendingVersion;
      if (!pending) return null;
      return {
        status: pending.status,
        versionId: pending.versionId ?? null,
        error: pending.error ?? null,
      };
    }
    try {
      await job.done;
    } catch {
      // 失败/取消已写入 job.result；这里不再把取消误报成失败。
    }
    return job.result;
  }

  async #run(job, pending) {
    const { datasetId, versionId, partitionId, controller } = job;
    const signal = controller.signal;
    let committed = false;
    try {
      const dataset = this.catalog.requireDataset(datasetId);
      const files = generateFiles({
        seed: job.seed,
        count: dataset.files.length,
        sizeBytes: 256 * 1024 + job.sizeDelta,
        prefix: 'part',
      }).map((file, index) => ({
        ...file,
        fileId: dataset.files[index].id,
      }));
      const totalBytes = files.reduce((sum, file) => sum + file.size, 0);
      pending.totalBytes = totalBytes;

      // 冷服务需要源文件路径，先把新内容写到热盘暂存区（不会登记为目录热副本）。
      const staging = `version-${versionId}`;
      const sources = [];
      for (const file of files) {
        const stagedPath = await this.hot.stage(staging, `${file.fileId}-${job.seed}-${randomId('')}`);
        await writeFile(stagedPath, file.content);
        sources.push({
          fileId: file.fileId,
          name: file.name,
          size: file.size,
          sha256: sha256Hex(file.content),
          sourcePath: stagedPath,
        });
      }
      const tmpVersion = `${versionId}_tmp`;
      try {
        await this.cold.putVersion({
          partitionId,
          versionId: tmpVersion,
          files: sources,
          signal,
          temporary: true,
          onProgress: (progress) => { pending.writtenBytes = progress.bytes; },
        });
      } finally {
        await this.hot.discardStaging(staging).catch(() => {});
      }

      if (signal.aborted) throw Object.assign(new Error(job.cancelReason ?? '版本替换已取消'), { code: 'ABORTED' });
      pending.phase = 'verifying';
      for (const file of sources) {
        const result = await this.cold.verifyObject(partitionId, tmpVersion, file.fileId);
        if (!result.ok) {
          throw Object.assign(new Error(`新冷版本对象校验失败：${file.name}`), { code: 'VERSION_VERIFY_FAILED' });
        }
      }

      // 转正：manifest 里的 versionId 与目录一致。
      const { rename } = await import('node:fs/promises');
      const { atomicWriteJson, readJson } = await import('./util.js');
      const tmpRoot = this.cold.versionRoot(partitionId, tmpVersion);
      const finalRoot = this.cold.versionRoot(partitionId, versionId);
      const manifest = await readJson(`${tmpRoot}/manifest.json`, null);
      if (manifest) {
        manifest.versionId = versionId;
        manifest.temporary = false;
        delete manifest.fingerprint;
        await atomicWriteJson(`${tmpRoot}/manifest.json`, manifest);
      }
      await rename(tmpRoot, finalRoot);
      const finalManifest = await this.cold.readManifest(partitionId, versionId);

      if (signal.aborted) throw Object.assign(new Error(job.cancelReason ?? '版本替换已取消'), { code: 'ABORTED' });

      const checksum = finalManifest.checksum;
      this.catalog.commitVersionReplacement(datasetId, {
        partitionId,
        versionId,
        checksum,
        files: sources.map((file) => ({
          fileId: file.fileId, sha256: file.sha256, size: file.size,
        })),
        evidence: '新冷版本逐对象复算 SHA-256，并与构建输入比对一致；旧版本保留给定版查询',
        actor: job.actor,
        note: `版本替换到 ${versionId}；旧冷版本按租约与宽限期回收`,
      });
      committed = true;
      pending.status = 'completed';
      pending.phase = 'done';
      pending.finishedAt = nowIso();
      pending.note = `新冷版本 ${versionId} 已生效，旧版本保留`;
      delete this.catalog.requireDataset(datasetId).pendingVersion;
      await this.catalog.flush();
      return { status: 'completed', versionId, checksum };
    } catch (error) {
      const cancelled = error.code === 'ABORTED';
      await this.cold.deleteVersion(partitionId, `${versionId}_tmp`).catch(() => {});
      if (!committed) {
        // versionId 是本任务随机生成的，从未登记到目录库，失败时可直接删除。
        await this.cold.deleteVersion(partitionId, versionId).catch(() => {});
      }
      const dataset = this.catalog.requireDataset(datasetId);
      dataset.pendingVersion = {
        ...pending,
        status: cancelled ? 'cancelled' : 'failed',
        phase: 'failed',
        finishedAt: nowIso(),
        error: error.message,
        note: committed ? '已提交' : '未提交；当前可读位置与版本不变',
      };
      this.catalog.addEvent({
        kind: cancelled ? 'version.replacement-cancelled' : 'version.replacement-failed',
        datasetId,
        reason: error.message,
        evidence: { versionId, committed },
      });
      await this.catalog.flush();
      throw error;
    }
  }
}
