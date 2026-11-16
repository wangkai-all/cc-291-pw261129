/**
 * 查询引擎：把“数据在哪一层、如何取回、如何校验、取消后共享任务是否继续”
 * 串成一条可观测的读取路径。
 *
 * 读取策略：
 *  - 热层命中：校验热文件摘要后直接流式返回。
 *  - 冷层命中 + mode=file：加入/创建“单文件共享取回”，校验通过后从热层读取。
 *  - 冷层命中 + mode=reheat-partition：加入/创建“整分区共享取回”，完成后从热层读取。
 *
 * 版本固定：查询开始时固定 sha256 与冷版本；期间发生的版本替换不会改变这次查询
 * 读到的内容，旧冷版本在查询结束前不会被空间回收删除。
 */
import { createReadStream } from 'node:fs';
import { nowIso, randomId, sha256File } from './util.js';

function readAll(filePath) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    createReadStream(filePath)
      .on('data', (chunk) => chunks.push(chunk))
      .on('end', () => resolve(Buffer.concat(chunks)))
      .on('error', reject);
  });
}

export class QueryEngine {
  constructor({
    catalog, cold, hot, retrievals, pins, log = () => {},
  }) {
    this.catalog = catalog;
    this.cold = cold;
    this.hot = hot;
    this.retrievals = retrievals;
    this.pins = pins;
    this.log = log;
    this.active = new Set();
  }

  activeReads() {
    return this.active.size;
  }

  async #resolveVersion(dataset, versionId) {
    const partitionId = await this.#partitionForVersion(dataset, versionId);
    if (!partitionId) throw this.#error('冷版本不存在', 'COLD_VERSION_NOT_FOUND', 404);
    const manifest = await this.cold.readManifest(partitionId, versionId);
    return { manifest, partitionId };
  }

  async #partitionForVersion(dataset, versionId) {
    if (dataset.coldVersion === versionId) return dataset.coldPartition;
    for (const partition of this.catalog.state.partitions) {
      if ((partition.versionIds ?? []).includes(versionId)) return partition.id;
    }
    for (const partition of this.catalog.state.partitions) {
      if (await this.cold.versionExists(partition.id, versionId)) return partition.id;
    }
    return null;
  }

  /**
   * 返回 { record, body, headers }。body 为完整内容（示例数据规模可控）。
   */
  async run({
    datasetId, fileId, mode = 'auto', versionId = null, signal, actor = 'user', clientId = null,
  }) {
    const startedAt = Date.now();
    const { dataset, file } = this.catalog.findFile(datasetId, fileId);
    const queryId = `q_${randomId('')}`;
    const strategy = mode === 'reheat-partition'
      ? 'partition-reheat'
      : (mode === 'file' ? 'file-fetch' : 'auto');
    const record = {
      id: queryId,
      datasetId,
      datasetName: dataset.name,
      fileId,
      fileName: file.name,
      mode,
      strategy,
      clientId,
      status: 'running',
      startedAt: nowIso(),
      finishedAt: null,
      firstByteAt: null,
      firstByteMs: null,
      coldBytes: 0,
      hotBytes: 0,
      cacheBytesBefore: 0,
      cacheBytesAfter: 0,
      readFrom: null,
      versionId: null,
      pinnedSha256: file.sha256,
      sharedJoin: false,
      taskId: null,
      taskContinuedAfterCancel: null,
      cancelPolicy: this.catalog.settings().cancelPolicy,
      verification: null,
      error: null,
    };
    this.catalog.addQuery(record);
    this.active.add(queryId);
    let releasePin = null;
    let taskSub = null;
    const coldBytesAtStart = this.catalog.state.stats.coldFetchBytes;
    try {
      record.cacheBytesBefore = await this.hot.usageBytes();

      let targetSha = file.sha256;
      let targetVersion = null;
      let entryName = file.name;
      let partitionId = file.cold?.partitionId ?? dataset.coldPartition;
      if (versionId) {
        const { manifest, partitionId: foundPartition } = await this.#resolveVersion(dataset, versionId);
        targetVersion = versionId;
        partitionId = foundPartition;
        const entry = manifest.files.find((candidate) => candidate.fileId === fileId);
        if (!entry) throw this.#error('该版本中没有此文件', 'VERSION_FILE_NOT_FOUND', 404);
        targetSha = entry.sha256;
        entryName = entry.name;
      } else if (file.cold) {
        targetVersion = file.cold.versionId;
      }
      record.versionId = targetVersion;
      record.pinnedSha256 = targetSha;
      if (targetVersion && partitionId) {
        releasePin = this.pins.pin(partitionId, targetVersion);
      }

      const hotCandidate = file.hot && file.hot.sha256 === targetSha ? file.hot : null;
      if (hotCandidate) {
        try {
          return await this.#readHot({
            datasetId, fileId, hotCandidate, targetSha, record, startedAt, entryName, actor,
          });
        } catch (error) {
          // 热副本校验失败时不让查询失败：标记失效并回退到已校验的冷副本。
          if (!file.cold) throw error;
          if (!['HOT_VERIFY_FAILED', 'HOT_MISSING', 'HOT_DRAINING'].includes(error.code)) throw error;
          record.hotFallbackReason = `${error.code}：${error.message}`;
          this.catalog.addEvent({
            kind: 'query.hot-fallback',
            datasetId,
            fileId,
            reason: `热层读取失败，回退冷层：${error.message}`,
            evidence: { queryId, code: error.code },
            actor,
          });
        }
      }
      if (!file.cold) throw this.#error('该文件没有可用副本', 'NO_READABLE_REPLICA', 503);
      if (!targetVersion) targetVersion = file.cold.versionId;
      if (!partitionId) partitionId = file.cold.partitionId;
      if (!partitionId || !targetVersion) {
        throw this.#error('冷副本元数据不完整', 'COLD_METADATA_MISSING', 500);
      }
      const scope = mode === 'reheat-partition' ? 'dataset' : 'file';
      const { task, shared } = this.retrievals.ensure({
        datasetId,
        scope,
        fileId: scope === 'file' ? fileId : null,
        partitionId,
        versionId: targetVersion,
        reason: scope === 'file' ? '按文件读取触发共享取回' : '整分区回热触发共享取回',
      });
      record.taskId = task.id;
      record.sharedJoin = shared;
      if (shared) this.catalog.countSharedJoin();
      const subscription = this.retrievals.subscribe(task.id, null);
      taskSub = subscription;
      if (!subscription) throw this.#error('共享取回任务不可用', 'TASK_GONE', 500);

      const cancelPolicy = this.catalog.settings().cancelPolicy;
      const onAbort = () => {
        const outcome = this.retrievals.unsubscribe(task.id, subscription.subscription);
        taskSub = null;
        record.status = 'cancelled';
        record.finishedAt = nowIso();
        record.taskContinuedAfterCancel = outcome.taskContinues;
        record.error = outcome.taskContinues
          ? '查询已取消；共享取回任务继续下载并缓存，供后续查询复用'
          : '查询已取消；没有其他订阅者，按策略中止共享取回任务';
        this.catalog.updateQuery(queryId, {
          status: record.status,
          finishedAt: record.finishedAt,
          taskContinuedAfterCancel: record.taskContinuedAfterCancel,
          error: record.error,
          coldBytes: Math.max(0, this.catalog.state.stats.coldFetchBytes - coldBytesAtStart),
        });
        this.catalog.addEvent({
          kind: 'query.cancelled',
          datasetId,
          fileId,
          reason: record.error,
          evidence: {
            queryId, taskId: task.id, policy: cancelPolicy,
          },
          actor,
        });
      };
      if (signal) {
        if (signal.aborted) {
          onAbort();
          throw this.#error('查询已取消', 'ABORTED', 499);
        }
        signal.addEventListener('abort', onAbort, { once: true });
      }
      try {
        await task.promise;
      } catch (error) {
        if (signal?.aborted) throw this.#error('查询已取消', 'ABORTED', 499);
        const current = this.catalog.findFile(datasetId, fileId).file;
        throw this.#error(
          `冷存储取回失败：${error.message}；可读位置保持为 ${current.readable}`,
          error.code ?? 'RETRIEVAL_FAILED',
          502,
        );
      } finally {
        signal?.removeEventListener('abort', onAbort);
        if (taskSub) {
          this.retrievals.unsubscribe(task.id, taskSub.subscription);
          taskSub = null;
        }
      }
      if (signal?.aborted) throw this.#error('查询已取消', 'ABORTED', 499);
      if (task.status === 'failed') {
        const current = this.catalog.findFile(datasetId, fileId).file;
        throw this.#error(
          `冷存储取回失败：${task.error}；可读位置保持为 ${current.readable}`,
          task.failures?.at(-1)?.code ?? 'RETRIEVAL_FAILED',
          502,
        );
      }
      if (signal?.aborted) throw this.#error('查询已取消', 'ABORTED', 499);

      record.coldBytes = Math.max(0, this.catalog.state.stats.coldFetchBytes - coldBytesAtStart);
      const { file: refreshed } = this.catalog.findFile(datasetId, fileId);
      // 固定版本查询优先读同摘要热副本；若期间版本被替换，内容路径仍按 sha 寻址。
      const warm = refreshed.hot && refreshed.hot.sha256 === targetSha ? refreshed.hot : null;
      const fallbackPath = await this.hot.contentPathIfExists(datasetId, fileId, targetSha);
      const hotPath = warm?.path ?? fallbackPath;
      if (hotPath) {
        try {
          const result = await this.#readHot({
            datasetId,
            fileId,
            hotCandidate: warm ?? { path: hotPath, sha256: targetSha, size: undefined },
            targetSha,
            record,
            startedAt,
            entryName,
            actor,
            fromCold: true,
            task,
          });
          return result;
        } catch (error) {
          if (!['HOT_VERIFY_FAILED', 'HOT_MISSING', 'HOT_DRAINING'].includes(error.code)) throw error;
          record.hotFallbackReason = `${error.code}：${error.message}`;
        }
      }
      // 最后一层保障：热副本在取回完成前被回收时，直接从固定冷版本校验读取。
      return await this.#readColdDirect({
        partitionId,
        versionId: targetVersion,
        fileId,
        targetSha,
        record,
        startedAt,
        entryName,
        actor,
      });
    } catch (error) {
      if (taskSub) {
        const outcome = this.retrievals.unsubscribe(taskSub.taskId, taskSub.subscription);
        record.taskContinuedAfterCancel = outcome.taskContinues;
      }
      if (!record.finishedAt) {
        record.status = error.code === 'ABORTED' ? 'cancelled' : 'failed';
        record.finishedAt = nowIso();
        record.error = error.message;
        record.readFrom = this.catalog.getDataset(datasetId)
          ?.files.find((item) => item.id === fileId)?.readable ?? null;
        record.cacheBytesAfter = await this.hot.usageBytes().catch(() => record.cacheBytesBefore);
        this.catalog.updateQuery(queryId, {
          status: record.status,
          finishedAt: record.finishedAt,
          error: record.error,
          readFrom: record.readFrom,
          taskContinuedAfterCancel: record.taskContinuedAfterCancel,
          cacheBytesAfter: record.cacheBytesAfter,
          coldBytes: Math.max(0, this.catalog.state.stats.coldFetchBytes - coldBytesAtStart),
        });
        this.catalog.addEvent({
          kind: error.code === 'ABORTED' ? 'query.cancelled' : 'query.failed',
          datasetId,
          fileId,
          reason: error.message,
          evidence: { queryId, code: error.code, readFrom: record.readFrom },
          actor,
        });
      }
      throw error;
    } finally {
      this.active.delete(queryId);
      releasePin?.();
      // 查询结束只请求合并落盘，不阻塞在 fsync 上；位置变更仍由操作方显式 flush。
      this.catalog.persist().catch(() => {});
    }
  }

  async #readHot({
    datasetId, fileId, hotCandidate, targetSha, record, startedAt, entryName, actor,
    fromCold = false, task = null,
  }) {
    const path = hotCandidate.path;
    // 先尝试取租约；若回收已完成，会直接以 HOT_DRAINING 拒绝，查询可回退冷层。
    const releaseLease = this.hot.leases.acquire(path);
    try {
      // sha256File 返回 null 表示文件已不存在或不可读。
      const actual = await sha256File(path).catch(() => null);
      if (actual !== targetSha) {
        const current = this.catalog.requireDataset(datasetId).files
          .find((candidate) => candidate.id === fileId);
        this.catalog.markHotMissing(datasetId, fileId, {
          coldExists: Boolean(current.cold),
          detail: actual === null ? '（文件缺失）' : '（摘要不一致）',
        });
        throw this.#error('热副本校验失败，已标记失效；请改用冷层重试', 'HOT_VERIFY_FAILED', 503);
      }
      const body = await readAll(path);
      const elapsed = Date.now() - startedAt;
      record.readFrom = 'hot';
      record.firstByteMs = elapsed;
      record.firstByteAt = nowIso();
      record.hotBytes = body.length;
      record.status = 'completed';
      record.finishedAt = nowIso();
      record.cacheBytesAfter = await this.hot.usageBytes().catch(() => record.cacheBytesBefore);
      record.verification = {
        layer: 'hot',
        expectedSha256: targetSha,
        actualSha256: actual,
        ok: actual === targetSha,
        evidence: hotCandidate.evidence ?? '读取前复算热副本 SHA-256 与目录记录一致',
        verifiedAt: hotCandidate.verifiedAt ?? nowIso(),
      };
      record.headers = {
        'x-tier-read-from': 'hot',
        'x-tier-sha256': targetSha,
        'x-tier-verified': 'sha256',
        'x-tier-version': record.versionId ?? 'current',
        'x-tier-query': record.id,
      };
      if (task) {
        record.headers['x-tier-shared-task'] = task.id;
        record.headers['x-tier-shared-join'] = String(record.sharedJoin);
      }
      this.catalog.recordFileRead(datasetId, fileId, 'hot');
      this.catalog.updateQuery(record.id, {
        status: record.status,
        finishedAt: record.finishedAt,
        firstByteAt: record.firstByteAt,
        firstByteMs: record.firstByteMs,
        readFrom: 'hot',
        hotBytes: record.hotBytes,
        coldBytes: record.coldBytes,
        verification: record.verification,
        cacheBytesAfter: record.cacheBytesAfter,
        versionId: record.versionId,
        sharedJoin: record.sharedJoin,
        taskId: record.taskId,
      });
      this.catalog.addEvent({
        kind: 'query.completed',
        datasetId,
        fileId,
        reason: `热层命中 ${entryName}，${record.firstByteMs}ms，${body.length} 字节`,
        evidence: {
          queryId: record.id,
          sha256: targetSha,
          fromCold,
          sharedJoin: record.sharedJoin,
          versionId: record.versionId,
        },
        actor,
      });
      await this.catalog.flush();
      return { record, body, headers: record.headers };
    } finally {
      releaseLease();
    }
  }

  /**
   * 直接冷读（不登记热副本）：用于热副本在取回后消失、但仍需保持读版的场景。
   * 同样做 SHA-256 校验，失败则不返回任何字节。
   */
  async #readColdDirect({
    partitionId, versionId, fileId, targetSha, record, startedAt, entryName, actor,
  }) {
    const staging = await this.hot.stage(`direct-${record.id}`, fileId);
    try {
      const result = await this.cold.retrieveFile({
        partitionId,
        versionId,
        fileId,
        destPath: staging,
        signal: undefined,
      });
      if (result.sha256 !== targetSha) {
        this.catalog.countVerifyFailure();
        throw this.#error('冷对象校验失败，拒绝返回未校验内容', 'COLD_VERIFY_FAILED', 502);
      }
      const body = await readAll(staging);
      record.readFrom = 'cold-direct';
      record.firstByteMs = Date.now() - startedAt;
      record.firstByteAt = nowIso();
      record.coldBytes = Math.max(record.coldBytes, result.bytes);
      record.status = 'completed';
      record.finishedAt = nowIso();
      record.verification = {
        layer: 'cold',
        expectedSha256: targetSha,
        actualSha256: result.sha256,
        ok: true,
        evidence: '冷对象读取后复算 SHA-256 与固定版本 manifest 一致；未登记为热副本',
        verifiedAt: nowIso(),
      };
      record.headers = {
        'x-tier-read-from': 'cold-direct',
        'x-tier-sha256': targetSha,
        'x-tier-verified': 'sha256',
        'x-tier-version': versionId,
        'x-tier-query': record.id,
      };
      this.catalog.addColdBytes(result.bytes);
      this.catalog.updateQuery(record.id, record);
      this.catalog.addEvent({
        kind: 'query.completed-cold-direct',
        datasetId: record.datasetId,
        fileId: record.fileId,
        reason: `固定版本 ${versionId} 直接冷读 ${entryName}，校验一致`,
        evidence: { queryId: record.id, sha256: targetSha, bytes: result.bytes },
        actor,
      });
      return { record, body, headers: record.headers };
    } finally {
      await this.hot.discardStaging(`direct-${record.id}`).catch(() => {});
    }
  }

  #error(message, code, statusCode) {
    const error = new Error(message);
    error.code = code;
    error.statusCode = statusCode;
    return error;
  }
}
