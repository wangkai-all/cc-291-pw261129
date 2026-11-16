/**
 * 目录库（catalog）是“每份数据当前可读位置”的唯一事实来源。
 *
 * 设计要点：
 *  - 每个文件记录 hot/cold 两份副本各自的摘要与校验依据，readable 只描述
 *    当前真正能读到的那一层。
 *  - 位置切换遵守固定顺序：新副本先写入并校验 → 登记目录 → 持久化 → 才允许
 *    回收旧副本。任何一步失败都不会出现“唯一副本已被删除”。
 *  - generation 单调递增，便于前端判断并发写是否推进了状态。
 *  - history/events 保留每次位置变更的校验依据，供网页展示。
 */
import { EventEmitter } from 'node:events';
import path from 'node:path';
import {
  atomicWriteJson, ensureDir, makeError, nowIso, partitionChecksum,
  randomId, readJson,
} from './util.js';

const CATALOG_FILE = 'catalog.json';
const HISTORY_LIMIT = 40;
const EVENT_LIMIT = 2000;
const RETRIEVAL_LIMIT = 400;
const QUERY_LIMIT = 400;

const DEFAULT_SETTINGS = () => ({
  hotCapacityBytes: 512 * 1024 * 1024,
  hotPath: '',
  coldLatencyBaseMs: 35,
  coldBandwidthBytesPerSec: 6 * 1024 * 1024,
  coldFaultMode: 'none',
  coldFaultFileId: '',
  cancelPolicy: 'continue', // continue | cancel-if-idle
  versionGraceMs: 60_000,
  maxVerifyRetries: 1,
});

const DEFAULT_STATE = () => ({
  format: 1,
  generation: 0,
  nextDatasetSeq: 1,
  nextVersionSeq: 1,
  settings: DEFAULT_SETTINGS(),
  partitions: [],
  datasets: [],
  events: [],
  retrievals: [],
  queries: [],
  stats: {
    coldFetchBytes: 0,
    coldFetchCount: 0,
    coldTasks: 0,
    sharedJoins: 0,
    hotEvictions: 0,
    reclaimedBytes: 0,
    verificationFailures: 0,
    detachedContinues: 0,
    detachedAborts: 0,
    capacityEvictions: 0,
  },
});

function pushHistory(file, entry) {
  file.history = file.history ?? [];
  file.history.push(entry);
  if (file.history.length > HISTORY_LIMIT) file.history.splice(0, file.history.length - HISTORY_LIMIT);
}

function updateReadable(file) {
  if (file.hot) file.readable = 'hot';
  else if (file.cold) file.readable = 'cold';
  else file.readable = 'none';
  const layers = [];
  if (file.hot) layers.push('hot');
  if (file.cold) layers.push('cold');
  file.replica = layers.join('+') || 'none';
}

export class Catalog extends EventEmitter {
  constructor({ dir }) {
    super();
    this.dir = dir;
    this.file = path.join(dir, CATALOG_FILE);
    this.state = DEFAULT_STATE();
    this.writeChain = Promise.resolve();
    this.dirty = false;
    this.writing = false;
    this.pendingWrites = 0;
  }

  async load({ hotRoot } = {}) {
    await ensureDir(this.dir);
    const state = await readJson(this.file, null);
    if (state) {
      this.state = {
        ...DEFAULT_STATE(),
        ...state,
        settings: { ...DEFAULT_SETTINGS(), ...state.settings },
      };
      this.state.stats = { ...DEFAULT_STATE().stats, ...state.stats };
    } else {
      this.state.settings.hotPath = hotRoot ?? '';
      await this.persist();
    }
    if (hotRoot && !this.state.settings.hotPath) this.state.settings.hotPath = hotRoot;
  }

  /**
   * 合并落盘：高并发下每秒可能有数百次位置/查询更新，如果每个请求都排一次
   * 原子写，写链会远远落后于内存状态。这里只保留“脏”标记，写循环反复写最新
   * 快照，队列深度最大为 1，读操作因此不会被历史版本的写拖住。
   */
  persist() {
    this.dirty = true;
    if (this.writing) return this.writeChain;
    this.writing = true;
    this.pendingWrites += 1;
    this.writeChain = this.writeChain
      .catch(() => {})
      .then(async () => {
        while (this.dirty) {
          this.dirty = false;
          // 快照在每次写之前生成，保证写入的是最新状态而不是排队时捕获的旧状态。
          const snapshot = structuredClone(this.state);
          await atomicWriteJson(this.file, snapshot);
        }
      })
      .finally(() => {
        this.pendingWrites = Math.max(0, this.pendingWrites - 1);
        this.writing = false;
      });
    return this.writeChain;
  }

  async flush() {
    await this.persist();
    await this.writeChain;
    // 写循环退出后若又有新的脏标记（极小概率），再等一轮。
    if (this.dirty) {
      await this.persist();
      await this.writeChain;
    }
  }

  snapshot() {
    return structuredClone(this.state);
  }

  #touch() {
    this.state.generation += 1;
  }

  #pushCapped(list, item, limit) {
    list.push(item);
    if (list.length > limit) list.splice(0, list.length - limit);
  }

  #event({ kind, datasetId, fileId = null, from = null, to = null, reason = '', evidence = {}, actor = 'system' }) {
    const event = {
      id: randomId('evt_'),
      at: nowIso(),
      kind,
      datasetId,
      fileId,
      from,
      to,
      reason,
      evidence,
      actor,
    };
    this.#pushCapped(this.state.events, event, EVENT_LIMIT);
    this.emit('event', event);
    return event;
  }

  addEvent(args) {
    this.#touch();
    const event = this.#event(args);
    this.persist().catch((error) => this.emit('persist-error', error));
    return event;
  }

  settings() {
    return { ...this.state.settings };
  }

  async updateSettings(patch) {
    Object.assign(this.state.settings, patch);
    this.#touch();
    await this.persist();
    return this.settings();
  }

  listDatasets() {
    return structuredClone(this.state.datasets);
  }

  getDataset(datasetId) {
    return this.state.datasets.find((dataset) => dataset.id === datasetId) ?? null;
  }

  requireDataset(datasetId) {
    const dataset = this.getDataset(datasetId);
    if (!dataset) throw makeError('数据集不存在', 'DATASET_NOT_FOUND', 404);
    return dataset;
  }

  findFile(datasetId, fileId) {
    const dataset = this.requireDataset(datasetId);
    const file = dataset.files.find((candidate) => candidate.id === fileId);
    if (!file) throw makeError('文件不存在', 'FILE_NOT_FOUND', 404);
    return { dataset, file };
  }

  async reserveIds({ dataset = false, version = false } = {}) {
    const ids = {};
    if (dataset) {
      ids.datasetId = `ds_${String(this.state.nextDatasetSeq).padStart(3, '0')}`;
      this.state.nextDatasetSeq += 1;
    }
    if (version) {
      ids.versionId = `cv_${String(this.state.nextVersionSeq).padStart(3, '0')}`;
      this.state.nextVersionSeq += 1;
    }
    this.#touch();
    await this.persist();
    return ids;
  }

  createDataset({ id, name, description = '', files, totalBytes, hotDir }) {
    let datasetId = id;
    if (!datasetId) {
      datasetId = `ds_${String(this.state.nextDatasetSeq).padStart(3, '0')}`;
      this.state.nextDatasetSeq += 1;
    }
    const checksum = partitionChecksum(files);
    const dataset = {
      id: datasetId,
      name,
      description,
      createdAt: nowIso(),
      updatedAt: nowIso(),
      currentVersion: 1,
      hotDir,
      coldPartition: null,
      coldVersion: null,
      checksum,
      totalBytes,
      fileCount: files.length,
      files: files.map((file, index) => {
        const entry = {
          id: `f_${String(index + 1).padStart(3, '0')}`,
          name: file.name,
          size: file.size,
          sha256: file.sha256,
          version: 1,
          hot: {
            path: file.hotPath,
            sha256: file.sha256,
            size: file.size,
            verifiedAt: nowIso(),
            evidence: '热盘写入后复算 SHA-256 与生成内容一致',
          },
          cold: null,
          readable: 'hot',
          replica: 'hot',
          lastError: null,
          history: [{
            at: nowIso(),
            action: 'created',
            from: null,
            to: 'hot',
            sha256: file.sha256,
            evidence: '热盘写入后复算 SHA-256 一致',
          }],
        };
        return entry;
      }),
      migration: null,
      lastVerifiedAt: nowIso(),
      lastQueryAt: null,
    };
    this.state.datasets.push(dataset);
    this.#touch();
    this.#event({
      kind: 'dataset.created',
      datasetId,
      reason: `建立热副本（${files.length} 个文件）`,
      evidence: { checksum, verifiedFiles: files.length },
      actor: 'user',
    });
    this.persist().catch((error) => this.emit('persist-error', error));
    return structuredClone(dataset);
  }

  addPartition({ name, description = '' }) {
    const partitionId = `pt_${String(this.state.partitions.length + 1).padStart(2, '0')}`;
    const partition = {
      id: partitionId,
      name,
      description,
      createdAt: nowIso(),
      versionIds: [],
    };
    this.state.partitions.push(partition);
    this.#touch();
    this.#event({
      kind: 'partition.created',
      datasetId: null,
      reason: `创建冷分区 ${name}`,
      evidence: { partitionId },
      actor: 'user',
    });
    this.persist().catch((error) => this.emit('persist-error', error));
    return structuredClone(partition);
  }

  getPartition(partitionId) {
    return this.state.partitions.find((partition) => partition.id === partitionId) ?? null;
  }

  requirePartition(partitionId) {
    const partition = this.getPartition(partitionId);
    if (!partition) throw makeError('冷分区不存在', 'PARTITION_NOT_FOUND', 404);
    return partition;
  }

  notePartitionVersion(partitionId, versionId) {
    const partition = this.getPartition(partitionId);
    if (!partition) return;
    partition.versionIds ??= [];
    partition.versionMeta ??= {};
    if (!partition.versionIds.includes(versionId)) partition.versionIds.push(versionId);
    partition.versionMeta[versionId] ??= { addedAt: nowIso(), retiredAt: null };
  }

  /** 标记某个分区里除 currentVersionId 之外的版本“已退役”，宽限期从这里开始算。 */
  retireOtherVersions(partitionId, currentVersionId) {
    const partition = this.getPartition(partitionId);
    if (!partition) return;
    partition.versionMeta ??= {};
    for (const versionId of partition.versionIds ?? []) {
      if (versionId === currentVersionId) continue;
      partition.versionMeta[versionId] ??= { addedAt: nowIso(), retiredAt: null };
      if (!partition.versionMeta[versionId].retiredAt) {
        partition.versionMeta[versionId].retiredAt = nowIso();
      }
    }
  }

  forgetPartitionVersion(partitionId, versionId) {
    const partition = this.getPartition(partitionId);
    if (!partition) return;
    partition.versionIds = (partition.versionIds ?? []).filter((id) => id !== versionId);
  }

  setMigration(datasetId, migration) {
    const dataset = this.requireDataset(datasetId);
    dataset.migration = migration;
    dataset.updatedAt = nowIso();
    this.#touch();
    this.persist().catch((error) => this.emit('persist-error', error));
  }

  updateMigrationProgress(datasetId, patch, { persist = false } = {}) {
    const dataset = this.requireDataset(datasetId);
    if (!dataset.migration) return;
    Object.assign(dataset.migration, patch);
    this.#touch();
    if (persist) this.persist().catch((error) => this.emit('persist-error', error));
  }

  /**
   * 登记一批冷副本。只有写入并逐对象校验通过的条目才会进来；
   * 提交完成后才允许调用方删除热副本。
   */
  commitColdReplicas(datasetId, { partitionId, versionId, checksum, files, evidence, actor = 'system' }) {
    const dataset = this.requireDataset(datasetId);
    for (const update of files) {
      const file = dataset.files.find((candidate) => candidate.id === update.fileId);
      if (!file) throw makeError(`提交未知文件 ${update.fileId}`, 'UNKNOWN_FILE', 500);
      if (file.sha256 !== update.sha256) {
        // 版本在迁移期间被替换过。拒绝把旧内容登记到新版本上。
        continue;
      }
      const from = file.readable;
      file.cold = {
        partitionId,
        versionId,
        objectId: update.fileId,
        sha256: update.sha256,
        size: update.size,
        verifiedAt: nowIso(),
        evidence: evidence ?? '冷对象读回复算 SHA-256 与目录记录一致',
      };
      file.lastError = null;
      updateReadable(file);
      pushHistory(file, {
        at: nowIso(),
        action: 'cold-committed',
        from,
        to: file.readable,
        sha256: update.sha256,
        versionId,
        evidence: evidence ?? '冷对象 SHA-256 校验一致',
      });
    }
    dataset.coldPartition = partitionId;
    dataset.coldVersion = versionId;
    // 目录 checksum 描述“当前文件集合”，不等于某个冷版本的 manifest 校验和。
    dataset.checksum = partitionChecksum(dataset.files.map((file) => ({
      name: file.name, size: file.size, sha256: file.sha256,
    })));
    dataset.updatedAt = nowIso();
    dataset.lastVerifiedAt = nowIso();
    this.notePartitionVersion(partitionId, versionId);
    this.retireOtherVersions(partitionId, versionId);
    this.#touch();
    this.#event({
      kind: 'migration.cold-committed',
      datasetId,
      from: 'hot',
      to: 'hot+cold',
      reason: `冷版本 ${versionId} 已登记，${files.length} 个文件校验一致`,
      evidence: { partitionId, versionId, checksum, files: files.length },
      actor,
    });
    this.persist().catch((error) => this.emit('persist-error', error));
  }

  /**
   * 登记回热得到的副本。若数据集在取回期间已被替换版本，则拒绝旧内容
   * （返回 accepted=false），但取回的字节仍可供固定版本的查询读取。
   */
  commitHotReplicas(datasetId, { files, evidence, actor = 'system' }) {
    const dataset = this.requireDataset(datasetId);
    const accepted = [];
    const rejected = [];
    for (const update of files) {
      const file = dataset.files.find((candidate) => candidate.id === update.fileId);
      if (!file) continue;
      if (file.sha256 !== update.sha256) {
        rejected.push(update.fileId);
        continue;
      }
      const from = file.readable;
      file.hot = {
        path: update.path,
        sha256: update.sha256,
        size: update.size,
        verifiedAt: nowIso(),
        evidence: evidence ?? '冷存储取回后复算 SHA-256 与冷对象一致',
      };
      file.lastError = null;
      updateReadable(file);
      pushHistory(file, {
        at: nowIso(),
        action: 'reheated',
        from,
        to: file.readable,
        sha256: update.sha256,
        evidence: evidence ?? '取回复算 SHA-256 与冷对象一致',
      });
      accepted.push(update.fileId);
    }
    if (accepted.length) {
      const firstHot = dataset.files.find((file) => file.hot?.path);
      if (firstHot && dataset.files.every((file) => file.hot)) {
        dataset.hotDir = path.dirname(firstHot.hot.path);
      }
    }
    dataset.updatedAt = nowIso();
    this.#touch();
    if (accepted.length) {
      this.#event({
        kind: 'reheat.hot-committed',
        datasetId,
        from: 'cold',
        to: 'hot+cold',
        reason: `${accepted.length} 个文件回热并校验一致`,
        evidence: { files: accepted },
        actor,
      });
    }
    if (rejected.length) {
      this.#event({
        kind: 'reheat.stale-discarded',
        datasetId,
        reason: '回热期间数据版本已替换，旧内容未登记为当前热副本',
        evidence: { files: rejected },
      });
    }
    this.persist().catch((error) => this.emit('persist-error', error));
    return { accepted, rejected };
  }

  /** 删除热副本。调用方必须已确认每个文件都有校验过的冷副本。 */
  dropHotReplicas(datasetId, { evidence, actor = 'system', reason = '', fileIds = null }) {
    const dataset = this.requireDataset(datasetId);
    const targets = fileIds ? new Set(fileIds) : null;
    let dropped = 0;
    for (const file of dataset.files) {
      if (targets && !targets.has(file.id)) continue;
      if (!file.cold) {
        throw makeError(`拒绝删除唯一副本：${file.name} 没有冷副本`, 'LAST_REPLICA', 409);
      }
      if (!file.hot) continue;
      if (file.cold.sha256 !== file.hot.sha256) {
        // 冷副本已是新版本：热盘上的旧内容可能是定版查询的唯一来源，不能删。
        continue;
      }
      const from = file.readable;
      file.hot = null;
      file.lastError = null;
      updateReadable(file);
      pushHistory(file, {
        at: nowIso(),
        action: 'hot-dropped',
        from,
        to: file.readable,
        sha256: file.sha256,
        evidence: evidence ?? '冷副本校验一致，热副本可安全回收',
      });
      dropped += 1;
    }
    if (dropped > 0) {
      if (dataset.files.every((file) => !file.hot)) dataset.hotDir = null;
      dataset.updatedAt = nowIso();
      this.#touch();
      this.#event({
        kind: 'hot.evicted',
        datasetId,
        from: 'hot',
        to: 'cold',
        reason: reason || '热副本已回收，冷副本校验一致',
        evidence: { ...(evidence ?? {}), dropped },
        actor,
      });
    }
    this.persist().catch((error) => this.emit('persist-error', error));
    return dropped;
  }

  markHotMissing(datasetId, fileId, { coldExists, detail = '' }) {
    const { file } = this.findFile(datasetId, fileId);
    if (!file.hot) return;
    const from = file.readable;
    file.hot = null;
    updateReadable(file);
    file.lastError = coldExists
      ? `热副本缺失，已回退到冷副本${detail}`
      : `热副本缺失且没有冷副本${detail}`;
    pushHistory(file, {
      at: nowIso(),
      action: 'hot-missing',
      from,
      to: file.readable,
      sha256: file.sha256,
      evidence: file.lastError,
    });
    this.#touch();
    this.#event({
      kind: coldExists ? 'recovery.hot-missing' : 'recovery.no-replica',
      datasetId,
      fileId,
      from,
      to: file.readable,
      reason: file.lastError,
    });
    this.persist().catch((error) => this.emit('persist-error', error));
  }

  markFileError(datasetId, fileId, message, { evidence = {}, kind = 'file.error' } = {}) {
    const { file } = this.findFile(datasetId, fileId);
    file.lastError = message;
    this.#touch();
    this.#event({ kind, datasetId, fileId, reason: message, evidence });
    this.persist().catch((error) => this.emit('persist-error', error));
  }

  setPendingReheat(datasetId, fileId, pending, taskInfo = {}) {
    const { file } = this.findFile(datasetId, fileId);
    if (pending) file.pendingReheat = { at: nowIso(), ...taskInfo };
    else delete file.pendingReheat;
    this.#touch();
  }

  /**
   * 慢存储版本替换：新内容校验完成后，一次性把一批文件切换到新版本。
   * 旧冷版本仍保留（不再被目录引用），进行中的固定版本查询还能继续读。
   */
  commitVersionReplacement(datasetId, {
    partitionId, versionId, checksum, files, evidence, actor = 'user', note = '',
  }) {
    const dataset = this.requireDataset(datasetId);
    for (const update of files) {
      const file = dataset.files.find((candidate) => candidate.id === update.fileId);
      if (!file) continue;
      const from = file.readable;
      file.version += 1;
      file.sha256 = update.sha256;
      file.size = update.size;
      file.hot = null;
      file.cold = {
        partitionId,
        versionId,
        objectId: file.id,
        sha256: update.sha256,
        size: update.size,
        verifiedAt: nowIso(),
        evidence: evidence ?? '新冷版本对象读回复算 SHA-256 一致',
      };
      file.lastError = null;
      updateReadable(file);
      pushHistory(file, {
        at: nowIso(),
        action: 'version-replaced',
        from,
        to: file.readable,
        sha256: update.sha256,
        versionId,
        evidence: evidence ?? '新冷版本逐对象校验一致，旧版本保留',
      });
    }
    dataset.currentVersion += 1;
    dataset.coldPartition = partitionId;
    dataset.coldVersion = versionId;
    this.retireOtherVersions(partitionId, versionId);
    dataset.checksum = checksum ?? partitionChecksum(files.map((file) => ({
      name: dataset.files.find((candidate) => candidate.id === file.fileId)?.name ?? file.fileId,
      size: file.size,
      sha256: file.sha256,
    })));
    dataset.totalBytes = dataset.files.reduce((sum, file) => sum + file.size, 0);
    dataset.updatedAt = nowIso();
    dataset.lastVerifiedAt = nowIso();
    this.notePartitionVersion(partitionId, versionId);
    this.#touch();
    this.#event({
      kind: 'version.replaced',
      datasetId,
      from: 'cold',
      to: 'cold',
      reason: note || `慢存储版本切换到 ${versionId}；旧版本保留给固定版本查询`,
      evidence: { partitionId, versionId, checksum, files: files.length },
      actor,
    });
    this.persist().catch((error) => this.emit('persist-error', error));
  }

  recordFileRead(datasetId, fileId, from) {
    const { file, dataset } = this.findFile(datasetId, fileId);
    file.lastReadAt = nowIso();
    file.lastReadFrom = from;
    dataset.lastQueryAt = file.lastReadAt;
    this.#touch();
  }

  addRetrieval(record) {
    this.#pushCapped(this.state.retrievals, record, RETRIEVAL_LIMIT);
    this.#touch();
    this.persist().catch((error) => this.emit('persist-error', error));
  }

  updateRetrieval(id, patch) {
    const record = this.state.retrievals.find((item) => item.id === id);
    if (!record) return null;
    Object.assign(record, patch);
    this.#touch();
    this.persist().catch((error) => this.emit('persist-error', error));
    return record;
  }

  addQuery(record) {
    this.#pushCapped(this.state.queries, record, QUERY_LIMIT);
    this.#touch();
  }

  updateQuery(id, patch, { persist = true } = {}) {
    const record = this.state.queries.find((item) => item.id === id);
    if (!record) return null;
    Object.assign(record, patch);
    this.#touch();
    if (persist) this.persist().catch((error) => this.emit('persist-error', error));
    return record;
  }

  addColdBytes(bytes) {
    this.state.stats.coldFetchBytes += bytes;
    this.#touch();
  }

  countColdTask() {
    this.state.stats.coldTasks += 1;
    this.state.stats.coldFetchCount += 1;
    this.#touch();
  }

  countSharedJoin() {
    this.state.stats.sharedJoins += 1;
    this.#touch();
  }

  countEviction() {
    this.state.stats.hotEvictions += 1;
    this.#touch();
  }

  countDetached(continued) {
    if (continued) this.state.stats.detachedContinues += 1;
    else this.state.stats.detachedAborts += 1;
    this.#touch();
  }

  countVerifyFailure() {
    this.state.stats.verificationFailures += 1;
    this.#touch();
  }

  addReclaimed(bytes) {
    this.state.stats.reclaimedBytes += bytes;
    this.#touch();
  }

  /** 走查每个文件的副本情况，生成一致性报告。 */
  analyze() {
    const problems = [];
    for (const dataset of this.state.datasets) {
      for (const file of dataset.files) {
        const hasHot = Boolean(file.hot);
        const hasCold = Boolean(file.cold);
        const base = { datasetId: dataset.id, datasetName: dataset.name, fileId: file.id, fileName: file.name };
        if (!hasHot && !hasCold) {
          problems.push({ ...base, code: 'NO_REPLICA', severity: 'critical', message: `${file.name} 没有任何可用副本` });
        }
        if (file.readable === 'hot' && !hasHot) {
          problems.push({ ...base, code: 'READABLE_HOT_MISSING', severity: 'warning', message: `${file.name} 标记为热可读但热副本缺失` });
        }
        if (file.readable === 'cold' && !hasCold) {
          problems.push({ ...base, code: 'READABLE_COLD_MISSING', severity: 'critical', message: `${file.name} 标记为冷可读但冷副本缺失` });
        }
        if (hasHot && hasCold && file.hot.sha256 !== file.cold.sha256) {
          problems.push({ ...base, code: 'REPLICA_MISMATCH', severity: 'warning', message: `${file.name} 热/冷副本摘要不一致` });
        }
        if (file.readable === 'none' && file.lastError) {
          // NO_REPLICA 已报告，避免重复。
        }
      }
      if (dataset.migration && dataset.migration.status === 'running'
        && Date.now() - Date.parse(dataset.migration.heartbeatAt ?? dataset.migration.startedAt) > 120_000) {
        problems.push({
          datasetId: dataset.id, datasetName: dataset.name, fileId: null, fileName: null,
          code: 'MIGRATION_STALLED', severity: 'warning', message: `迁移 ${dataset.migration.id} 长时间无进展`,
        });
      }
    }
    return problems;
  }

  /** 进程恢复：中断的迁移/回热/查询保留原可读位置并标记，不删任何副本。 */
  async recoverInterrupted() {
    const notes = [];
    for (const dataset of this.state.datasets) {
      if (dataset.migration && dataset.migration.status === 'running') {
        dataset.migration.status = 'interrupted';
        dataset.migration.phase = 'interrupted';
        dataset.migration.finishedAt = nowIso();
        dataset.migration.note = '进程重启时迁移未完成；保留原可读位置，不删除任何唯一副本';
        notes.push({ datasetId: dataset.id, kind: 'migration.interrupted' });
        this.#event({
          kind: 'migration.interrupted',
          datasetId: dataset.id,
          reason: dataset.migration.note,
          evidence: { committedFiles: dataset.migration.committedFiles ?? 0 },
        });
      }
      if (dataset.migration && dataset.migration.status === 'switching') {
        dataset.migration.status = 'interrupted';
        dataset.migration.note = '进程重启时迁移处于切换阶段；冷副本已登记，热副本按磁盘实况回收';
        notes.push({ datasetId: dataset.id, kind: 'migration.interrupted-switch' });
      }
      for (const file of dataset.files) {
        if (file.pendingReheat) {
          file.lastError = '回热在进程重启时中断，原可读位置保持不变';
          delete file.pendingReheat;
          notes.push({ datasetId: dataset.id, fileId: file.id, kind: 'reheat.interrupted' });
          this.#event({
            kind: 'reheat.interrupted',
            datasetId: dataset.id,
            fileId: file.id,
            reason: file.lastError,
          });
        }
      }
    }
    for (const record of this.state.retrievals) {
      if (record.status === 'running') {
        record.status = 'interrupted';
        record.finishedAt = nowIso();
        record.error = '进程重启，取回任务中止';
        notes.push({ datasetId: record.datasetId, kind: 'retrieval.interrupted', id: record.id });
      }
    }
    for (const query of this.state.queries) {
      if (query.status === 'running') {
        query.status = 'interrupted';
        query.finishedAt = nowIso();
        query.error = '进程重启，查询中止';
      }
    }
    if (notes.length) {
      this.#touch();
      await this.persist();
    }
    return notes;
  }
}
