/**
 * 空间回收。
 *
 * 回收对象分两类：
 *  1. 热盘上的孤儿内容文件：没有任何目录记录引用，且没有读者租约。
 *  2. 冷分区中不再被目录引用的旧版本：必须同时满足
 *       - 距离被替换超过 versionGraceMs（给刚结束/刚开始的查询留缓冲）
 *       - 没有查询/取回任务通过 VersionPins 固定该版本
 *     否则跳过并如实报告“因租约跳过”。
 *
 * 迁移/回热/回收并发时，先登记的副本永远先于删除存在，因此不会删掉唯一副本。
 */
import { nowIso, randomId } from './util.js';

export class Reclaimer {
  constructor({
    catalog, cold, hot, pins, log = () => {},
  }) {
    this.catalog = catalog;
    this.cold = cold;
    this.hot = hot;
    this.pins = pins;
    this.log = log;
    this.running = null;
  }

  /** 计算回收计划，不修改任何状态。 */
  plan() {
    const state = this.catalog.snapshot();
    const referencedHot = new Set();
    const referencedCold = new Map(); // partitionId -> Set(versionId)
    for (const dataset of state.datasets) {
      for (const file of dataset.files) {
        if (file.hot?.path) referencedHot.add(file.hot.path);
        if (file.cold?.partitionId && file.cold.versionId) {
          if (!referencedCold.has(file.cold.partitionId)) referencedCold.set(file.cold.partitionId, new Set());
          referencedCold.get(file.cold.partitionId).add(file.cold.versionId);
        }
      }
    }
    const coldCandidates = [];
    const skippedVersions = [];
    for (const partition of state.partitions) {
      const referenced = referencedCold.get(partition.id) ?? new Set();
      for (const versionId of partition.versionIds ?? []) {
        if (referenced.has(versionId)) continue;
        const pinnedCount = this.pins.count(partition.id, versionId);
        if (pinnedCount > 0) {
          skippedVersions.push({
            partitionId: partition.id, versionId, reason: `被 ${pinnedCount} 个进行中的查询固定`,
          });
          continue;
        }
        const replacedAt = this.#replacedAt(state, partition, versionId);
        const ageMs = replacedAt ? Date.now() - Date.parse(replacedAt) : Infinity;
        const graceMs = Number(state.settings.versionGraceMs ?? 0);
        if (ageMs < graceMs) {
          skippedVersions.push({
            partitionId: partition.id,
            versionId,
            reason: `宽限期内（剩余 ${Math.ceil((graceMs - ageMs) / 1000)}s）`,
          });
          continue;
        }
        coldCandidates.push({ partitionId: partition.id, versionId, replacedAt });
      }
    }
    return { referencedHot, referencedCold, coldCandidates, skippedVersions };
  }

  #replacedAt(state, partition, versionId) {
    // 优先用目录记录的退役时间；旧数据没有元数据时退回事件时间，再退回“现在”，
    // 保证刚启动的旧版本也会先经过一个宽限期。
    const meta = partition.versionMeta?.[versionId];
    if (meta?.retiredAt) return meta.retiredAt;
    if (meta?.addedAt) return meta.addedAt;
    const events = state.events.filter((event) => event.evidence?.versionId === versionId
      || event.evidence?.partitionId === partition.id);
    const created = [...events].reverse().find((event) => event.kind === 'migration.completed');
    return created?.at ?? nowIso();
  }

  /**
   * 超出热盘容量时，按最久未读顺序把仍有冷副本的热文件降级。
   * 不删除唯一副本；有读者租约的文件跳过，等下次回收。
   */
  async #evictToCapacity(state, actions, dryRun) {
    const cap = Number(state.settings.hotCapacityBytes ?? 0);
    if (!cap) return { freed: 0, evicted: 0 };
    let hotBytes = await this.hot.usageBytes({ fresh: true });
    if (hotBytes <= cap) return { freed: 0, evicted: 0 };
    const candidates = [];
    for (const dataset of state.datasets) {
      for (const file of dataset.files) {
        if (!file.hot?.path || !file.cold) continue;
        if (file.cold.sha256 !== file.hot.sha256) continue;
        // 快照路径与大小：候选列表构建后到真正删除之间有 await，
        // 期间其他操作可能已把热副本降级并置空 file.hot。
        candidates.push({
          dataset, file, hotPath: file.hot.path, size: file.hot.size ?? 0,
        });
      }
    }
    candidates.sort((a, b) => String(a.file.lastReadAt ?? '').localeCompare(String(b.file.lastReadAt ?? '')));
    let freed = 0;
    let evicted = 0;
    for (const { dataset, file, hotPath, size } of candidates) {
      if (hotBytes - freed <= cap) break;
      const active = this.hot.leases.activeFor(hotPath);
      if (active > 0) {
        actions.push({
          type: 'hot-skip', datasetId: dataset.id, path: hotPath, bytes: 0,
          reason: `超出容量但仍有 ${active} 个读者，留待下次回收`,
        });
        continue;
      }
      const current = this.catalog.getDataset(dataset.id)?.files.find((item) => item.id === file.id);
      if (!current?.hot || current.hot.path !== hotPath || !current.cold
        || current.cold.sha256 !== current.hot.sha256) {
        continue;
      }
      actions.push({
        type: dryRun ? 'hot-capacity-evict' : 'hot-capacity-evicted',
        datasetId: dataset.id,
        path: hotPath,
        fileId: file.id,
        bytes: size,
        reason: `热盘超出容量，按最久未读降级；冷副本 ${file.cold.versionId} 已校验`,
      });
      freed += size;
      evicted += 1;
      if (!dryRun) {
        await this.hot.removeFile(hotPath);
        this.catalog.dropHotReplicas(dataset.id, {
          evidence: '热盘容量回收：冷副本摘要一致，热副本可安全降级',
          reason: '热盘容量回收',
          fileIds: [file.id],
        });
        this.catalog.countEviction();
        this.catalog.addReclaimed(size);
      }
    }
    return { freed, evicted };
  }

  /**
   * 执行一次回收。dryRun 只统计不删除。
   * 返回每个动作的字节数与原因，供网页展示“为什么删/为什么跳过”。
   */
  async reclaim({ dryRun = false, actor = 'user' } = {}) {
    if (this.running) return this.running;
    this.running = this.#reclaim({ dryRun, actor }).finally(() => { this.running = null; });
    return this.running;
  }

  async #reclaim({ dryRun, actor }) {
    const startedAt = Date.now();
    const runId = `rc_${randomId('')}`;
    const plan = this.plan();
    const state = this.catalog.snapshot();
    const actions = [];
    let freed = 0;

    for (const dataset of state.datasets) {
      const referenced = new Set(dataset.files.map((file) => file.hot?.path).filter(Boolean));
      for (const path of referenced) {
        const activeLeases = this.hot.leases.activeFor(path);
        if (activeLeases > 0) {
          actions.push({
            type: 'hot-skip', datasetId: dataset.id, path, bytes: 0,
            reason: `有 ${activeLeases} 个进行中的读者持有租约`,
          });
        }
      }
      let files = [];
      try {
        files = await this.hot.listFiles(dataset.id);
      } catch {
        files = [];
      }
      for (const file of files) {
        if (referenced.has(file.path)) continue;
        const activeLeases = this.hot.leases.activeFor(file.path);
        if (activeLeases > 0) {
          actions.push({
            type: 'hot-skip', datasetId: dataset.id, path: file.path, bytes: file.size,
            reason: `有 ${activeLeases} 个进行中的读者持有租约`,
          });
          continue;
        }
        // 快照之后可能有回热刚把这份内容登记为热副本：删除前用最新目录再确认。
        const isReferencedNow = () => (this.catalog.getDataset(dataset.id)?.files ?? [])
          .some((candidate) => candidate.hot?.path === file.path);
        if (isReferencedNow()) continue;
        if (dryRun) {
          actions.push({
            type: 'hot-orphan', datasetId: dataset.id, path: file.path, bytes: file.size,
            reason: '没有目录引用的孤儿热文件',
          });
          freed += file.size;
          continue;
        }
        // keep 在租约排空之后、unlink 之前执行：期间若有回热登记，则这次不删。
        const bytes = await this.hot.removeFile(file.path, { keep: async () => isReferencedNow() });
        if (!bytes) continue;
        freed += bytes;
        actions.push({
          type: 'hot-removed', datasetId: dataset.id, path: file.path, bytes,
          reason: '没有目录引用的孤儿热文件，已删除',
        });
      }
    }

    for (const candidate of plan.coldCandidates) {
      // 计划生成后可能有查询固定该版本，删除前再确认一次。
      if (this.pins.count(candidate.partitionId, candidate.versionId) > 0) {
        actions.push({
          type: 'cold-version-skipped',
          partitionId: candidate.partitionId,
          versionId: candidate.versionId,
          bytes: 0,
          reason: '计划生成后被进行中的查询固定，跳过',
        });
        continue;
      }
      if (dryRun) {
        const versions = await this.cold.listVersions(candidate.partitionId).catch(() => []);
        const version = versions.find((item) => item.versionId === candidate.versionId);
        actions.push({
          type: 'cold-version', partitionId: candidate.partitionId, versionId: candidate.versionId,
          bytes: version?.bytes ?? 0,
          reason: '不再被目录引用，宽限期已过，无查询固定',
        });
        freed += version?.bytes ?? 0;
        continue;
      }
      const size = await this.cold.deleteVersion(candidate.partitionId, candidate.versionId);
      this.catalog.forgetPartitionVersion(candidate.partitionId, candidate.versionId);
      freed += size;
      actions.push({
        type: 'cold-version-removed', partitionId: candidate.partitionId, versionId: candidate.versionId,
        bytes: size,
        reason: '不再被目录引用，宽限期已过，无查询固定',
      });
    }
    for (const skipped of plan.skippedVersions) {
      actions.push({
        type: 'cold-version-skipped',
        partitionId: skipped.partitionId,
        versionId: skipped.versionId,
        bytes: 0,
        reason: skipped.reason,
      });
    }

    const capacity = await this.#evictToCapacity(state, actions, dryRun);
    freed += capacity.freed;

    if (!dryRun) {
      this.catalog.addReclaimed(freed);
      this.catalog.addEvent({
        kind: 'reclaim.completed',
        datasetId: null,
        reason: `回收 ${freed} 字节（${actions.filter((action) => /removed|evicted/.test(action.type)).length} 项，容量降级 ${capacity.evicted} 个）`,
        evidence: { runId, freed, actions },
        actor,
      });
      await this.catalog.flush();
    }
    return {
      runId,
      dryRun,
      freedBytes: freed,
      durationMs: Date.now() - startedAt,
      actions,
    };
  }
}
