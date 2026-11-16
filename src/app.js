/**
 * 应用装配：目录库、热盘、冷服务、迁移、取回、版本替换、回收、恢复。
 * 这一层不关心 HTTP，便于测试直接驱动。
 */
import path from 'node:path';
import { Catalog } from './catalog.js';
import { ColdService } from './cold.js';
import { HotStore } from './storage.js';
import { MigrationRunner } from './migrations.js';
import { RetrievalManager } from './retrievals.js';
import { QueryEngine } from './queries.js';
import { Reclaimer } from './reclaim.js';
import { VersionReplacer } from './versioning.js';
import { VersionPins } from './versions.js';
import { Recovery } from './recovery.js';
import { runComparison } from './benchmark.js';
import { generateFiles } from './samples.js';
import {
  ensureDir, partitionChecksum, sha256Hex, sleep,
} from './util.js';
import { readFile as readFileFs, writeFile as writeFileFs } from 'node:fs/promises';

export async function createApp({
  dataRoot, log = () => {},
}) {
  const root = path.resolve(dataRoot);
  const hotRoot = path.join(root, 'hot');
  const coldRoot = path.join(root, 'cold');
  const catalogDir = path.join(root, 'catalog');
  await ensureDir(hotRoot);
  await ensureDir(coldRoot);
  await ensureDir(catalogDir);

  const catalog = new Catalog({ dir: catalogDir });
  await catalog.load({ hotRoot });
  const cold = new ColdService({ root: coldRoot, getSettings: () => catalog.settings() });
  await cold.init();
  const hot = new HotStore({ root: hotRoot });
  await hot.init();
  const pins = new VersionPins();
  const retrievals = new RetrievalManager({
    catalog, cold, hot, pins, log,
  });
  const migrations = new MigrationRunner({ catalog, cold, hot, log });
  const versioning = new VersionReplacer({ catalog, cold, hot, pins, log });
  const reclaimer = new Reclaimer({ catalog, cold, hot, pins, log });
  const recovery = new Recovery({ catalog, cold, hot, log });
  const queries = new QueryEngine({ catalog, cold, hot, retrievals, pins, log });

  /** 创建数据集：把内容写到热盘并校验，再登记目录（登记前不删任何东西）。 */
  async function createDataset({
    name, description = '', count = 4, sizeBytes = 256 * 1024, seed = null,
  }) {
    const { datasetId } = await catalog.reserveIds({ dataset: true });
    const datasetSeed = seed ?? datasetId;
    const generated = generateFiles({ seed: datasetSeed, count, sizeBytes, prefix: 'part' });
    const files = [];
    for (let index = 0; index < generated.length; index += 1) {
      const file = generated[index];
      const fileId = `f_${String(index + 1).padStart(3, '0')}`;
      const target = hot.contentPath(datasetId, fileId, sha256Hex(file.content));
      await ensureDir(path.dirname(target));
      await writeFileFs(target, file.content);
      const actual = await sha256Hex(await readFileFs(target));
      if (actual !== file.sha256) {
        throw Object.assign(new Error('热盘写入后校验失败'), { code: 'HOT_WRITE_VERIFY_FAILED' });
      }
      files.push({
        name: file.name,
        size: file.size,
        sha256: file.sha256,
        hotPath: target,
      });
    }
    const dataset = catalog.createDataset({
      id: datasetId,
      name,
      description,
      files,
      totalBytes: files.reduce((sum, file) => sum + file.size, 0),
      hotDir: hot.datasetRoot(datasetId),
    });
    await catalog.flush();
    return dataset;
  }

  /**
   * 主动放弃热副本（仅当每个文件都有校验通过的冷副本）。用于演示冷读与回收。
   * 先等读者租约排空再删文件，然后一次性登记 readable=cold。
   */
  async function evictHot(datasetId, { reason = '手动释放热副本以便观察冷读' } = {}) {
    const dataset = catalog.requireDataset(datasetId);
    if (dataset.files.some((file) => !file.cold)) {
      throw Object.assign(new Error('存在没有冷副本的文件，拒绝释放热副本'), {
        code: 'LAST_REPLICA', statusCode: 409,
      });
    }
    let freed = 0;
    const targets = [];
    for (const file of dataset.files) {
      if (!file.hot?.path) continue;
      if (file.cold.sha256 !== file.hot.sha256) continue;
      targets.push({ id: file.id, path: file.hot.path });
    }
    for (const target of targets) {
      const live = catalog.getDataset(datasetId)?.files.find((file) => file.id === target.id);
      if (!live?.hot || live.hot.path !== target.path || !live.cold) continue;
      freed += await hot.removeFile(target.path);
    }
    const dropped = catalog.dropHotReplicas(datasetId, {
      evidence: '冷副本均已校验；读者租约已排空',
      reason,
    });
    if (freed) catalog.addReclaimed(freed);
    await catalog.flush();
    return { dropped, freed };
  }

  function summary() {
    const state = catalog.snapshot();
    return {
      ...state,
      serverTime: new Date().toISOString(),
      runtime: {
        migrations: migrations.active(),
        versionReplacements: versioning.active(),
        retrievals: retrievals.list(),
        pins: pins.list(),
        activeQueries: queries.activeReads(),
        reclaimRunning: Boolean(reclaimer.running),
      },
      consistency: catalog.analyze(),
    };
  }

  async function shutdown() {
    for (const job of migrations.jobs.values()) job.controller.abort();
    for (const job of versioning.jobs.values()) job.controller.abort();
    await sleep(10);
    await catalog.flush();
  }

  return {
    root,
    hotRoot,
    coldRoot,
    catalog,
    cold,
    hot,
    pins,
    retrievals,
    migrations,
    versioning,
    reclaimer,
    recovery,
    queries,
    createDataset,
    evictHot,
    runComparison: (options) => runComparison({
      createDataset,
      catalog,
      hot,
      queries,
      migrations,
    }, options),
    summary,
    shutdown,
    generateFiles,
    ensureDir,
    partitionChecksum,
  };
}
