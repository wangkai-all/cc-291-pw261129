import path from 'node:path';
import { mkdtemp, readdir, stat } from 'node:fs/promises';
import { rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { createApp } from '../src/app.js';

// 测试结束时清理自己创建的临时数据目录（进程退出时同步删除，避免 /tmp 堆积）。
const tempRoots = new Set();
let cleanupRegistered = false;
function registerCleanup(root) {
  tempRoots.add(root);
  if (cleanupRegistered) return;
  cleanupRegistered = true;
  process.on('exit', () => {
    for (const dir of tempRoots) {
      try { rmSync(dir, { recursive: true, force: true }); } catch { /* 忽略清理失败 */ }
    }
  });
}

export async function tempRoot(label = 'tier-test') {
  const root = await mkdtemp(path.join(tmpdir(), `${label}-`));
  registerCleanup(root);
  return root;
}

export async function makeApp(options = {}) {
  const root = options.dataRoot ?? await tempRoot();
  const app = await createApp({ dataRoot: root });
  if (options.settings) await app.catalog.updateSettings(options.settings);
  return { app, root };
}

export async function seedDataset(app, { name = 'ds', count = 3, sizeBytes = 64 * 1024, seed = 'x' } = {}) {
  return app.createDataset({ name, count, sizeBytes, seed });
}

export async function migrate(app, datasetId) {
  const partition = app.catalog.addPartition({ name: `p-${datasetId}` });
  app.migrations.start(datasetId, { partitionId: partition.id });
  return app.migrations.waitFor(datasetId);
}

export async function waitFor(fn, { timeoutMs = 15_000, intervalMs = 25, label = 'condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`等待超时：${label}`);
}

export function fileOf(app, datasetId, index = 0) {
  return app.catalog.requireDataset(datasetId).files[index];
}

export async function exists(target) {
  try { await stat(target); return true; } catch { return false; }
}

export async function listDirRecursive(dir) {
  const out = [];
  async function walk(current) {
    let entries;
    try { entries = await readdir(current, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      out.push(full);
      if (entry.isDirectory()) await walk(full);
    }
  }
  await walk(dir);
  return out;
}
