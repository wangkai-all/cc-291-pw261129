import test from 'node:test';
import assert from 'node:assert/strict';
import { startServer } from '../src/server.js';
import { tempRoot } from './helpers.js';

async function withServer(fn) {
  const dataRoot = await tempRoot('tier-http');
  const started = await startServer({ port: 0, dataRoot, quiet: true });
  try {
    await fn(started);
  } finally {
    await started.close();
  }
}

test('HTTP：完整流程（建数据集 → 迁移 → 冷读 → 回热 → 回收 → 恢复）', async () => {
  await withServer(async ({ url }) => {
    const post = async (path, body = {}) => {
      const response = await fetch(`${url}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      });
      return { status: response.status, body: await response.json(), headers: response.headers };
    };
    const get = async (path) => {
      const response = await fetch(`${url}${path}`);
      return { status: response.status, body: await response.json() };
    };

    const settings = await post('/api/settings', {
      coldLatencyBaseMs: 1, coldBandwidthBytesPerSec: 16 * 1024 * 1024,
    });
    assert.equal(settings.status, 200);

    const created = await post('/api/datasets', { name: 'HTTP 用例', count: 3, sizeMb: 0.1 });
    assert.equal(created.status, 201);
    const datasetId = created.body.dataset.id;

    const migrated = await post(`/api/datasets/${datasetId}/migrate`, {});
    assert.equal(migrated.status, 202);
    const waited = await post(`/api/datasets/${datasetId}/migrate/wait`, {});
    assert.equal(waited.body.result.status, 'completed');

    const files = (await get(`/api/datasets/${datasetId}`)).body.dataset.files;
    assert.ok(files.every((file) => file.readable === 'cold'));

    const query = await fetch(`${url}/api/datasets/${datasetId}/query`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ fileId: files[0].id, mode: 'file' }),
    });
    assert.equal(query.status, 200);
    assert.equal(query.headers.get('x-tier-verified'), 'sha256');
    const body = Buffer.from(await query.arrayBuffer());
    assert.equal(body.length, files[0].size);

    const reheat = await post(`/api/datasets/${datasetId}/reheat`, { scope: 'dataset' });
    assert.equal(reheat.status, 202);
    assert.ok(reheat.body.task.id);

    const reclaim = await post('/api/reclaim', { dryRun: true });
    assert.equal(reclaim.status, 200);
    assert.ok(Array.isArray(reclaim.body.result.actions));

    const recovery = await post('/api/recovery', { verifyCold: true });
    assert.equal(recovery.status, 200);
    assert.deepEqual(recovery.body.report.problems, []);

    const state = await get('/api/state');
    assert.equal(state.status, 200);
    assert.ok(state.body.datasets.length >= 1);
    assert.equal(typeof state.body.usage.hotBytes, 'number');
  });
});

test('HTTP：错误参数返回结构化错误', async () => {
  await withServer(async ({ url }) => {
    const response = await fetch(`${url}/api/datasets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: 'not json',
    });
    assert.equal(response.status, 400);
    const body = await response.json();
    assert.equal(body.code, 'INVALID_JSON');

    const missing = await fetch(`${url}/api/datasets/ds_999`);
    assert.equal(missing.status, 404);
    assert.equal((await missing.json()).code, 'DATASET_NOT_FOUND');

    const traversal = await fetch(`${url}/..%2f..%2fetc%2fpasswd`);
    assert.ok([403, 404].includes(traversal.status));
  });
});

test('HTTP：版本替换与取消后共享任务字段', async () => {
  await withServer(async ({ url }) => {
    const post = async (path, body = {}) => {
      const response = await fetch(`${url}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      });
      return response.json();
    };
    const get = async (path) => (await fetch(`${url}${path}`)).json();

    await post('/api/settings', {
      coldLatencyBaseMs: 1, coldBandwidthBytesPerSec: 8 * 1024 * 1024, coldFaultMode: 'none',
    });
    const dataset = (await post('/api/datasets', { name: 'v', count: 2, sizeMb: 0.05 })).dataset;
    await post(`/api/datasets/${dataset.id}/migrate`, {});
    await post(`/api/datasets/${dataset.id}/migrate/wait`, {});
    const beforeState = await get('/api/state');
    const oldVersion = beforeState.datasets.find((item) => item.id === dataset.id).files[0].cold.versionId;
    const pending = await post(`/api/datasets/${dataset.id}/version`, { seed: 'http-v2' });
    assert.ok(pending.pendingVersion.versionId);

    // 等待替换完成（轮询目录库，避免固定 sleep 在慢机器上过早读取）
    const deadline = Date.now() + 15_000;
    let after = null;
    while (Date.now() < deadline) {
      const state = await get('/api/state');
      after = state.datasets.find((item) => item.id === dataset.id);
      if (after?.coldVersion !== oldVersion) break;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    assert.notEqual(after.coldVersion, oldVersion);

    const response = await fetch(`${url}/api/datasets/${dataset.id}/query`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ fileId: after.files[0].id, mode: 'file', versionId: oldVersion }),
    });
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('x-tier-version'), oldVersion);
  });
});
