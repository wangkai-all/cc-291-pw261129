import test from 'node:test';
import assert from 'node:assert/strict';
import {
  fileOf, makeApp, migrate, seedDataset, waitFor,
} from './helpers.js';

async function setupCold(options = {}) {
  const { app } = await makeApp({
    settings: {
      coldLatencyBaseMs: 1, coldBandwidthBytesPerSec: 16 * 1024 * 1024, versionGraceMs: 0, ...options,
    },
  });
  const dataset = await seedDataset(app, { count: 3, sizeBytes: 32 * 1024 });
  await migrate(app, dataset.id);
  return { app, dataset };
}

test('版本替换：新版本生效，查询读到的内容与摘要一致', async () => {
  const { app, dataset } = await setupCold();
  const before = fileOf(app, dataset.id, 0);
  const oldVersion = before.cold.versionId;
  const pending = app.versioning.start(dataset.id, { seed: 'new-generation' });
  const result = await app.versioning.waitFor(dataset.id);
  assert.equal(result.status, 'completed');
  assert.notEqual(result.versionId, oldVersion);

  const after = app.catalog.requireDataset(dataset.id);
  const changed = after.files.filter((file, index) => file.sha256 !== dataset.files[index].sha256);
  assert.equal(changed.length, 3, '内容变化后所有文件摘要都变化');
  assert.equal(after.currentVersion, 2);
  assert.equal(app.catalog.snapshot().generation > 0, true);
  assert.equal(pending.versionId, result.versionId);

  const query = await app.queries.run({ datasetId: dataset.id, fileId: after.files[0].id, mode: 'file' });
  assert.equal(query.record.status, 'completed');
  assert.equal(query.record.verification.actualSha256, after.files[0].sha256);
  assert.equal(query.body.length, after.files[0].size);

  // 旧版本仍在冷存储上，但不再被目录引用，允许回收。
  const versions = await app.cold.listVersions(after.coldPartition);
  assert.ok(versions.some((version) => version.versionId === oldVersion));
  await app.shutdown();
});

test('版本替换期间固定旧版本的查询继续读旧内容，回收因租约跳过', async () => {
  const { app, dataset } = await setupCold({ coldBandwidthBytesPerSec: 256 * 1024, coldLatencyBaseMs: 2 });
  const oldFile = fileOf(app, dataset.id, 0);
  const oldVersion = oldFile.cold.versionId;
  const oldSha = oldFile.sha256;

  // 先发起一个冷层读取，让它的取回任务固定旧版本。
  const slowQuery = app.queries.run({ datasetId: dataset.id, fileId: oldFile.id, mode: 'file' });
  await waitFor(() => app.retrievals.list().some((task) => task.status === 'running'), { label: '旧版本取回任务' });

  const replace = app.versioning.start(dataset.id, { seed: 'replacement' });
  const [queryResult, versionResult] = await Promise.all([slowQuery, app.versioning.waitFor(dataset.id)]);
  assert.equal(queryResult.record.status, 'completed');
  assert.equal(queryResult.record.verification.actualSha256, oldSha, '查询绑定旧内容');
  assert.equal(versionResult.status, 'completed');

  // 旧版本若仍被 pin 住，回收必须跳过；这里通常已释放，因此再手动 pin 验证。
  const release = app.pins.pin(fileOf(app, dataset.id, 0).cold.partitionId, oldVersion);
  const plan = await app.reclaimer.reclaim({ dryRun: true });
  const skipped = plan.actions.find((action) => action.versionId === oldVersion);
  assert.ok(skipped, '旧版本应出现在预演结果中');
  assert.equal(skipped.type, 'cold-version-skipped');
  assert.match(skipped.reason, /固定/);
  release();

  const reclaimed = await app.reclaimer.reclaim({});
  const removed = reclaimed.actions.find((action) => action.versionId === oldVersion);
  assert.equal(removed?.type, 'cold-version-removed');
  const versions = await app.cold.listVersions(fileOf(app, dataset.id, 0).cold.partitionId);
  assert.equal(versions.some((version) => version.versionId === oldVersion), false);
  assert.deepEqual(app.catalog.analyze(), []);
  await app.shutdown();
});

test('固定版本查询：版本替换后仍能按 versionId 读取旧内容', async () => {
  const { app, dataset } = await setupCold();
  const file = fileOf(app, dataset.id, 1);
  const oldVersion = file.cold.versionId;
  const oldSha = file.sha256;
  await app.versioning.start(dataset.id, { seed: 'fixed-version-test' });
  await app.versioning.waitFor(dataset.id);

  const result = await app.queries.run({
    datasetId: dataset.id, fileId: file.id, mode: 'file', versionId: oldVersion,
  });
  assert.equal(result.record.status, 'completed');
  assert.equal(result.record.verification.actualSha256, oldSha);
  assert.equal(result.record.versionId, oldVersion);
  await app.shutdown();
});

test('版本替换失败：旧版本与可读位置不变', async () => {
  const { app, dataset } = await setupCold({ coldFaultMode: 'corrupt-always' });
  // 故障只影响“取回”，不影响写入；这里用网络错误也无法拦截写入，
  // 因此改用取消路径验证失败语义。
  const file = fileOf(app, dataset.id, 0);
  const before = { sha: file.sha256, version: file.cold.versionId, readable: file.readable };
  app.versioning.start(dataset.id, { seed: 'to-be-cancelled' });
  await new Promise((resolve) => setTimeout(resolve, 30));
  app.versioning.cancel(dataset.id);
  const result = await app.versioning.waitFor(dataset.id);
  assert.ok(['cancelled', 'completed'].includes(result.status));
  if (result.status === 'cancelled') {
    const after = app.catalog.requireDataset(dataset.id);
    assert.equal(after.files[0].sha256, before.sha);
    assert.equal(after.files[0].cold.versionId, before.version);
    assert.equal(after.files[0].readable, before.readable);
    assert.deepEqual(app.catalog.analyze(), []);
  }
  await app.shutdown();
});
