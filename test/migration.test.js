import test from 'node:test';
import assert from 'node:assert/strict';
import { exists, makeApp, migrate, seedDataset, tempRoot } from './helpers.js';

test('迁移：冷副本校验通过后才登记并删除热副本', async () => {
  const { app } = await makeApp({ settings: { coldLatencyBaseMs: 1, coldBandwidthBytesPerSec: 64 * 1024 * 1024 } });
  const dataset = await seedDataset(app, { count: 3, sizeBytes: 32 * 1024 });
  const hotPaths = dataset.files.map((file) => file.hot.path);
  assert.ok(hotPaths.every(await exists));

  const result = await migrate(app, dataset.id);
  assert.equal(result.status, 'completed');

  const after = app.catalog.requireDataset(dataset.id);
  assert.equal(after.files.length, 3);
  for (const file of after.files) {
    assert.equal(file.readable, 'cold');
    assert.equal(file.hot, null);
    assert.ok(file.cold);
    assert.equal(file.cold.sha256, file.sha256);
    assert.ok(file.cold.versionId);
    assert.match(file.cold.evidence, /SHA-256/);
  }
  for (const path of hotPaths) assert.equal(await exists(path), false, '热文件应被回收');

  // 冷对象确实存在且可校验
  const manifest = await app.cold.readManifest(after.coldPartition, after.coldVersion);
  assert.equal(manifest.files.length, 3);
  for (const entry of manifest.files) {
    const check = await app.cold.verifyObject(after.coldPartition, after.coldVersion, entry.fileId);
    assert.equal(check.ok, true);
  }
  assert.deepEqual(app.catalog.analyze(), []);
  await app.shutdown();
});

test('迁移取消：不删除唯一副本，可读位置不变', async () => {
  const { app } = await makeApp({ settings: { coldLatencyBaseMs: 2, coldBandwidthBytesPerSec: 128 * 1024 } });
  const dataset = await seedDataset(app, { count: 2, sizeBytes: 256 * 1024 });
  const partition = app.catalog.addPartition({ name: 'slow' });
  app.migrations.start(dataset.id, { partitionId: partition.id });
  await new Promise((resolve) => setTimeout(resolve, 120));
  app.migrations.cancel(dataset.id, '测试取消');
  const result = await app.migrations.waitFor(dataset.id);
  assert.equal(result.status, 'cancelled');

  const after = app.catalog.requireDataset(dataset.id);
  for (const file of after.files) {
    assert.ok(file.hot, '热副本必须仍在');
    assert.equal(file.readable, 'hot');
  }
  for (const file of after.files) assert.equal(await exists(file.hot.path), true);
  assert.deepEqual(app.catalog.analyze(), []);
  await app.shutdown();
});

test('崩溃中断的迁移在重启后标记为 interrupted 且仍读热副本', async () => {
  const root = await tempRoot();
  {
    const { app } = await makeApp({ dataRoot: root, settings: { coldLatencyBaseMs: 2, coldBandwidthBytesPerSec: 64 * 1024 } });
    const dataset = await seedDataset(app, { count: 2, sizeBytes: 256 * 1024 });
    const partition = app.catalog.addPartition({ name: 'crash' });
    app.migrations.start(dataset.id, { partitionId: partition.id });
    await new Promise((resolve) => setTimeout(resolve, 90));
    // 模拟进程崩溃：不等待迁移收尾，直接持久化目录并丢弃服务。
    await app.catalog.flush();
    await new Promise((resolve) => setTimeout(resolve, 60));
    app.migrations.cancel(dataset.id);
    await app.migrations.waitFor(dataset.id).catch(() => {});
    // 手工把状态改回 running，模拟“没走到收尾逻辑就断电”。
    const live = app.catalog.requireDataset(dataset.id);
    live.migration.status = 'running';
    live.migration.phase = 'cold-writing';
    await app.catalog.flush();
  }
  {
    const { app } = await makeApp({ dataRoot: root });
    const notes = await app.recovery.run();
    assert.ok(notes.interrupted.some((note) => note.kind === 'migration.interrupted'));
    const dataset = app.catalog.listDatasets()[0];
    assert.equal(dataset.migration.status, 'interrupted');
    for (const file of dataset.files) {
      assert.ok(file.hot, '中断后仍读热副本');
      assert.equal(file.readable, 'hot');
    }
    assert.deepEqual(app.catalog.analyze(), []);
    await app.shutdown();
  }
});
