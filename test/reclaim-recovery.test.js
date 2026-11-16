import test from 'node:test';
import assert from 'node:assert/strict';
import {
  appendFile, rm, writeFile,
} from 'node:fs/promises';
import {
  exists, fileOf, makeApp, migrate, seedDataset, tempRoot, waitFor,
} from './helpers.js';

test('回收：孤儿热文件被清理，被引用的热副本保留', async () => {
  const { app } = await makeApp();
  const dataset = await seedDataset(app, { count: 2, sizeBytes: 16 * 1024 });
  const orphan = `${app.hot.datasetRoot(dataset.id)}/f_099-deadbeef0000.bin`;
  await writeFile(orphan, Buffer.alloc(4096));
  const preview = await app.reclaimer.reclaim({ dryRun: true });
  assert.ok(preview.actions.some((action) => action.type === 'hot-orphan' && action.path === orphan));
  assert.equal(await exists(orphan), true, '预演不删除文件');

  const result = await app.reclaimer.reclaim({});
  assert.ok(result.actions.some((action) => action.type === 'hot-removed' && action.path === orphan));
  assert.equal(await exists(orphan), false);
  for (const file of app.catalog.requireDataset(dataset.id).files) {
    assert.equal(await exists(file.hot.path), true, '被目录引用的热副本保留');
  }
  await app.shutdown();
});

test('回收：正在被读取的热副本不会删除', async () => {
  const { app } = await makeApp();
  const dataset = await seedDataset(app, { count: 1, sizeBytes: 16 * 1024 });
  const file = fileOf(app, dataset.id, 0);
  const release = app.hot.leases.acquire(file.hot.path);
  const result = await app.reclaimer.reclaim({});
  assert.ok(result.actions.some((action) => action.type === 'hot-skip' && action.path === file.hot.path));
  assert.equal(await exists(file.hot.path), true);
  release();
  await app.shutdown();
});

test('回收：未被引用但仍在宽限期的冷版本跳过', async () => {
  const { app } = await makeApp({ settings: { versionGraceMs: 60_000 } });
  const dataset = await seedDataset(app, { count: 2, sizeBytes: 16 * 1024 });
  await migrate(app, dataset.id);
  const oldVersion = fileOf(app, dataset.id, 0).cold.versionId;
  await app.versioning.start(dataset.id, { seed: 'grace-test' });
  await app.versioning.waitFor(dataset.id);
  const result = await app.reclaimer.reclaim({});
  const skipped = result.actions.find((action) => action.versionId === oldVersion);
  assert.equal(skipped?.type, 'cold-version-skipped');
  assert.match(skipped.reason, /宽限期/);
  const versions = await app.cold.listVersions(fileOf(app, dataset.id, 0).cold.partitionId);
  assert.ok(versions.some((version) => version.versionId === oldVersion));
  await app.shutdown();
});

test('恢复：热文件被外部删除后回退到冷副本', async () => {
  const root = await tempRoot();
  let datasetId;
  let fileId;
  let hotPath;
  {
    const { app } = await makeApp({ dataRoot: root });
    const dataset = await seedDataset(app, { count: 2, sizeBytes: 16 * 1024 });
    await migrate(app, dataset.id);
    const file = fileOf(app, dataset.id, 0);
    await app.queries.run({ datasetId: dataset.id, fileId: file.id, mode: 'file' });
    hotPath = fileOf(app, dataset.id, 0).hot.path;
    datasetId = dataset.id;
    fileId = file.id;
    await app.shutdown();
  }
  await rm(hotPath);
  {
    const { app } = await makeApp({ dataRoot: root });
    const report = await app.recovery.run();
    assert.ok(report.actions.some((action) => action.action === 'hot-invalidated'));
    const file = app.catalog.requireDataset(datasetId).files.find((item) => item.id === fileId);
    assert.equal(file.readable, 'cold');
    assert.equal(file.hot, null);
    assert.match(file.lastError, /回退到冷副本/);
    const query = await app.queries.run({ datasetId, fileId, mode: 'file' });
    assert.equal(query.record.status, 'completed');
    assert.deepEqual(app.catalog.analyze(), []);
    await app.shutdown();
  }
});

test('恢复：冷副本校验失败但热副本完好时继续读热', async () => {
  const root = await tempRoot();
  let datasetId;
  {
    const { app } = await makeApp({ dataRoot: root });
    const dataset = await seedDataset(app, { count: 1, sizeBytes: 16 * 1024 });
    await migrate(app, dataset.id);
    const file = fileOf(app, dataset.id, 0);
    await app.queries.run({ datasetId: dataset.id, fileId: file.id, mode: 'file' });
    const refreshed = fileOf(app, dataset.id, 0);
    await appendFile(
      app.cold.objectPath(refreshed.cold.partitionId, refreshed.cold.versionId, refreshed.cold.objectId),
      'corrupt',
    );
    datasetId = dataset.id;
    await app.shutdown();
  }
  {
    const { app } = await makeApp({ dataRoot: root });
    const report = await app.recovery.run({ verifyCold: true });
    assert.ok(report.actions.some((action) => action.action === 'cold-invalid-hot-kept'));
    const file = app.catalog.requireDataset(datasetId).files[0];
    assert.ok(file.hot);
    assert.equal(file.readable, 'hot');
    const query = await app.queries.run({ datasetId, fileId: file.id, mode: 'auto' });
    assert.equal(query.record.status, 'completed');
    assert.equal(query.record.readFrom, 'hot');
    await app.shutdown();
  }
});

test('进程恢复：中断的回热与查询被标记，原可读位置不变', async () => {
  const root = await tempRoot();
  let datasetId;
  let fileId;
  {
    const { app } = await makeApp({ dataRoot: root, settings: { coldBandwidthBytesPerSec: 128 * 1024 } });
    const dataset = await seedDataset(app, { count: 2, sizeBytes: 128 * 1024 });
    await migrate(app, dataset.id);
    datasetId = dataset.id;
    fileId = fileOf(app, dataset.id, 0).id;
    // 手工构造“崩溃时磁盘上遗留的状态”：回热进行中、查询 running。
    const live = app.catalog.requireDataset(datasetId);
    live.files[0].pendingReheat = { at: new Date().toISOString(), taskId: 'rt_crashed' };
    live.files[1].pendingReheat = { at: new Date().toISOString(), taskId: 'rt_crashed' };
    app.catalog.state.queries.push({
      id: 'q_crashed', datasetId, fileId, status: 'running', mode: 'auto',
    });
    app.catalog.state.retrievals.push({
      id: 'rt_crashed', datasetId, status: 'running', scope: 'dataset',
    });
    await app.catalog.flush();
    // 不调用 shutdown：模拟进程在任务收尾前消失。
  }
  // 直接重新加载目录库，走启动恢复路径。
  const { Catalog } = await import('../src/catalog.js');
  const reloaded = new Catalog({ dir: `${root}/catalog` });
  await reloaded.load();
  const notes = await reloaded.recoverInterrupted();
  assert.ok(notes.some((note) => note.kind === 'reheat.interrupted'));
  assert.ok(notes.some((note) => note.kind === 'retrieval.interrupted'));
  const dataset = reloaded.requireDataset(datasetId);
  assert.ok(dataset.files.every((file) => file.pendingReheat === undefined));
  assert.ok(dataset.files.every((file) => ['cold', 'hot'].includes(file.readable)));
  const query = reloaded.state.queries.find((item) => item.id === 'q_crashed');
  assert.equal(query.status, 'interrupted');
  assert.equal(reloaded.state.retrievals.find((item) => item.id === 'rt_crashed').status, 'interrupted');
  assert.deepEqual(reloaded.analyze(), []);
});

test('查询遇到被篡改的热副本时回退冷层并带回校验依据', async () => {
  const { app } = await makeApp({
    settings: { coldLatencyBaseMs: 1, coldBandwidthBytesPerSec: 16 * 1024 * 1024 },
  });
  const dataset = await seedDataset(app, { count: 2, sizeBytes: 16 * 1024 });
  await migrate(app, dataset.id);
  const file = fileOf(app, dataset.id, 0);
  await app.queries.run({ datasetId: dataset.id, fileId: file.id, mode: 'file' });
  const warm = fileOf(app, dataset.id, 0);
  assert.ok(warm.hot);
  await appendFile(warm.hot.path, 'tampered');

  const result = await app.queries.run({ datasetId: dataset.id, fileId: file.id, mode: 'file' });
  assert.equal(result.record.status, 'completed');
  assert.equal(result.record.verification.actualSha256, file.sha256);
  assert.ok(result.record.hotFallbackReason, '记录回退原因');
  const events = app.catalog.snapshot().events.filter((event) => event.kind === 'query.hot-fallback');
  assert.equal(events.length, 1);
  await app.shutdown();
});
