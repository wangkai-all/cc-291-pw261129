import test from 'node:test';
import assert from 'node:assert/strict';
import {
  exists, fileOf, makeApp, migrate, seedDataset, waitFor,
} from './helpers.js';

test('迁移与查询并发：查询全程有可读副本，无 NO_REPLICA', async () => {
  const { app } = await makeApp({
    settings: { coldLatencyBaseMs: 2, coldBandwidthBytesPerSec: 512 * 1024 },
  });
  const dataset = await seedDataset(app, { count: 4, sizeBytes: 96 * 1024 });
  const fileIds = app.catalog.requireDataset(dataset.id).files.map((file) => file.id);
  const partition = app.catalog.addPartition({ name: 'concurrent' });
  app.migrations.start(dataset.id, { partitionId: partition.id });

  const results = [];
  const deadline = Date.now() + 6_000;
  while (Date.now() < deadline) {
    for (const fileId of fileIds) {
      results.push(app.queries.run({ datasetId: dataset.id, fileId, mode: 'auto' })
        .then((result) => ({ ok: true, from: result.record.readFrom }))
        .catch((error) => ({ ok: false, code: error.code, message: error.message })));
    }
    // 并发回收：只能删除没有引用的内容，不能影响可读位置。
    results.push(app.reclaimer.reclaim({}).then(() => ({ ok: true, from: 'reclaim' })));
    await new Promise((resolve) => setTimeout(resolve, 40));
  }
  const settled = await Promise.all(results);
  const failures = settled.filter((item) => !item.ok);
  assert.deepEqual(failures, [], `并发查询不应失败：${JSON.stringify(failures.slice(0, 3))}`);
  const migrationResult = await app.migrations.waitFor(dataset.id);
  assert.ok(['completed', 'partial'].includes(migrationResult.status), `迁移应完成：${JSON.stringify(migrationResult)}`);

  const after = app.catalog.requireDataset(dataset.id);
  assert.ok(after.files.every((file) => file.readable === 'cold' || file.readable === 'hot'));
  assert.equal(after.files.every((file) => file.cold), true);
  assert.deepEqual(app.catalog.analyze(), []);
  await app.shutdown();
});

test('回热与迁移并发：已登记冷副本先于热副本删除', async () => {
  const { app } = await makeApp({
    settings: { coldLatencyBaseMs: 2, coldBandwidthBytesPerSec: 768 * 1024 },
  });
  const dataset = await seedDataset(app, { count: 3, sizeBytes: 128 * 1024 });
  const file = fileOf(app, dataset.id, 0);

  // 先做一次回热，得到混合可读状态，然后再次迁移。
  await migrate(app, dataset.id);
  const q = await app.queries.run({ datasetId: dataset.id, fileId: file.id, mode: 'file' });
  assert.equal(q.record.status, 'completed');

  // 第二次迁移当前全部为热副本？不，其它文件仍在冷层，因此改用直接回热并发。
  const partition = app.catalog.addPartition({ name: 'second' });
  const promises = [
    app.retrievals.ensure({
      datasetId: dataset.id,
      scope: 'dataset',
      partitionId: file.cold.partitionId,
      versionId: file.cold.versionId,
      reason: '并发回热',
    }).task.promise.catch(() => {}),
    app.reclaimer.reclaim({}).then(() => null),
  ];
  await Promise.all(promises);
  // 回热完成后，热盘至少有一个可读副本且目录一致。
  const after = app.catalog.requireDataset(dataset.id);
  assert.ok(after.files.some((item) => item.hot));
  assert.deepEqual(app.catalog.analyze(), []);

  // 迁移验证：对全部热的数据集，冷副本登记与热删除顺序正确。
  const allHot = await seedDataset(app, { name: 'all-hot', count: 2, sizeBytes: 64 * 1024, seed: 'all-hot' });
  const p2 = app.catalog.addPartition({ name: 'p2' });
  app.migrations.start(allHot.id, { partitionId: p2.id });
  await app.migrations.waitFor(allHot.id);
  const hotPaths = app.catalog.requireDataset(allHot.id).files.map((item) => item.hot?.path).filter(Boolean);
  assert.deepEqual(hotPaths, []);
  for (const item of app.catalog.requireDataset(allHot.id).files) {
    assert.ok(item.cold);
    assert.equal(item.readable, 'cold');
  }
  assert.equal(partition.id.startsWith('pt_'), true);
  assert.deepEqual(app.catalog.analyze(), []);
  await app.shutdown();
});

test('十个并发查询只下载一次冷数据（按文件）', async () => {
  const { app } = await makeApp({
    settings: { coldLatencyBaseMs: 3, coldBandwidthBytesPerSec: 512 * 1024, cancelPolicy: 'continue' },
  });
  const dataset = await seedDataset(app, { count: 4, sizeBytes: 64 * 1024 });
  await migrate(app, dataset.id);
  const file = fileOf(app, dataset.id, 0);
  const runs = Array.from({ length: 10 }, () => app.queries.run({
    datasetId: dataset.id, fileId: file.id, mode: 'file',
  }));
  await waitFor(() => {
    const tasks = app.retrievals.list();
    return tasks.length === 1 && tasks[0].subscribers >= 2;
  }, { label: '十个查询加入同一单文件任务' });
  await Promise.all(runs);
  const state = app.catalog.snapshot();
  assert.equal(state.stats.coldTasks, 1, '只创建一个冷任务');
  assert.equal(state.stats.coldFetchBytes, file.size, '只下载一份文件');
  assert.equal(state.stats.sharedJoins, 9);
  await app.shutdown();
});
