import test from 'node:test';
import assert from 'node:assert/strict';
import { fileOf, makeApp, migrate, seedDataset, waitFor } from './helpers.js';

test('冷层查询：按文件读取只下载该文件，二次查询命中热层', async () => {
  const { app } = await makeApp({ settings: { coldLatencyBaseMs: 1, coldBandwidthBytesPerSec: 32 * 1024 * 1024 } });
  const dataset = await seedDataset(app, { count: 3, sizeBytes: 64 * 1024 });
  await migrate(app, dataset.id);
  const file = fileOf(app, dataset.id, 0);

  const first = await app.queries.run({ datasetId: dataset.id, fileId: file.id, mode: 'file' });
  assert.equal(first.record.status, 'completed');
  assert.equal(first.record.readFrom, 'hot');
  assert.equal(first.record.coldBytes, file.size, '按文件读取只下载一个文件');
  assert.equal(first.record.verification.ok, true);
  assert.equal(first.body.length, file.size);

  const second = await app.queries.run({ datasetId: dataset.id, fileId: file.id, mode: 'auto' });
  assert.equal(second.record.readFrom, 'hot');
  assert.equal(second.record.coldBytes, 0, '第二次查询命中热层，无冷流量');
  assert.ok(second.record.firstByteMs <= first.record.firstByteMs);

  const state = app.catalog.snapshot();
  assert.equal(state.stats.coldFetchBytes, file.size);
  assert.equal(state.files ?? null, null);
  assert.deepEqual(app.catalog.analyze(), []);
  await app.shutdown();
});

test('整分区回热：只产生一个共享任务，多个查询共享下载', async () => {
  const { app } = await makeApp({
    settings: { coldLatencyBaseMs: 5, coldBandwidthBytesPerSec: 512 * 1024 },
  });
  const dataset = await seedDataset(app, { count: 3, sizeBytes: 128 * 1024 });
  await migrate(app, dataset.id);
  const file = fileOf(app, dataset.id, 0);
  const total = app.catalog.requireDataset(dataset.id).totalBytes;

  const runs = Array.from({ length: 4 }, () => app.queries.run({
    datasetId: dataset.id, fileId: file.id, mode: 'reheat-partition',
  }));
  await waitFor(() => app.retrievals.list().some((task) => task.status === 'running' && task.subscribers >= 2), {
    label: '四个查询加入同一任务',
  });
  const during = app.retrievals.list().filter((task) => task.status === 'running');
  assert.equal(during.length, 1, '同一分区版本只应有一个任务');
  const results = await Promise.all(runs);
  for (const result of results) assert.equal(result.record.status, 'completed');

  const state = app.catalog.snapshot();
  assert.equal(state.stats.coldTasks, 1, '只产生一个冷任务');
  assert.equal(state.stats.sharedJoins, 3, '其余 3 个查询加入共享任务');
  assert.equal(state.stats.coldFetchBytes, total, '整分区只下载一次');
  for (const f of app.catalog.requireDataset(dataset.id).files) assert.ok(f.hot, '回热后文件都应有热副本');
  await app.shutdown();
});

test('取消查询后共享取回任务按策略继续（continue）', async () => {
  const { app } = await makeApp({
    settings: { coldLatencyBaseMs: 5, coldBandwidthBytesPerSec: 256 * 1024, cancelPolicy: 'continue' },
  });
  const dataset = await seedDataset(app, { count: 2, sizeBytes: 256 * 1024 });
  await migrate(app, dataset.id);
  const file = fileOf(app, dataset.id, 1);
  const controller = new AbortController();
  const query = app.queries.run({
    datasetId: dataset.id, fileId: file.id, mode: 'reheat-partition', signal: controller.signal,
  }).catch((error) => error);
  await waitFor(() => app.retrievals.list().some((task) => task.status === 'running' && task.bytesDownloaded > 0), {
    label: '任务开始下载',
  });
  controller.abort();
  const error = await query;
  assert.equal(error.code, 'ABORTED');
  const record = app.catalog.snapshot().queries.at(-1);
  assert.equal(record.status, 'cancelled');
  assert.equal(record.taskContinuedAfterCancel, true);

  const task = app.retrievals.list()[0];
  await waitFor(() => task.status === 'completed', { label: '共享任务继续完成' });
  const snapshot = app.retrievals.list()[0];
  assert.equal(snapshot.status, 'completed');
  assert.ok(snapshot.bytesDownloaded > 0, '查询取消后任务仍在下载并缓存');
  assert.ok(app.catalog.requireDataset(dataset.id).files.every((f) => f.hot));
  await app.shutdown();
});

test('取消查询且策略 cancel-if-idle：无订阅者时任务中止', async () => {
  const { app } = await makeApp({
    settings: { coldLatencyBaseMs: 5, coldBandwidthBytesPerSec: 256 * 1024, cancelPolicy: 'cancel-if-idle' },
  });
  const dataset = await seedDataset(app, { count: 2, sizeBytes: 256 * 1024 });
  await migrate(app, dataset.id);
  const file = fileOf(app, dataset.id, 0);
  const controller = new AbortController();
  const query = app.queries.run({
    datasetId: dataset.id, fileId: file.id, mode: 'reheat-partition', signal: controller.signal,
  }).catch((error) => error);
  await waitFor(() => app.retrievals.list().length > 0, { label: '任务创建' });
  controller.abort();
  await query;
  const record = app.catalog.snapshot().queries.at(-1);
  assert.equal(record.taskContinuedAfterCancel, false);
  const snapshot = await waitFor(async () => {
    const task = app.retrievals.list()[0];
    return task?.status !== 'running' ? task : null;
  }, { label: '任务中止' });
  assert.equal(snapshot.status, 'cancelled');
  // 原可读位置保持不变：仍是冷层整体可读。
  for (const f of app.catalog.requireDataset(dataset.id).files) {
    assert.equal(f.readable, 'cold');
  }
  assert.deepEqual(app.catalog.analyze(), []);
  await app.shutdown();
});

test('冷存储网络故障：查询报错但可读位置保持冷层', async () => {
  const { app } = await makeApp({
    settings: {
      coldLatencyBaseMs: 1,
      coldBandwidthBytesPerSec: 16 * 1024 * 1024,
      coldFaultMode: 'error-always',
      maxVerifyRetries: 0,
    },
  });
  const dataset = await seedDataset(app, { count: 2, sizeBytes: 32 * 1024 });
  await migrate(app, dataset.id);
  const file = fileOf(app, dataset.id, 0);
  await assert.rejects(
    () => app.queries.run({ datasetId: dataset.id, fileId: file.id, mode: 'file' }),
    (error) => {
      assert.equal(error.statusCode, 502);
      return true;
    },
  );
  const after = app.catalog.requireDataset(dataset.id);
  assert.equal(after.files[0].readable, 'cold');
  assert.match(after.files[0].lastError, /回热失败/);
  assert.deepEqual(app.catalog.analyze(), []);
  await app.shutdown();
});

test('回热校验失败：坏字节不登记为热副本，重试后成功', async () => {
  const { app } = await makeApp({
    settings: {
      coldLatencyBaseMs: 1,
      coldBandwidthBytesPerSec: 16 * 1024 * 1024,
      coldFaultMode: 'corrupt-once',
      maxVerifyRetries: 1,
    },
  });
  const dataset = await seedDataset(app, { count: 2, sizeBytes: 32 * 1024 });
  await migrate(app, dataset.id);
  const file = fileOf(app, dataset.id, 0);
  const result = await app.queries.run({ datasetId: dataset.id, fileId: file.id, mode: 'file' });
  assert.equal(result.record.status, 'completed');
  assert.equal(result.record.verification.ok, true);
  const after = app.catalog.requireDataset(dataset.id);
  assert.ok(after.files[0].hot, '重试成功后应有热副本');
  assert.equal(after.files[0].hot.sha256, file.sha256);
  const state = app.catalog.snapshot();
  assert.ok(state.stats.verificationFailures >= 1, '记录校验失败');
  const events = state.events.filter((event) => event.kind === 'reheat.verify-retry');
  assert.equal(events.length, 1, '应记录一次重试');
  assert.ok(events[0].evidence.actualSha256 !== events[0].evidence.expectedSha256);
  await app.shutdown();
});

test('回热校验持续失败：查询失败且不产生热副本', async () => {
  const { app } = await makeApp({
    settings: {
      coldLatencyBaseMs: 1,
      coldBandwidthBytesPerSec: 16 * 1024 * 1024,
      coldFaultMode: 'corrupt-always',
      maxVerifyRetries: 0,
    },
  });
  const dataset = await seedDataset(app, { count: 2, sizeBytes: 32 * 1024 });
  await migrate(app, dataset.id);
  const file = fileOf(app, dataset.id, 0);
  await assert.rejects(
    () => app.queries.run({ datasetId: dataset.id, fileId: file.id, mode: 'file' }),
    (error) => /校验|校验失败|取回失败/.test(error.message) || error.statusCode === 502,
  );
  const after = app.catalog.requireDataset(dataset.id);
  assert.equal(after.files[0].hot, null, '校验失败的字节不能登记为热副本');
  assert.equal(after.files[0].readable, 'cold');
  const state = app.catalog.snapshot();
  assert.ok(state.stats.verificationFailures >= 1);
  assert.deepEqual(app.catalog.analyze(), []);
  await app.shutdown();
});
