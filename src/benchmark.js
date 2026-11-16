/**
 * 对比实验：同一个数据集、同一份文件，分别用
 *   A. 按文件读取   B. 整分区回热
 * 在 4 个并发查询下测量：首屏延迟（首个查询拿到完整内容的耗时）、
 * 冷存储流量、热缓存增量、冷任务数。
 */
import { nowIso } from './util.js';

export async function runComparison(app, {
  files = 6, sizeBytes = 256 * 1024, clients = 4, seed = `cmp-${Date.now()}`,
} = {}) {
  const results = [];
  for (const [label, mode] of [['file', 'file'], ['partition', 'reheat-partition']]) {
    const dataset = await app.createDataset({
      name: `对比实验 · ${label === 'file' ? '按文件' : '整分区'}回热`,
      description: 'benchmark',
      count: files,
      sizeBytes,
      seed: `${seed}-${label}`,
    });
    const partition = app.catalog.addPartition({ name: `对比实验分区 ${label}` });
    app.migrations.start(dataset.id, { partitionId: partition.id });
    await app.migrations.waitFor(dataset.id);

    const file = app.catalog.requireDataset(dataset.id).files[0];
    const coldBefore = app.catalog.snapshot().stats.coldFetchBytes;
    const tasksBefore = app.catalog.snapshot().stats.coldTasks;
    const joinsBefore = app.catalog.snapshot().stats.sharedJoins;
    const hotBefore = await app.hot.usageBytes();
    const startedAt = Date.now();
    const latencies = [];
    await Promise.all(Array.from({ length: clients }, async () => {
      const queryStarted = Date.now();
      await app.queries.run({ datasetId: dataset.id, fileId: file.id, mode });
      latencies.push(Date.now() - queryStarted);
    }));
    const wallMs = Date.now() - startedAt;
    const snapshot = app.catalog.snapshot();
    const hotAfter = await app.hot.usageBytes();
    results.push({
      label,
      mode,
      datasetId: dataset.id,
      clients,
      fileBytes: file.size,
      datasetBytes: app.catalog.requireDataset(dataset.id).totalBytes,
      firstByteMs: Math.min(...latencies),
      slowestQueryMs: Math.max(...latencies),
      wallMs,
      coldBytes: snapshot.stats.coldFetchBytes - coldBefore,
      coldTasks: snapshot.stats.coldTasks - tasksBefore,
      sharedJoins: snapshot.stats.sharedJoins - joinsBefore,
      hotCacheDeltaBytes: hotAfter - hotBefore,
      completedAt: nowIso(),
    });
  }
  const [fileRun, partitionRun] = results;
  return {
    seed,
    clients,
    files,
    sizeBytes,
    results,
    summary: {
      latency: {
        file: fileRun.firstByteMs,
        partition: partitionRun.firstByteMs,
        delta: partitionRun.firstByteMs - fileRun.firstByteMs,
      },
      traffic: {
        file: fileRun.coldBytes,
        partition: partitionRun.coldBytes,
        delta: partitionRun.coldBytes - fileRun.coldBytes,
      },
      cache: {
        file: fileRun.hotCacheDeltaBytes,
        partition: partitionRun.hotCacheDeltaBytes,
        delta: partitionRun.hotCacheDeltaBytes - fileRun.hotCacheDeltaBytes,
      },
      tasks: {
        file: fileRun.coldTasks,
        partition: partitionRun.coldTasks,
      },
    },
  };
}
