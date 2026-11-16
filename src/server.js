import http from 'node:http';
import path from 'node:path';
import { stat } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { createApp } from './app.js';
import { bytesFromMb, makeError } from './util.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.resolve(__dirname, '..', 'public');
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8',
};

function sendJson(res, status, value) {
  const body = Buffer.from(JSON.stringify(value, null, 2));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': body.length,
    'cache-control': 'no-store',
  });
  res.end(body);
}

function sendError(res, error) {
  sendJson(res, error.statusCode ?? 500, {
    error: error.message,
    code: error.code ?? 'INTERNAL',
    extra: error.extra ?? undefined,
  });
}

async function readBody(req, limit = 2 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw makeError('请求体过大', 'BODY_TOO_LARGE', 413);
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw makeError('请求体不是合法 JSON', 'INVALID_JSON', 400);
  }
}

function routeParams(pattern, pathname) {
  const patternParts = pattern.split('/').filter(Boolean);
  const pathParts = pathname.split('/').filter(Boolean);
  if (patternParts.length !== pathParts.length) return null;
  const params = {};
  for (let i = 0; i < patternParts.length; i += 1) {
    const part = patternParts[i];
    if (part.startsWith(':')) params[part.slice(1)] = decodeURIComponent(pathParts[i]);
    else if (part !== pathParts[i]) return null;
  }
  return params;
}

export async function startServer({
  port = 0, host = '127.0.0.1', dataRoot, quiet = false,
} = {}) {
  const app = await createApp({ dataRoot });
  const log = (...args) => { if (!quiet) console.log('[tier]', ...args); };
  let recoveryReport = null;

  // 启动时对齐磁盘与目录库：中断的迁移/回热只标记，不删任何副本。
  try {
    recoveryReport = await app.recovery.run({ verifyCold: false });
    if (recoveryReport.actions.length) log('启动恢复修正', recoveryReport.actions.length, '项');
  } catch (error) {
    log('启动恢复失败', error.message);
  }

  const routes = [];
  const route = (method, pattern, handler) => routes.push({ method, pattern, handler });

  async function statePayload() {
    const state = app.summary();
    const hotBytes = await app.hot.usageBytes();
    const coldBytes = await app.cold.usageBytes();
    state.recoveryReport = recoveryReport;
    state.usage = {
      hotBytes,
      coldBytes,
      hotCapacityBytes: state.settings.hotCapacityBytes,
      hotFreeBytes: Math.max(0, state.settings.hotCapacityBytes - hotBytes),
      hotPct: Math.min(100, Math.round((hotBytes / Math.max(1, state.settings.hotCapacityBytes)) * 100)),
    };
    return state;
  }

  route('GET', '/api/state', async (req, res) => {
    sendJson(res, 200, await statePayload());
  });

  route('GET', '/api/events', async (req, res, params, url) => {
    const limit = Math.min(500, Math.max(1, Number(url.searchParams.get('limit') ?? 120)));
    const state = app.catalog.snapshot();
    sendJson(res, 200, { events: state.events.slice(-limit).reverse() });
  });

  route('GET', '/api/datasets', async (req, res) => {
    sendJson(res, 200, { datasets: app.catalog.listDatasets() });
  });

  route('POST', '/api/datasets', async (req, res) => {
    const body = await readBody(req);
    const name = String(body.name ?? '').trim() || `数据集 ${new Date().toLocaleTimeString('zh-CN')}`;
    const dataset = await app.createDataset({
      name,
      description: String(body.description ?? ''),
      count: Math.min(24, Math.max(1, Number(body.count ?? 4))),
      sizeBytes: bytesFromMb(Math.min(8, Math.max(0.05, Number(body.sizeMb ?? 0.25)))),
      seed: body.seed ? String(body.seed) : null,
    });
    log('创建数据集', dataset.id, name);
    sendJson(res, 201, { dataset });
  });

  route('GET', '/api/datasets/:id', async (req, res, params) => {
    sendJson(res, 200, { dataset: app.catalog.requireDataset(params.id) });
  });

  route('POST', '/api/datasets/:id/migrate', async (req, res, params) => {
    const body = await readBody(req);
    app.catalog.requireDataset(params.id);
    let partitionId = body.partitionId ? String(body.partitionId) : null;
    if (!partitionId) {
      const dataset = app.catalog.requireDataset(params.id);
      const partition = app.catalog.addPartition({
        name: `${dataset.name} 冷分区`, description: '迁移时自动创建',
      });
      partitionId = partition.id;
    }
    const migration = app.migrations.start(params.id, { partitionId, createdBy: 'user' });
    log('开始迁移', params.id, '->', partitionId);
    sendJson(res, 202, { migration });
  });

  route('POST', '/api/datasets/:id/migrate/cancel', async (req, res, params) => {
    const body = await readBody(req);
    const cancelled = app.migrations.cancel(params.id, body.reason ? String(body.reason) : undefined);
    if (!cancelled) throw makeError('没有进行中的迁移', 'NO_MIGRATION', 409);
    sendJson(res, 200, { cancelled: true });
  });

  route('POST', '/api/datasets/:id/migrate/wait', async (req, res, params) => {
    const result = await app.migrations.waitFor(params.id);
    sendJson(res, 200, { result });
  });

  route('POST', '/api/datasets/:id/query', async (req, res, params) => {
    const body = await readBody(req);
    const mode = ['auto', 'file', 'reheat-partition'].includes(body.mode) ? body.mode : 'auto';
    const cancelAfterMs = Number(body.cancelAfterMs);
    const controller = new AbortController();
    let cancelledByTimeout = false;
    let timer = null;
    if (Number.isFinite(cancelAfterMs) && cancelAfterMs > 0) {
      timer = setTimeout(() => {
        cancelledByTimeout = true;
        controller.abort();
      }, cancelAfterMs);
    }
    res.on('close', () => {
      if (!res.writableEnded) controller.abort();
    });
    try {
      const result = await app.queries.run({
        datasetId: params.id,
        fileId: String(body.fileId ?? ''),
        mode,
        versionId: body.versionId ? String(body.versionId) : null,
        signal: controller.signal,
        actor: 'user',
        clientId: body.clientId ? String(body.clientId) : null,
      });
      res.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-length': result.body.length,
        'cache-control': 'no-store',
        'x-tier-cancel-policy': app.catalog.settings().cancelPolicy,
        'x-tier-query-id': result.record.id,
        ...result.headers,
      });
      res.end(result.body);
    } catch (error) {
      sendJson(res, error.statusCode ?? 500, {
        error: error.message,
        code: error.code ?? 'INTERNAL',
        cancelledByTimeout,
        queryId: error.queryId,
        taskContinuedAfterCancel: error.taskContinuedAfterCancel,
      });
    } finally {
      if (timer) clearTimeout(timer);
    }
  });

  route('POST', '/api/datasets/:id/reheat', async (req, res, params) => {
    const body = await readBody(req);
    const scope = body.scope === 'file' ? 'file' : 'dataset';
    const dataset = app.catalog.requireDataset(params.id);
    const fileId = scope === 'file' ? String(body.fileId ?? '') : null;
    if (scope === 'file') {
      const { file } = app.catalog.findFile(params.id, fileId);
      if (!file.cold) throw makeError('该文件没有冷副本，无需回热', 'NO_COLD_REPLICA', 409);
    }
    const partitionId = body.partitionId
      ? String(body.partitionId)
      : dataset.files.find((file) => file.cold)?.cold.partitionId ?? dataset.coldPartition;
    const versionId = body.versionId
      ? String(body.versionId)
      : dataset.files.find((file) => file.cold)?.cold.versionId ?? dataset.coldVersion;
    if (!partitionId || !versionId) throw makeError('没有可回热的冷版本', 'NO_COLD_VERSION', 409);
    const { task, shared } = app.retrievals.ensure({
      datasetId: params.id,
      scope,
      fileId,
      partitionId,
      versionId,
      reason: '网页手动回热',
    });
    sendJson(res, 202, { task: app.retrievals.list().find((item) => item.id === task.id), shared });
  });

  route('POST', '/api/datasets/:id/evict-hot', async (req, res, params) => {
    const body = await readBody(req);
    const result = await app.evictHot(params.id, {
      reason: body.reason ? String(body.reason) : undefined,
    });
    sendJson(res, 200, result);
  });

  route('GET', '/api/tasks', async (req, res) => {
    sendJson(res, 200, { tasks: app.retrievals.list(), pins: app.pins.list() });
  });

  route('POST', '/api/tasks/:id/cancel', async (req, res, params) => {
    const ok = app.retrievals.cancel(params.id);
    if (!ok) throw makeError('取回任务不存在', 'TASK_NOT_FOUND', 404);
    sendJson(res, 200, { cancelled: true });
  });

  route('POST', '/api/datasets/:id/version', async (req, res, params) => {
    const body = await readBody(req);
    const pendingVersion = app.versioning.start(params.id, {
      partitionId: body.partitionId ? String(body.partitionId) : null,
      seed: body.seed ? String(body.seed) : null,
      sizeDelta: Number(body.sizeDelta ?? 0),
      actor: 'user',
    });
    sendJson(res, 202, { pendingVersion });
  });

  route('POST', '/api/datasets/:id/version/cancel', async (req, res, params) => {
    const ok = app.versioning.cancel(params.id);
    if (!ok) throw makeError('没有进行中的版本替换', 'NO_VERSION_REPLACEMENT', 409);
    sendJson(res, 200, { cancelled: true });
  });

  route('GET', '/api/partitions', async (req, res) => {
    const state = app.catalog.snapshot();
    const partitions = [];
    for (const partition of state.partitions) {
      const versions = await app.cold.listVersions(partition.id);
      partitions.push({
        ...partition,
        versions,
        bytes: versions.reduce((sum, version) => sum + version.bytes, 0),
        pinned: versions
          .filter((version) => app.pins.count(partition.id, version.versionId) > 0)
          .map((version) => ({
            versionId: version.versionId,
            pins: app.pins.count(partition.id, version.versionId),
          })),
      });
    }
    sendJson(res, 200, { partitions });
  });

  route('POST', '/api/partitions', async (req, res) => {
    const body = await readBody(req);
    const partition = app.catalog.addPartition({
      name: String(body.name ?? '').trim() || '冷分区',
      description: String(body.description ?? ''),
    });
    sendJson(res, 201, { partition });
  });

  route('POST', '/api/benchmark', async (req, res) => {
    const body = await readBody(req);
    const result = await app.runComparison({
      files: Math.min(12, Math.max(2, Number(body.files ?? 6))),
      sizeBytes: bytesFromMb(Math.min(2, Math.max(0.05, Number(body.sizeMb ?? 0.25)))),
      clients: Math.min(10, Math.max(1, Number(body.clients ?? 4))),
    });
    sendJson(res, 200, { result });
  });

  route('POST', '/api/reclaim', async (req, res) => {
    const body = await readBody(req);
    const result = await app.reclaimer.reclaim({ dryRun: Boolean(body.dryRun), actor: 'user' });
    sendJson(res, 200, { result });
  });

  route('POST', '/api/recovery', async (req, res) => {
    const body = await readBody(req);
    const report = await app.recovery.run({ verifyCold: Boolean(body.verifyCold), actor: 'user' });
    recoveryReport = report;
    sendJson(res, 200, { report });
  });

  route('POST', '/api/settings', async (req, res) => {
    const body = await readBody(req);
    const patch = {};
    if (body.hotCapacityBytes !== undefined) {
      patch.hotCapacityBytes = Math.max(1024 * 1024, Number(body.hotCapacityBytes));
    }
    if (body.coldLatencyBaseMs !== undefined) {
      patch.coldLatencyBaseMs = Math.min(5000, Math.max(0, Number(body.coldLatencyBaseMs)));
    }
    if (body.coldBandwidthBytesPerSec !== undefined) {
      patch.coldBandwidthBytesPerSec = Math.min(
        1024 * 1024 * 1024,
        Math.max(64 * 1024, Number(body.coldBandwidthBytesPerSec)),
      );
    }
    if (body.coldFaultMode !== undefined) {
      const allowed = ['none', 'error-once', 'error-always', 'corrupt-once', 'corrupt-always'];
      if (!allowed.includes(body.coldFaultMode)) throw makeError('未知故障模式', 'INVALID_FAULT_MODE', 400);
      patch.coldFaultMode = body.coldFaultMode;
    }
    if (body.coldFaultFileId !== undefined) patch.coldFaultFileId = String(body.coldFaultFileId);
    if (body.cancelPolicy !== undefined) {
      if (!['continue', 'cancel-if-idle'].includes(body.cancelPolicy)) {
        throw makeError('未知取消策略', 'INVALID_CANCEL_POLICY', 400);
      }
      patch.cancelPolicy = body.cancelPolicy;
    }
    if (body.versionGraceMs !== undefined) {
      patch.versionGraceMs = Math.max(0, Number(body.versionGraceMs));
    }
    if (body.maxVerifyRetries !== undefined) {
      patch.maxVerifyRetries = Math.min(5, Math.max(0, Number(body.maxVerifyRetries)));
    }
    const settings = await app.catalog.updateSettings(patch);
    sendJson(res, 200, { settings });
  });

  async function serveStatic(req, res, pathname) {
    const target = pathname === '/' ? '/index.html' : pathname;
    const full = path.join(PUBLIC_DIR, path.normalize(target));
    if (full !== PUBLIC_DIR && !full.startsWith(`${PUBLIC_DIR}${path.sep}`)) {
      res.writeHead(403); res.end('Forbidden'); return;
    }
    try {
      const info = await stat(full);
      if (!info.isFile()) throw Object.assign(new Error('not file'), { code: 'ENOENT' });
      const ext = path.extname(full).toLowerCase();
      res.writeHead(200, {
        'content-type': MIME[ext] ?? 'application/octet-stream',
        'content-length': info.size,
        'cache-control': 'no-cache',
      });
      createReadStream(full).pipe(res);
    } catch {
      res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
      res.end('Not Found');
    }
  }

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://${req.headers.host ?? 'localhost'}`);
    const pathname = url.pathname;
    try {
      if (req.method === 'GET' && !pathname.startsWith('/api/')) {
        await serveStatic(req, res, pathname);
        return;
      }
      for (const { method, pattern, handler } of routes) {
        if (method !== req.method) continue;
        const params = routeParams(pattern, pathname);
        if (!params) continue;
        await handler(req, res, params, url);
        return;
      }
      sendJson(res, 404, { error: '接口不存在', code: 'NOT_FOUND' });
    } catch (error) {
      if (res.headersSent) { res.destroy(); return; }
      if (!error.statusCode) log('请求失败', req.method, pathname, error.stack ?? error.message);
      sendError(res, error);
    }
  });

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, resolve);
  });
  const address = server.address();
  log(`服务启动 http://${host}:${address.port}（数据目录 ${app.root}）`);

  return {
    server,
    app,
    port: address.port,
    url: `http://${host}:${address.port}`,
    statePayload,
    async close() {
      await app.shutdown();
      await new Promise((resolve) => server.close(resolve));
    },
  };
}
