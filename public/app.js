const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

const state = {
  data: null,
  polling: true,
  lastLogAt: null,
  logLines: [],
  busy: new Set(),
  settingsTouchedAt: 0,
};

const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
const esc = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ESC[char]);

function fmtBytes(bytes) {
  const n = Number(bytes ?? 0);
  if (!Number.isFinite(n)) return '—';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 ** 3) return `${(n / 1024 / 1024).toFixed(2)} MiB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`;
}

function fmtMs(ms) {
  if (ms === null || ms === undefined) return '—';
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(2)} s`;
}

function fmtTime(iso) {
  if (!iso) return '—';
  const date = new Date(iso);
  return `${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}:${String(date.getSeconds()).padStart(2, '0')}`;
}

function shortHash(hash) {
  return hash ? `${hash.slice(0, 10)}…${hash.slice(-4)}` : '—';
}

function toast(message, kind = '') {
  const el = $('#toast');
  el.textContent = message;
  el.className = `toast ${kind}`;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => el.classList.add('hidden'), 4200);
}

function log(message) {
  const line = `[${fmtTime(new Date().toISOString())}] ${message}`;
  state.logLines.unshift(line);
  state.logLines = state.logLines.slice(0, 200);
  $('#log').textContent = state.logLines.join('\n');
}

async function api(path, { method = 'GET', body = null } = {}) {
  const options = { method, headers: {} };
  if (body !== null) {
    options.headers['content-type'] = 'application/json';
    options.body = JSON.stringify(body);
  }
  const response = await fetch(path, options);
  const type = response.headers.get('content-type') ?? '';
  if (!response.ok) {
    let detail = { error: response.statusText, code: 'HTTP_ERROR' };
    if (type.includes('application/json')) {
      try { detail = await response.json(); } catch { /* ignore */ }
    }
    const error = new Error(detail.error ?? `HTTP ${response.status}`);
    error.code = detail.code;
    error.status = response.status;
    throw error;
  }
  if (type.includes('application/json')) return response.json();
  return response;
}

async function action(name, fn, { quiet = false } = {}) {
  if (state.busy.has(name)) return;
  state.busy.add(name);
  renderButtons();
  try {
    const result = await fn();
    if (!quiet) toast('操作已完成', 'ok');
    await refresh();
    return result;
  } catch (error) {
    if (error.name !== 'AbortError') {
      toast(`${error.code ?? ''} ${error.message}`.trim(), 'bad');
      log(`失败：${error.code ?? ''} ${error.message}`);
    }
    return null;
  } finally {
    state.busy.delete(name);
    renderButtons();
  }
}

function renderButtons() {
  for (const button of $$('[data-busy-key]')) {
    const key = button.dataset.busyKey;
    if (state.busy.has(key)) {
      button.disabled = true;
      if (!button.dataset.label) button.dataset.label = button.textContent;
      button.innerHTML = '<span class="spin"></span>';
    } else if (button.dataset.label) {
      button.disabled = false;
      button.textContent = button.dataset.label;
      delete button.dataset.label;
    }
  }
}

// ---------- 渲染 ----------

function renderStats(data) {
  const stats = data.stats ?? {};
  const datasets = data.datasets ?? [];
  const hotFiles = datasets.flatMap((dataset) => dataset.files.filter((file) => file.hot));
  const coldFiles = datasets.flatMap((dataset) => dataset.files.filter((file) => file.cold));
  const runningTasks = (data.runtime?.retrievals ?? []).filter((task) => task.status === 'running');
  const activeMigrations = (data.runtime?.migrations ?? []).length;
  const cards = [
    { k: '数据集', v: datasets.length, d: `${datasets.reduce((sum, ds) => sum + ds.files.length, 0)} 个文件` },
    { k: '热层文件', v: hotFiles.length, d: `${fmtBytes(data.usage?.hotBytes)} / ${fmtBytes(data.settings?.hotCapacityBytes)}` },
    { k: '冷层文件', v: coldFiles.length, d: `${fmtBytes(data.usage?.coldBytes)} 占用` },
    { k: '冷读累计', v: fmtBytes(stats.coldFetchBytes ?? 0), d: `${stats.coldFetchCount ?? 0} 次取回请求` },
    { k: '共享任务', v: runningTasks.length, d: `累计 ${stats.coldTasks ?? 0} 个任务 / ${stats.sharedJoins ?? 0} 次复用加入` },
    { k: '取消后行为', v: stats.detachedContinues ? '继续' : (stats.detachedAborts ? '中止' : '—'), d: `继续 ${stats.detachedContinues ?? 0} 次 / 无人订阅中止 ${stats.detachedAborts ?? 0} 次` },
    { k: '一致性', v: (data.consistency ?? []).length === 0 ? '正常' : `${data.consistency.length} 项`, d: activeMigrations ? `${activeMigrations} 个迁移进行中` : '无迁移进行中' },
  ];
  $('#stats').innerHTML = cards.map((card) => `
    <div class="card">
      <div class="k">${esc(card.k)}</div>
      <div class="v num">${esc(card.v)}</div>
      <div class="d">${esc(card.d)}</div>
    </div>`).join('');
}

function renderTiers(data) {
  const usage = data.usage ?? {};
  const pct = Math.max(0, Math.min(100, usage.hotPct ?? 0));
  $('#hot-bar').style.width = `${pct}%`;
  $('#hot-usage').textContent = `${fmtBytes(usage.hotBytes)} / ${fmtBytes(usage.hotCapacityBytes)}（${pct}%）`;
  const reclaimable = (data.partitions ?? []).flatMap((partition) => partition.versions)
    .filter((version) => !(version.pinned?.length));
  $('#cold-usage').textContent = `${fmtBytes(usage.coldBytes)} · ${(data.partitions ?? []).length} 个分区 · ${(data.partitions ?? []).reduce((sum, p) => sum + (p.versions?.length ?? 0), 0)} 个版本`;
  const pins = data.runtime?.pins ?? [];
  $('#cold-note').textContent = pins.length
    ? `${pins.length} 个版本正被进行中的查询固定，回收会跳过它们。`
    : '读取受基准延迟与带宽限制，可注入故障。';
  const hotNote = state.data?.runtime?.retrievals?.some((task) => task.status === 'running' && task.bytesDownloaded > 0)
    ? '正在往热盘写回取回内容；读者租约保护正在读取的文件。'
    : '内容寻址文件名：同一份字节可被多个查询安全共享。';
  $('#hot-note').textContent = hotNote;
}

function renderSettings(data) {
  if (Date.now() - state.settingsTouchedAt < 1500) return;
  const settings = data.settings ?? {};
  $('#latency').value = settings.coldLatencyBaseMs ?? 35;
  $('#latency-out').textContent = `${settings.coldLatencyBaseMs ?? 35} ms`;
  $('#bandwidth').value = settings.coldBandwidthBytesPerSec ?? 6291456;
  $('#bandwidth-out').textContent = `${fmtBytes(settings.coldBandwidthBytesPerSec ?? 0)}/s`;
  $('#fault').value = settings.coldFaultMode ?? 'none';
  $('#cancel-policy').value = settings.cancelPolicy ?? 'continue';
  $('#retries').value = settings.maxVerifyRetries ?? 1;
  $('#grace').value = Math.round((settings.versionGraceMs ?? 60000) / 1000);
}

function fileLocation(file) {
  const badges = [];
  if (file.hot) badges.push('<span class="badge hot">热</span>');
  if (file.cold) badges.push(`<span class="badge cold">冷 ${esc(file.cold.versionId?.slice(0, 10) ?? '')}</span>`);
  if (!file.hot && !file.cold) badges.push('<span class="badge bad">无副本</span>');
  if (file.readable === 'cold') badges.push('<span class="badge">读冷</span>');
  if (file.pendingReheat) badges.push('<span class="badge warn">回热中</span>');
  return badges.join('');
}

function fileEvidence(file) {
  if (file.lastError) return `<span title="${esc(file.lastError)}">⚠ ${esc(file.lastError)}</span>`;
  if (file.readable === 'hot' && file.hot) return `热副本 ${shortHash(file.hot.sha256)}`;
  if (file.cold) return `冷对象 ${shortHash(file.cold.sha256)}`;
  return '—';
}

function renderDatasets(data) {
  const container = $('#datasets');
  const datasets = data.datasets ?? [];
  if (!datasets.length) {
    container.innerHTML = '<div class="empty">还没有数据集。先在右上输入数量与大小，然后点“新建数据集”。</div>';
    return;
  }
  const partitions = data.partitions ?? [];
  container.innerHTML = datasets.map((dataset) => {
    const m = dataset.migration;
    const allHot = dataset.files.every((file) => file.hot);
    const allCold = dataset.files.every((file) => file.cold);
    const migrationRunning = m && (m.status === 'running' || m.status === 'switching');
    const progress = m && m.totalBytes
      ? Math.min(100, Math.round(((m.phase === 'hot-reclaiming' || m.status === 'completed') ? m.totalBytes : (m.coldWrittenBytes ?? 0)) / Math.max(1, m.totalBytes) * 100))
      : 0;
    const rows = dataset.files.map((file) => `
      <div class="file-row">
        <div class="file-name" title="${esc(file.name)}">${esc(file.name)}</div>
        <div class="file-size">${fmtBytes(file.size)}</div>
        <div class="file-loc">${fileLocation(file)}</div>
        <div class="file-evidence" title="${esc(file.lastError ?? file.evidence ?? '')}">${fileEvidence(file)}</div>
        <div class="file-actions">
          <button class="btn tiny" data-busy-key="q:${dataset.id}:${file.id}" data-query="${dataset.id}:${file.id}">查询</button>
          ${file.cold && !file.hot ? `<button class="btn tiny" data-busy-key="rh:${dataset.id}:${file.id}" data-reheat="${dataset.id}:${file.id}">按文件回热</button>` : ''}
        </div>
      </div>`).join('');
    const pendingVersion = dataset.pendingVersion;
    return `
    <article class="dataset" data-ds="${esc(dataset.id)}">
      <div class="dataset-head">
        <div class="dataset-title">
          <h3>${esc(dataset.name)}</h3>
          <span class="id">${esc(dataset.id)} · v${dataset.currentVersion}</span>
        </div>
        <div class="dataset-meta">
          <span class="badge ${allHot ? 'hot' : ''}">${allHot ? '全部热' : (allCold ? '全部冷' : '混合')}</span>
          <span class="badge">${dataset.files.length} 文件 · ${fmtBytes(dataset.totalBytes)}</span>
          ${dataset.coldVersion ? `<span class="badge cold" title="${esc(dataset.coldVersion)}">冷 ${esc(dataset.coldVersion.slice(0, 12))}</span>` : ''}
          ${pendingVersion && pendingVersion.status === 'running' ? '<span class="badge warn">版本替换中</span>' : ''}
        </div>
        <div class="ds-actions">
          ${allHot && !migrationRunning ? `<button class="btn tiny" data-busy-key="mg:${dataset.id}" data-migrate="${dataset.id}">迁移到冷</button>` : ''}
          ${migrationRunning ? `<button class="btn tiny danger" data-busy-key="mgc:${dataset.id}" data-migrate-cancel="${dataset.id}">取消迁移</button>` : ''}
          ${!allHot && !migrationRunning ? `<button class="btn tiny" data-busy-key="rhp:${dataset.id}" data-reheat-all="${dataset.id}">整分区回热</button>` : ''}
          ${allCold ? `<button class="btn tiny" data-busy-key="ver:${dataset.id}" data-version="${dataset.id}">替换冷版本</button>` : ''}
          ${allCold ? `<button class="btn tiny" data-busy-key="rh2:${dataset.id}" data-reheat-all="${dataset.id}">重新回热</button>` : ''}
        </div>
      </div>
      <div class="dataset-body">
        ${migrationRunning || (m && m.status !== 'completed') ? `
          <div class="progress"><span style="width:${progress}%"></span></div>
          <div class="migration-note">
            迁移 ${esc(m.id.slice(0, 10))} · ${esc(m.phase)} · 已写冷 ${fmtBytes(m.coldWrittenBytes)} / ${fmtBytes(m.totalBytes)}
            ${m.verifiedFiles ? ` · 已校验 ${m.verifiedFiles}/${m.totalFiles}` : ''}
            ${m.deletedHotFiles ? ` · 已回收热 ${m.deletedHotFiles}` : ''}
          </div>` : ''}
        ${m && m.status === 'completed' && m.note ? `<div class="migration-note">${esc(m.note)}</div>` : ''}
        ${m && m.status === 'failed' ? `<div class="migration-note" style="color:var(--bad)">迁移失败：${esc(m.error ?? m.note)}</div>` : ''}
        ${m && m.status === 'cancelled' ? `<div class="migration-note" style="color:var(--warn)">已取消：${esc(m.note)}</div>` : ''}
        ${pendingVersion && pendingVersion.status !== 'completed' && pendingVersion.status !== undefined ? `<div class="migration-note">版本替换 ${esc(pendingVersion.status)} · ${esc(pendingVersion.note ?? '')}</div>` : ''}
        <div class="files">${rows}</div>
      </div>
    </article>`;
  }).join('');
}

function renderTasks(data) {
  const tasks = (data.runtime?.retrievals ?? []).slice().sort((a, b) => String(b.startedAt).localeCompare(String(a.startedAt)));
  const container = $('#tasks');
  if (!tasks.length) {
    container.innerHTML = '<div class="empty">暂无取回任务。冷层查询或手动回热会创建共享任务。</div>';
    return;
  }
  container.innerHTML = tasks.map((task) => {
    const pct = task.bytesTotal ? Math.min(100, Math.round((task.bytesDownloaded + task.filesReused * 0) / task.bytesTotal * 100)) : 0;
    const statusClass = task.status === 'completed' ? 'ok' : (task.status === 'failed' ? 'bad' : (task.detached ? 'warn' : ''));
    const statusText = {
      running: task.detached ? '运行中（已无订阅者）' : '运行中',
      completed: '已完成',
      failed: '部分失败',
      cancelled: '已取消',
      interrupted: '已中断',
    }[task.status] ?? task.status;
    return `
    <div class="task">
      <div class="t-main">
        <div class="t-title">
          <span class="badge ${statusClass}">${esc(statusText)}</span>
          <span>${task.scope === 'file' ? '单文件' : '整分区'} 取回 · ${esc(task.datasetName ?? task.datasetId)}</span>
          <span class="badge">订阅 ${task.subscribers}</span>
          ${task.detached ? '<span class="badge warn">查询取消后仍在继续</span>' : ''}
        </div>
        <div class="t-sub">${esc(task.id)} · 版本 ${esc((task.versionId ?? '').slice(0, 16))} · ${esc(task.currentFile ?? '')}</div>
        <div class="progress"><span style="width:${pct}%"></span></div>
      </div>
      <div class="t-stats">
        已下载 ${fmtBytes(task.bytesDownloaded)} / ${fmtBytes(task.bytesTotal)}<br/>
        ${task.filesDone}/${task.totalFiles} 文件 · 复用 ${task.filesReused} · 失败 ${task.filesFailed} · 重试 ${task.retries}
        ${task.error ? `<br/><span style="color:var(--bad)">${esc(task.error)}</span>` : ''}
      </div>
      <div>${task.status === 'running' ? `<button class="btn tiny danger" data-task-cancel="${esc(task.id)}">取消任务</button>` : ''}</div>
    </div>`;
  }).join('');
}

function renderPartitions(data) {
  const partitions = data.partitions ?? [];
  const container = $('#partitions');
  if (!partitions.length) {
    container.innerHTML = '<div class="empty">还没有冷分区。迁移时会自动创建，也可先手动创建。</div>';
    return;
  }
  const referenced = new Set();
  for (const dataset of data.datasets ?? []) {
    for (const file of dataset.files) {
      if (file.cold?.versionId) referenced.add(`${file.cold.partitionId}:${file.cold.versionId}`);
    }
  }
  container.innerHTML = partitions.map((partition) => {
    const versions = partition.versions ?? [];
    const rows = versions.map((version) => {
      const isReferenced = referenced.has(`${partition.id}:${version.versionId}`);
      const pins = partition.pinned?.find((item) => item.versionId === version.versionId)?.pins ?? 0;
      return `
      <div class="version ${isReferenced ? '' : 'orphan'}">
        <div class="vid" title="${esc(version.versionId)}">${esc(version.versionId)} ${isReferenced ? '' : '（不再引用）'}</div>
        <div class="num">${fmtBytes(version.bytes)}</div>
        <div>${version.temporary ? '<span class="badge warn">构建中</span>' : `<span class="badge ${pins ? 'warn' : 'ok'}">${pins ? `固定 ×${pins}` : '可回收'}</span>`}</div>
        <div class="vsum" title="${esc(version.checksum)}">校验和 ${esc(shortHash(version.checksum))}</div>
        <div class="right">${isReferenced ? '' : '<button class="btn tiny" data-reclaim-version="' + esc(partition.id) + ':' + esc(version.versionId) + '">回收</button>'}</div>
      </div>`;
    }).join('');
    return `
    <div class="partition">
      <div class="partition-head">
        <div><strong>${esc(partition.name)}</strong> <span class="badge">${esc(partition.id)}</span></div>
        <div class="num" style="color:var(--text-faint);font-size:12px">${versions.length} 个版本 · ${fmtBytes(partition.bytes)}</div>
      </div>
      <div class="version-list">${rows || '<div class="empty">此分区还没有已提交版本。</div>'}</div>
    </div>`;
  }).join('');
}

function renderConsistency(data) {
  const container = $('#consistency');
  const problems = data.consistency ?? [];
  const datasets = data.datasets ?? [];
  const byDataset = new Map();
  for (const problem of problems) {
    if (!byDataset.has(problem.datasetId)) byDataset.set(problem.datasetId, []);
    byDataset.get(problem.datasetId).push(problem);
  }
  if (!datasets.length) {
    container.innerHTML = '<div class="empty">暂无数据。</div>';
    return;
  }
  container.innerHTML = datasets.map((dataset) => {
    const list = byDataset.get(dataset.id) ?? [];
    const rows = dataset.files.map((file) => {
      const filesProblems = list.filter((problem) => problem.fileId === file.id);
      const position = file.hot ? 'hot' : (file.cold ? 'cold' : 'none');
      return `
        <div class="cons-file">
          <span class="cf-name" title="${esc(file.name)}">${esc(file.name)}</span>
          <span class="cf-pos" style="color:${position === 'hot' ? 'var(--hot)' : (position === 'cold' ? 'var(--cold)' : 'var(--bad)')}">${position}${file.version > 1 ? ` v${file.version}` : ''}</span>
          <span class="cf-hash" title="${esc(file.hot?.evidence ?? file.cold?.evidence ?? '')}">${shortHash(file.hot?.sha256 ?? file.cold?.sha256)}</span>
          ${filesProblems.map((problem) => `<div class="problem ${problem.severity === 'critical' ? '' : 'warn'}">${esc(problem.code)} ${esc(problem.message)}</div>`).join('')}
        </div>`;
    }).join('');
    return `<div class="cons-ds"><h3><span>${esc(dataset.name)}</span><span>${list.length ? `<span class="badge bad">${list.length} 项问题</span>` : '<span class="badge ok">一致</span>'}</span></h3>${rows}</div>`;
  }).join('');
}

function renderEvents(data) {
  const events = (data.events ?? []).slice(-80).reverse();
  const container = $('#events');
  if (!events.length) {
    container.innerHTML = '<div class="empty">暂无事件。</div>';
    return;
  }
  container.innerHTML = events.map((event) => {
    let kindClass = '';
    if (/(failed|interrupted|no-replica|cancelled|verify-failed|hot-missing)/.test(event.kind)) kindClass = 'k-bad';
    else if (/(completed|created|committed|reheated|replaced)/.test(event.kind)) kindClass = 'k-ok';
    const evidence = event.evidence && Object.keys(event.evidence).length
      ? Object.entries(event.evidence)
        .filter(([, value]) => value !== null && value !== undefined && typeof value !== 'object')
        .map(([key, value]) => `${key}=${value}`)
        .join(' · ')
      : '';
    return `
    <div class="event ${kindClass}">
      <span class="e-time">${fmtTime(event.at)}</span>
      <span class="e-kind" title="${esc(event.kind)}">${esc(event.kind)}</span>
      <span class="e-reason" title="${esc(event.reason)}">${esc(event.reason)}</span>
      ${evidence ? `<span class="e-evidence" title="${esc(evidence)}">${esc(evidence)}</span>` : ''}
    </div>`;
  }).join('');
}

function render(data) {
  state.data = data;
  $('#conn-pill').textContent = '已连接';
  $('#conn-pill').className = 'pill ok';
  $('#gen-pill').textContent = `generation ${data.generation} · ${fmtTime(data.serverTime)}`;
  renderStats(data);
  renderTiers(data);
  renderSettings(data);
  renderDatasets(data);
  renderTasks(data);
  renderPartitions(data);
  renderConsistency(data);
  renderEvents(data);
  renderButtons();
}

async function refresh() {
  try {
    const data = await api('/api/state');
    render(data);
  } catch (error) {
    $('#conn-pill').textContent = '连接失败';
    $('#conn-pill').className = 'pill bad';
    log(`状态刷新失败：${error.message}`);
  }
}

// ---------- 事件绑定 ----------

document.addEventListener('click', async (event) => {
  const button = event.target.closest('button');
  if (!button) return;

  const queryTarget = button.dataset.query;
  if (queryTarget) {
    const [, datasetId, fileId] = queryTarget.match(/^([^:]+):(.+)$/) ?? [];
    await action(`q:${datasetId}:${fileId}`, async () => {
      const started = performance.now();
      const response = await api(`/api/datasets/${datasetId}/query`, {
        method: 'POST', body: { fileId, mode: 'auto' },
      });
      const elapsed = Math.round(performance.now() - started);
      const body = await response.arrayBuffer();
      log(`查询 ${fileId}：${response.headers.get('x-tier-read-from') ?? '?'} 层，${fmtBytes(body.byteLength)}，${fmtMs(elapsed)}`
        + `（sha256 ${(response.headers.get('x-tier-sha256') ?? '').slice(0, 12)}…）`);
    });
    return;
  }
  if (button.dataset.reheat) {
    const [datasetId, fileId] = button.dataset.reheat.split(':');
    await action(`rh:${datasetId}:${fileId}`, async () => {
      const result = await api(`/api/datasets/${datasetId}/reheat`, {
        method: 'POST', body: { scope: 'file', fileId },
      });
      log(`已创建单文件取回任务 ${result.task?.id}${result.shared ? '（复用进行中的任务）' : ''}`);
    });
    return;
  }
  if (button.dataset.reheatAll) {
    await action(`rhp:${button.dataset.reheatAll}`, async () => {
      const result = await api(`/api/datasets/${button.dataset.reheatAll}/reheat`, {
        method: 'POST', body: { scope: 'dataset' },
      });
      log(`已创建整分区取回任务 ${result.task?.id}${result.shared ? '（复用进行中的任务）' : ''}`);
    });
    return;
  }
  if (button.dataset.migrate) {
    await action(`mg:${button.dataset.migrate}`, async () => {
      const result = await api(`/api/datasets/${button.dataset.migrate}/migrate`, { method: 'POST', body: {} });
      log(`迁移已启动 ${result.migration?.id}`);
    });
    return;
  }
  if (button.dataset.migrateCancel) {
    await action(`mgc:${button.dataset.migrateCancel}`, async () => {
      await api(`/api/datasets/${button.dataset.migrateCancel}/migrate/cancel`, { method: 'POST', body: {} });
      log('已请求取消迁移；热副本在冷副本登记前不会删除');
    });
    return;
  }
  if (button.dataset.version) {
    await action(`ver:${button.dataset.version}`, async () => {
      const result = await api(`/api/datasets/${button.dataset.version}/version`, { method: 'POST', body: {} });
      log(`版本替换已启动 ${result.pendingVersion?.versionId}`);
    });
    return;
  }
  if (button.dataset.evict) {
    await action(`ev:${button.dataset.evict}`, async () => {
      const result = await api(`/api/datasets/${button.dataset.evict}/evict-hot`, { method: 'POST', body: {} });
      log(`已释放热副本 ${result.dropped} 个，回收 ${fmtBytes(result.freed)}`);
    });
    return;
  }
  if (button.dataset.taskCancel) {
    await action(`tc:${button.dataset.taskCancel}`, async () => {
      await api(`/api/tasks/${button.dataset.taskCancel}/cancel`, { method: 'POST', body: {} });
      log(`已取消取回任务 ${button.dataset.taskCancel}`);
    });
    return;
  }
  if (button.dataset.reclaimVersion) {
    // 回收以“计划”为单位执行：先预演，能看到哪些版本因租约/宽限期被跳过，
    // 再决定是否真正删除。
    await action(`rv:${button.dataset.reclaimVersion}`, async () => {
      const preview = await api('/api/reclaim', { method: 'POST', body: { dryRun: true } });
      const target = preview.result?.actions?.find((actionItem) => (
        actionItem.versionId && `${actionItem.partitionId}:${actionItem.versionId}` === button.dataset.reclaimVersion
      ));
      if (!target) {
        log('该版本当前不可回收（可能被查询固定或在宽限期内），未执行删除');
        renderReclaim(preview.result);
        return;
      }
      const result = await api('/api/reclaim', { method: 'POST', body: {} });
      log(`回收完成：${fmtBytes(result.result?.freedBytes)}；${result.result?.actions?.length ?? 0} 个动作`);
      renderReclaim(result.result);
    });
    return;
  }
  if (button.id === 'create-ds') {
    await action('create', async () => {
      const name = $('#ds-name').value.trim();
      const result = await api('/api/datasets', {
        method: 'POST',
        body: { name, count: Number($('#ds-count').value), sizeMb: Number($('#ds-size').value) },
      });
      $('#ds-name').value = '';
      log(`已创建 ${result.dataset?.id}，${result.dataset?.files?.length} 个文件`);
    });
    return;
  }
  if (button.id === 'refresh') {
    await refresh();
    return;
  }
  if (button.id === 'reclaim-dry' || button.id === 'reclaim-run') {
    const dryRun = button.id === 'reclaim-dry';
    await action(button.id, async () => {
      const result = await api('/api/reclaim', { method: 'POST', body: { dryRun } });
      renderReclaim(result.result);
      log(`${dryRun ? '回收预演' : '回收执行'}：${fmtBytes(result.result?.freedBytes)}，跳过 ${result.result?.actions?.filter((a) => a.type.endsWith('skipped') || a.type.endsWith('skip')).length ?? 0} 项`);
    });
    return;
  }
  if (button.id === 'bench-run') {
    await action('bench', async () => {
      const result = await api('/api/benchmark', {
        method: 'POST',
        body: {
          files: Number($('#bench-files').value),
          sizeMb: Number($('#bench-size').value),
          clients: Number($('#bench-clients').value),
        },
      });
      renderBenchmark(result.result);
      log(`对比完成：按文件首屏 ${fmtMs(result.result?.summary?.latency?.file)}，整分区 ${fmtMs(result.result?.summary?.latency?.partition)}`);
    });
    return;
  }
  if (button.id === 'recover') {
    await action('recover', async () => {
      const result = await api('/api/recovery', { method: 'POST', body: { verifyCold: true } });
      log(`恢复扫描：${result.report?.actions?.length ?? 0} 项修正，${result.report?.problems?.length ?? 0} 项问题`);
    });
    return;
  }
});

function renderBenchmark(result) {
  const container = $('#benchmark');
  if (!result) {
    container.innerHTML = '<div class="empty">尚未运行。点击“运行对比”会创建两组同规格数据集，分别测量。</div>';
    return;
  }
  const s = result.summary ?? {};
  const rows = (result.results ?? []).map((run) => `
    <div class="bench-row">
      <div class="bench-label">${run.label === 'file' ? '按文件读取' : '整分区回热'}</div>
      <div class="num">首屏 ${fmtMs(run.firstByteMs)}</div>
      <div class="num">最慢查询 ${fmtMs(run.slowestQueryMs)}</div>
      <div class="num">冷流量 ${fmtBytes(run.coldBytes)}</div>
      <div class="num">热缓存 +${fmtBytes(run.hotCacheDeltaBytes)}</div>
      <div class="num">冷任务 ${run.coldTasks}</div>
    </div>`).join('');
  container.innerHTML = `
    <div class="bench-rows">${rows}</div>
    <div class="bench-summary">
      整分区相对按文件：首屏 ${s.latency?.delta >= 0 ? '+' : ''}${fmtMs(s.latency?.delta)} ·
      流量 ${fmtBytes(s.traffic?.delta)} ·
      缓存占用 ${fmtBytes(s.cache?.delta)} ·
      两者都只产生 ${s.tasks?.file} / ${s.tasks?.partition} 个共享冷任务（${result.clients} 个并发查询只下载一次）
    </div>
    <details><summary>原始数据</summary><pre class="log">${esc(JSON.stringify(result.results, null, 2))}</pre></details>`;
}

function renderReclaim(result) {
  const container = $('#reclaim-result');
  if (!result) { container.classList.add('hidden'); return; }
  container.classList.remove('hidden');
  const rows = (result.actions ?? []).map((action) => `
    <div class="action-row">
      <span class="action-type">${esc(action.type)}</span>
      <span class="action-reason">${esc(action.reason)}${action.versionId ? ` · ${esc(action.versionId.slice(0, 16))}` : ''}</span>
      <span class="action-bytes">${action.bytes ? fmtBytes(action.bytes) : ''}</span>
    </div>`).join('') || '<div class="empty">没有可回收对象。</div>';
  container.innerHTML = `
    <h3>${result.dryRun ? '回收预演' : '回收执行'} · 共 ${fmtBytes(result.freedBytes)} · ${fmtMs(result.durationMs)}</h3>
    ${rows}`;
}

let settingsTimer = null;
function queueSettings(patch) {
  state.settingsTouchedAt = Date.now();
  clearTimeout(settingsTimer);
  settingsTimer = setTimeout(async () => {
    try {
      await api('/api/settings', { method: 'POST', body: patch });
      log(`设置已更新：${Object.entries(patch).map(([key, value]) => `${key}=${value}`).join(', ')}`);
      await refresh();
    } catch (error) {
      toast(error.message, 'bad');
    }
  }, 250);
}

$('#latency').addEventListener('input', (event) => {
  const value = Number(event.target.value);
  $('#latency-out').textContent = `${value} ms`;
  queueSettings({ coldLatencyBaseMs: value });
});
$('#bandwidth').addEventListener('input', (event) => {
  const value = Number(event.target.value);
  $('#bandwidth-out').textContent = `${fmtBytes(value)}/s`;
  queueSettings({ coldBandwidthBytesPerSec: value });
});
$('#fault').addEventListener('change', (event) => queueSettings({ coldFaultMode: event.target.value }));
$('#cancel-policy').addEventListener('change', (event) => queueSettings({ cancelPolicy: event.target.value }));
$('#retries').addEventListener('change', (event) => queueSettings({ maxVerifyRetries: Number(event.target.value) }));
$('#grace').addEventListener('change', (event) => queueSettings({ versionGraceMs: Number(event.target.value) * 1000 }));

document.addEventListener('keydown', (event) => {
  if (event.key === 'Enter' && document.activeElement?.id === 'ds-name') {
    $('#create-ds').click();
  }
});

await refresh();
setInterval(() => { if (state.polling) refresh(); }, 1200);
