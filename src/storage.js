/**
 * 本地热盘（快盘）管理。
 *
 * 文件名按内容寻址：<fileId>-<sha 前 12 位>.bin。这样版本替换不会覆盖旧文件，
 * 进行中的查询可以继续持有旧文件句柄；回收时通过租约等待读者释放，
 * 避免“迁移/回收删掉正在被读的唯一副本”。
 */
import path from 'node:path';
import { open, readdir, rename, stat, unlink } from 'node:fs/promises';
import {
  ensureDir, makeError, pathExists, safeJoin, safeRm, sha256File,
} from './util.js';

export class PathLeases {
  #counts = new Map();
  #closing = new Set();
  #waiters = new Set();

  #isClosing(target) {
    for (const prefix of this.#closing) {
      if (target === prefix || target.startsWith(`${prefix}${path.sep}`)) return true;
    }
    return false;
  }

  acquire(target) {
    if (this.#isClosing(target)) {
      throw makeError('热副本正在回收，改用冷副本读取', 'HOT_DRAINING', 409);
    }
    // 回收完成到租约获取之间文件可能已被删除；这里不预判存在性，
    // 由调用方在拿到摘要为空（缺失）时回退冷层。

    this.#counts.set(target, (this.#counts.get(target) ?? 0) + 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = (this.#counts.get(target) ?? 1) - 1;
      if (next <= 0) this.#counts.delete(target);
      else this.#counts.set(target, next);
      for (const wake of this.#waiters) wake();
    };
  }

  activeFor(prefix) {
    let total = 0;
    for (const [target, count] of this.#counts) {
      if (target === prefix || target.startsWith(`${prefix}${path.sep}`)) total += count;
    }
    return total;
  }

  /**
   * 获取租约；若该路径正在被回收（closing），短暂等待回收结束后重试。
   * 用于“取回后写入内容路径并登记目录”这段必须与回收互斥的操作。
   */
  async acquireWhenOpen(target, { retries = 2000, delayMs = 5 } = {}) {
    for (let attempt = 0; ; attempt += 1) {
      try {
        return this.acquire(target);
      } catch (error) {
        if (error.code !== 'HOT_DRAINING' || attempt >= retries) throw error;
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
  }

  async drain(prefix) {
    return this.runExclusive(prefix, () => {});
  }

  /**
   * 在“关闭租约”状态下执行 fn：先标记该路径正在回收（新读者会收到 HOT_DRAINING），
   * 再等已持有的读者释放，最后才执行删除。删除完成前不会清除关闭标记——否则
   * 会出现“刚排空又来了新读者、文件却被 unlink”的窗口。
   */
  async runExclusive(prefix, fn) {
    this.#closing.add(prefix);
    try {
      while (this.activeFor(prefix) > 0) {
        await new Promise((resolve) => {
          const wake = () => {
            this.#waiters.delete(wake);
            resolve();
          };
          this.#waiters.add(wake);
        });
      }
      return await fn();
    } finally {
      this.#closing.delete(prefix);
    }
  }
}

export class HotStore {
  constructor({ root }) {
    this.root = path.resolve(root);
    this.leases = new PathLeases();
    this.stagingRoot = path.join(this.root, '.staging');
    this.usageCache = { at: 0, bytes: 0 };
    this.log = null;
  }

  async init() {
    await ensureDir(this.root);
    await ensureDir(this.stagingRoot);
    // 清理上次进程留下的半成品。
    const leftovers = await readdir(this.stagingRoot).catch(() => []);
    for (const entry of leftovers) await safeRm(path.join(this.stagingRoot, entry));
  }

  datasetRoot(datasetId) {
    return safeJoin(this.root, datasetId);
  }

  contentPath(datasetId, fileId, sha256) {
    if (!sha256 || !/^[a-f0-9]{64}$/.test(sha256)) {
      throw makeError('非法内容摘要', 'INVALID_SHA256', 500);
    }
    return safeJoin(this.root, datasetId, `${fileId}-${sha256.slice(0, 12)}.bin`);
  }

  stagingPath(taskId, fileId) {
    return safeJoin(this.stagingRoot, taskId, `${fileId}.bin`);
  }

  async stage(taskId, fileId) {
    const target = this.stagingPath(taskId, fileId);
    await ensureDir(path.dirname(target));
    return target;
  }

  /** 校验暂存文件摘要后原子改名到内容路径。 */
  async promoteStaged({ taskId, fileId, datasetId, sha256 }) {
    const staged = this.stagingPath(taskId, fileId);
    const actual = await sha256File(staged);
    if (actual !== sha256) {
      await safeRm(staged);
      throw makeError('暂存文件摘要不一致，拒绝登记为热副本', 'REHEAT_VERIFY_FAILED', 502);
    }
    await ensureDir(this.datasetRoot(datasetId));
    const target = this.contentPath(datasetId, fileId, sha256);
    // 与回收互斥：若该内容路径刚被回收/降级，等删除结束再写入，避免
    // “文件刚 rename 到内容路径、目录也登记了，却立刻被孤儿清理删掉”。
    const release = await this.leases.acquireWhenOpen(target);
    try {
      if (await pathExists(target)) {
        const existing = await sha256File(target).catch(() => null);
        if (existing === sha256) {
          await safeRm(staged);
          return target;
        }
        // 同名内容路径上是不匹配的字节（磁盘损坏或人为篡改）：用刚校验过的
        // 取回内容原子替换。POSIX rename 保证正在读旧 inode 的读者不受影响。
        this.log?.(`content-addressed 路径上的旧内容校验失败，替换：${target}`);
      }
      await rename(staged, target);
      const handle = await open(target, 'r');
      try { await handle.sync(); } finally { await handle.close(); }
      return target;
    } finally {
      release();
    }
  }

  async discardStaging(taskId) {
    await safeRm(safeJoin(this.stagingRoot, taskId));
  }

  /**
   * 删除热文件。先关闭租约并排空读者；`keep` 在排空之后、unlink 之前再确认一次，
   * 返回 true 表示“这份文件现在仍被目录引用/不应删除”，于是跳过删除。
   * 这样孤儿清理不会误删等待期间刚被回热登记的内容。
   */
  async removeFile(target, { keep = null } = {}) {
    if (!target) return 0;
    return this.leases.runExclusive(target, async () => {
      if (keep && await keep()) return 0;
      let size = 0;
      try { size = (await stat(target)).size; } catch { /* 已不存在 */ }
      try {
        await unlink(target);
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
      }
      return size;
    });
  }

  async usageBytes({ fresh = false } = {}) {
    if (!fresh && Date.now() - this.usageCache.at < 120) return this.usageCache.bytes;
    let total = 0;
    async function walk(dir) {
      let entries;
      try { entries = await readdir(dir, { withFileTypes: true }); } catch { return; }
      for (const entry of entries) {
        if (entry.name === '.staging') continue;
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) await walk(full);
        else if (entry.isFile()) {
          try { total += (await stat(full)).size; } catch { /* 并发删除 */ }
        }
      }
    }
    await walk(this.root);
    this.usageCache = { at: Date.now(), bytes: total };
    return total;
  }

  async contentPathIfExists(datasetId, fileId, sha256) {
    const target = this.contentPath(datasetId, fileId, sha256);
    return (await pathExists(target)) ? target : null;
  }

  async listFiles(datasetId) {
    const root = this.datasetRoot(datasetId);
    const files = [];
    const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
    for (const entry of entries) {
      if (!entry.isFile()) continue;
      const full = path.join(root, entry.name);
      try {
        const info = await stat(full);
        files.push({ path: full, size: info.size, mtimeMs: info.mtimeMs });
      } catch { /* 并发删除 */ }
    }
    return files;
  }

  /** 删除目录中不再被目录库引用的文件（回收空间）。 */
}
