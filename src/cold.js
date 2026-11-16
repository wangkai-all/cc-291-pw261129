/**
 * 可控延迟的“慢速冷存储服务”。
 *
 * 模拟真实对象存储的关键行为：
 *  - 独立根目录，与本地热盘分离。
 *  - 每个分区版本有 manifest（文件名、大小、SHA-256、整分区校验和）。
 *  - 传输有基准延迟与带宽限制，支持中断（AbortSignal）。
 *  - 版本替换是“新增版本”，旧版本保留，查询可以固定读取某个版本。
 *  - 可注入一次性/持续性读故障与字节损坏，用来验证回热校验与回退。
 */
import path from 'node:path';
import { open, readdir, stat, writeFile, rename } from 'node:fs/promises';
import {
  ensureDir, makeError, nowIso, partitionChecksum, pathExists, randomId,
  readJson, safeJoin, safeRm, sha256File, sleep, validId,
} from './util.js';

const CHUNK = 256 * 1024;

export function abortError() {
  const error = new Error('取回已取消');
  error.code = 'ABORTED';
  error.statusCode = 499;
  return error;
}

export async function abortableSleep(ms, signal) {
  if (!ms || ms <= 0) return;
  if (signal?.aborted) throw abortError();
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, Math.min(ms, 2_147_483_647));
    const onAbort = () => {
      clearTimeout(timer);
      reject(abortError());
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

async function dirSize(dir) {
  let total = 0;
  async function walk(current) {
    let entries;
    try {
      entries = await readdir(current, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) await walk(full);
      else if (entry.isFile()) {
        try { total += (await stat(full)).size; } catch { /* 并发删除 */ }
      }
    }
  }
  await walk(dir);
  return total;
}

export class ColdService {
  constructor({ root, getSettings }) {
    this.root = root;
    this.getSettings = getSettings;
    this.faultCounters = new Map();
    this.usageCache = { at: 0, bytes: 0 };
  }

  async init() {
    await ensureDir(path.join(this.root, 'partitions'));
    await ensureDir(path.join(this.root, 'tmp'));
    const leftovers = await readdir(path.join(this.root, 'tmp')).catch(() => []);
    for (const entry of leftovers) await safeRm(path.join(this.root, 'tmp', entry));
  }

  settings() {
    return this.getSettings();
  }

  partitionRoot(partitionId) {
    if (!validId(partitionId)) throw makeError('非法分区 ID', 'INVALID_PARTITION_ID', 400);
    return safeJoin(this.root, 'partitions', partitionId);
  }

  versionRoot(partitionId, versionId) {
    if (!validId(versionId)) throw makeError('非法版本 ID', 'INVALID_VERSION_ID', 400);
    return safeJoin(this.partitionRoot(partitionId), 'versions', versionId);
  }

  objectPath(partitionId, versionId, fileId) {
    if (!validId(fileId)) throw makeError('非法文件 ID', 'INVALID_FILE_ID', 400);
    return safeJoin(this.versionRoot(partitionId, versionId), 'objects', `${fileId}.bin`);
  }

  async readManifest(partitionId, versionId) {
    const manifest = await readJson(path.join(this.versionRoot(partitionId, versionId), 'manifest.json'), null);
    if (!manifest) throw makeError('冷存储版本不存在', 'COLD_VERSION_NOT_FOUND', 404);
    return manifest;
  }

  async versionExists(partitionId, versionId) {
    return pathExists(path.join(this.versionRoot(partitionId, versionId), 'manifest.json'));
  }

  async listVersions(partitionId) {
    if (!(await pathExists(this.partitionRoot(partitionId)))) return [];
    let dirs;
    try {
      dirs = await readdir(path.join(this.partitionRoot(partitionId), 'versions'), { withFileTypes: true });
    } catch {
      return [];
    }
    const versions = [];
    for (const entry of dirs) {
      if (!entry.isDirectory()) continue;
      try {
        const manifest = await this.readManifest(partitionId, entry.name);
        const bytes = await dirSize(this.versionRoot(partitionId, entry.name));
        versions.push({
          partitionId,
          versionId: entry.name,
          createdAt: manifest.createdAt,
          checksum: manifest.checksum,
          temporary: Boolean(manifest.temporary),
          fileCount: manifest.files.length,
          totalBytes: manifest.totalBytes,
          bytes,
          files: manifest.files.map((file) => ({
            fileId: file.fileId, name: file.name, size: file.size, sha256: file.sha256,
          })),
        });
      } catch {
        // 构建中的版本没有 manifest，不对外暴露。
      }
    }
    versions.sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));
    return versions;
  }

  async #writeObjects({ files, destDir, signal, onProgress, totalBytes }) {
    const settings = this.settings();
    let copied = 0;
    for (const file of files) {
      if (signal?.aborted) throw abortError();
      const dest = path.join(destDir, `${file.fileId}.bin`);
      const out = await open(dest, 'w', 0o644);
      const input = await open(file.sourcePath, 'r');
      try {
        let position = 0;
        const buffer = Buffer.allocUnsafe(CHUNK);
        while (true) {
          if (signal?.aborted) throw abortError();
          const { bytesRead } = await input.read(buffer, 0, buffer.length, position);
          if (bytesRead <= 0) break;
          await abortableSleep((bytesRead / Math.max(1, settings.coldBandwidthBytesPerSec)) * 1000, signal);
          await out.write(buffer.subarray(0, bytesRead));
          position += bytesRead;
          copied += bytesRead;
          onProgress({ bytes: copied, totalBytes, fileId: file.fileId });
        }
        await out.sync();
      } finally {
        await input.close();
        await out.close();
      }
      const actual = await sha256File(dest);
      if (actual !== file.sha256) {
        throw makeError(`冷存储写入后校验不一致：${file.name}`, 'COLD_WRITE_VERIFY_FAILED', 502);
      }
      await abortableSleep(settings.coldLatencyBaseMs, signal);
    }
    return copied;
  }

  /**
   * 写入一个完整的新版本。先在 tmp 下构建并逐个校验，manifest 最后写，
   * 再 rename 成不可见版本——半成品永远不会被查询看到。
   */
  async putVersion({
    partitionId, versionId, files, signal, onProgress = () => {}, temporary = false,
  }) {
    if (!validId(partitionId) || !validId(versionId)) {
      throw makeError('非法分区/版本 ID', 'INVALID_ID', 400);
    }
    const target = this.versionRoot(partitionId, versionId);
    if (await pathExists(target)) throw makeError('冷存储版本已存在', 'COLD_VERSION_EXISTS', 409);
    await ensureDir(path.join(this.partitionRoot(partitionId), 'versions'));
    const staging = path.join(this.root, 'tmp', `put_${randomId('')}`);
    const objectsDir = path.join(staging, 'objects');
    await ensureDir(objectsDir);
    const totalBytes = files.reduce((sum, file) => sum + file.size, 0);
    try {
      await this.#writeObjects({ files, destDir: objectsDir, signal, onProgress, totalBytes });
      if (signal?.aborted) throw abortError();
      const manifest = {
        format: 1,
        partitionId,
        versionId,
        temporary,
        createdAt: nowIso(),
        checksum: partitionChecksum(files.map((file) => ({
          name: file.name, size: file.size, sha256: file.sha256,
        }))),
        totalBytes,
        files: files.map((file) => ({
          fileId: file.fileId,
          name: file.name,
          size: file.size,
          sha256: file.sha256,
        })),
      };
      await writeFile(path.join(staging, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);
      const handle = await open(path.join(staging, 'manifest.json'), 'r');
      try { await handle.sync(); } finally { await handle.close(); }
      await rename(staging, target);
      this.usageCache.at = 0;
      return manifest;
    } catch (error) {
      await safeRm(staging);
      throw error;
    }
  }

  /** 读取完成后是否应注入故障（一次性故障只触发一次）。 */
  #faultFor(kind, fileId) {
    const settings = this.settings();
    const mode = settings.coldFaultMode ?? 'none';
    const [faultKind, persistence] = mode.split('-');
    if (faultKind !== kind) return false;
    const target = settings.coldFaultFileId;
    if (target && target !== fileId) return false;
    const key = `${kind}:${fileId}`;
    if (persistence === 'once') {
      if (this.faultCounters.has(key)) return false;
      this.faultCounters.set(key, true);
      return true;
    }
    return persistence === 'always';
  }

  /**
   * 将冷对象流式读到 destPath。返回实际字节摘要、首次收到数据的时刻和注入的故障。
   * 调用方负责比对摘要；本方法不修改 catalog。
   */
  async retrieveFile({ partitionId, versionId, fileId, destPath, signal, onProgress = () => {} }) {
    const manifest = await this.readManifest(partitionId, versionId);
    const entry = manifest.files.find((file) => file.fileId === fileId);
    if (!entry) throw makeError('冷存储中没有该文件', 'COLD_FILE_NOT_FOUND', 404);
    const settings = this.settings();
    if (this.#faultFor('error', fileId)) {
      await abortableSleep(settings.coldLatencyBaseMs, signal);
      throw makeError('模拟慢存储网络中断', 'COLD_NETWORK_ERROR', 503);
    }
    const source = this.objectPath(partitionId, versionId, fileId);
    await ensureDir(path.dirname(destPath));
    await abortableSleep(settings.coldLatencyBaseMs, signal);
    const input = await open(source, 'r');
    const output = await open(destPath, 'w', 0o644);
    let read = 0;
    let firstByteAt = null;
    try {
      const buffer = Buffer.allocUnsafe(CHUNK);
      let position = 0;
      while (true) {
        if (signal?.aborted) throw abortError();
        const { bytesRead } = await input.read(buffer, 0, buffer.length, position);
        if (bytesRead <= 0) break;
        await abortableSleep((bytesRead / Math.max(1, settings.coldBandwidthBytesPerSec)) * 1000, signal);
        if (firstByteAt === null) firstByteAt = nowIso();
        await output.write(buffer.subarray(0, bytesRead));
        position += bytesRead;
        read += bytesRead;
        onProgress({ bytes: read, totalBytes: entry.size, fileId });
        await sleep(0);
      }
      await output.sync();
    } catch (error) {
      await safeRm(destPath);
      throw error;
    } finally {
      await input.close();
      await output.close();
    }
    const faults = [];
    if (this.#faultFor('corrupt', fileId)) {
      const handle = await open(destPath, 'r+');
      try {
        const offset = Math.max(0, Math.floor(entry.size / 2));
        const byte = Buffer.alloc(1);
        await handle.read(byte, 0, 1, offset);
        byte[0] ^= 0xff;
        await handle.write(byte, 0, 1, offset);
        await handle.sync();
      } finally {
        await handle.close();
      }
      faults.push('corrupt');
    }
    const sha256 = await sha256File(destPath);
    return {
      sha256,
      bytes: read,
      firstByteAt,
      faults,
      expectedSha256: entry.sha256,
      manifestChecksum: manifest.checksum,
    };
  }

  async deleteVersion(partitionId, versionId, { temporaryOnly = false } = {}) {
    const target = this.versionRoot(partitionId, versionId);
    if (temporaryOnly) {
      const manifest = await readJson(path.join(target, 'manifest.json'), null);
      if (!manifest?.temporary) return 0;
    }
    const size = await dirSize(target);
    await safeRm(target);
    this.usageCache.at = 0;
    return size;
  }

  async usageBytes() {
    const now = Date.now();
    if (now - this.usageCache.at < 500) return this.usageCache.bytes;
    const bytes = await dirSize(path.join(this.root, 'partitions'));
    this.usageCache = { at: now, bytes };
    return bytes;
  }

  async verifyObject(partitionId, versionId, fileId) {
    const manifest = await this.readManifest(partitionId, versionId);
    const entry = manifest.files.find((file) => file.fileId === fileId);
    if (!entry) return { ok: false, reason: 'manifest 中不存在该文件' };
    const actual = await sha256File(this.objectPath(partitionId, versionId, fileId));
    return {
      ok: actual === entry.sha256,
      expectedSha256: entry.sha256,
      actualSha256: actual,
      verifiedAt: nowIso(),
    };
  }
}

export { CHUNK };
