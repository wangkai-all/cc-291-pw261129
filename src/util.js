import { createHash, randomBytes } from 'node:crypto';
import { createReadStream } from 'node:fs';
import {
  mkdir, open, readFile, rename, rm, stat, writeFile,
} from 'node:fs/promises';
import path from 'node:path';

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export function sha256Hex(data) {
  return createHash('sha256').update(data).digest('hex');
}

export async function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = createHash('sha256');
    createReadStream(filePath)
      .on('error', reject)
      .on('data', (chunk) => hash.update(chunk))
      .on('end', () => resolve(hash.digest('hex')));
  });
}

export function randomId(prefix = '') {
  return `${prefix}${randomBytes(8).toString('hex')}`;
}

export function nowIso() {
  return new Date().toISOString();
}

export async function ensureDir(dir) {
  await mkdir(dir, { recursive: true });
}

export async function pathExists(target) {
  try {
    await stat(target);
    return true;
  } catch (error) {
    if (error.code === 'ENOENT') return false;
    throw error;
  }
}

export async function fsyncDir(dir) {
  let handle;
  try {
    handle = await open(dir, 'r');
    await handle.sync();
  } catch (error) {
    // Some platforms/filesystems do not support fsync on a directory. The
    // durability protocol still uses rename(), which is atomic on POSIX.
    if (!['EISDIR', 'EINVAL', 'EPERM', 'EBADF'].includes(error.code)) throw error;
  } finally {
    await handle?.close();
  }
}

export async function atomicWriteFile(filePath, data, { mode = 0o644 } = {}) {
  await ensureDir(path.dirname(filePath));
  const tmp = `${filePath}.tmp-${process.pid}-${randomBytes(4).toString('hex')}`;
  const handle = await open(tmp, 'w', mode);
  try {
    await handle.writeFile(data);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(tmp, filePath);
  await fsyncDir(path.dirname(filePath));
}

export async function atomicWriteJson(filePath, value) {
  await atomicWriteFile(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

export async function readJson(filePath, fallback = null) {
  try {
    return JSON.parse(await readFile(filePath, 'utf8'));
  } catch (error) {
    if (error.code === 'ENOENT') return fallback;
    throw error;
  }
}

export async function safeRm(target) {
  await rm(target, { recursive: true, force: true });
}

export function partitionChecksum(files) {
  const canonical = [...files]
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((file) => `${file.name}\0${file.size}\0${file.sha256}`)
    .join('\n');
  return sha256Hex(canonical);
}

export function validId(value, max = 80) {
  return typeof value === 'string'
    && value.length > 0
    && value.length <= max
    && /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value);
}

export function safeJoin(root, ...segments) {
  const resolvedRoot = path.resolve(root);
  const target = path.resolve(resolvedRoot, ...segments);
  if (target !== resolvedRoot && !target.startsWith(`${resolvedRoot}${path.sep}`)) {
    const error = new Error('路径越界');
    error.statusCode = 400;
    error.code = 'PATH_TRAVERSAL';
    throw error;
  }
  return target;
}

export function makeError(message, code = 'ERROR', statusCode = 500) {
  const error = new Error(message);
  error.code = code;
  error.statusCode = statusCode;
  return error;
}

export function bytesFromMb(mb) {
  return Math.round(mb * 1024 * 1024);
}
