/**
 * 生成可复现、可读的示例数据。内容由 seed 决定，便于演示“版本替换”后校验依据变化。
 */
import { sha256Hex } from './util.js';

function xorshift(seed) {
  let state = 0x9e3779b9 ^ seed;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
}

function hashSeed(text) {
  const hex = sha256Hex(`tier-seed:${text}`);
  return Number.parseInt(hex.slice(0, 8), 16);
}

export function generateFiles({
  seed, count = 4, sizeBytes = 256 * 1024, prefix = 'part',
}) {
  const files = [];
  for (let index = 0; index < count; index += 1) {
    const random = xorshift(hashSeed(`${seed}:${index}`));
    const name = `${prefix}-${String(index + 1).padStart(2, '0')}.log`;
    const lines = [];
    lines.push(`# dataset=${seed} file=${name} generated=${new Date(0).toISOString()}`);
    lines.push('# columns: seq,tenant,latency_ms,status,payload');
    let seq = 0;
    let size = lines.join('\n').length + 1;
    while (size < sizeBytes) {
      const tenant = `t${(random() % 4000).toString(16).padStart(3, '0')}`;
      const latency = random() % 900;
      const status = random() % 17 === 0 ? 503 : 200;
      const payload = random().toString(16).padStart(8, '0');
      const line = `${seq},${tenant},${latency},${status},${payload}`;
      lines.push(line);
      size += line.length + 1;
      seq += 1;
    }
    const content = Buffer.from(`${lines.join('\n')}\n`);
    files.push({
      name,
      content,
      size: content.length,
      sha256: sha256Hex(content),
      seed: `${seed}:${index}`,
    });
  }
  return files;
}
