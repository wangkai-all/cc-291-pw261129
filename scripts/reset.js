#!/usr/bin/env node
import path from 'node:path';
import { rm } from 'node:fs/promises';

const dataRoot = process.env.DATA_DIR
  ? path.resolve(process.env.DATA_DIR)
  : path.resolve(process.cwd(), 'data');
await rm(dataRoot, { recursive: true, force: true });
console.log(`已删除 ${dataRoot}`);
