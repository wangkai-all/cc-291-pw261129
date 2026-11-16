#!/usr/bin/env node
import path from 'node:path';
import { startServer } from '../src/server.js';

const port = Number(process.env.PORT ?? 4317);
const host = process.env.HOST ?? '127.0.0.1';
const dataRoot = process.env.DATA_DIR
  ? path.resolve(process.env.DATA_DIR)
  : path.resolve(process.cwd(), 'data');

const started = await startServer({ port, host, dataRoot });

const shutdown = async (signal) => {
  console.log(`\n[tier] 收到 ${signal}，正在停止...`);
  await started.close();
  process.exit(0);
};
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
