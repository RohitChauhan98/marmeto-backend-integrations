#!/usr/bin/env node
import { createConfig } from './config.js';
import { connectDb, closeDb } from './db.js';
import { createCourierClient } from './courier.js';
import { createWorker } from './worker.js';

const config = createConfig();
const db = connectDb(config.databasePath);
const courier = createCourierClient(config);
const worker = createWorker(db, config, courier, { nowFn: () => Date.now() });

console.log(
  `[worker] polling every ${config.workerPollIntervalMs}ms (courier=${config.courierBaseUrl})`
);
const controller = new AbortController();
const loopPromise = worker.runLoop(controller.signal);

const shutdown = async () => {
  controller.abort();
  await Promise.race([loopPromise, new Promise((r) => setTimeout(r, 1500))]);
  closeDb(db);
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
