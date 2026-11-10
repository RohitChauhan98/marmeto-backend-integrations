#!/usr/bin/env node
import { createConfig } from './config.js';
import { connectDb, closeDb } from './db.js';
import { buildServer, startServer } from './server.js';

const config = createConfig();
const db = connectDb(config.databasePath);
const app = buildServer({ config, db });
const server = startServer({ config, db, app });

const shutdown = () => {
  server.close(() => {
    closeDb(db);
    process.exit(0);
  });
  // If connections keep the server open (keep-alive), force exit shortly.
  setTimeout(() => {
    closeDb(db);
    process.exit(0);
  }, 2000).unref();
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
