import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

/** Lazy ESM import so this module stays testable without an open handle. */
const Database = (await import('better-sqlite3')).default;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS orders (
  id TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  shipping_address TEXT NOT NULL,
  line_items TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
  webhook_id TEXT PRIMARY KEY,
  order_id TEXT NOT NULL REFERENCES orders(id),
  status TEXT NOT NULL,
  received_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_order
  ON webhook_deliveries(order_id);

CREATE TABLE IF NOT EXISTS shipments (
  order_id TEXT PRIMARY KEY REFERENCES orders(id),
  status TEXT NOT NULL,
  courier_shipment_id TEXT,
  detail TEXT,
  confirmed_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL DEFAULT 'shipment',
  order_id TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  next_attempt_at INTEGER NOT NULL,
  lease_owner TEXT,
  lease_expires_at INTEGER,
  last_error TEXT,
  error_history TEXT NOT NULL DEFAULT '[]',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_state_next
  ON jobs(state, next_attempt_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_order_workflow
  ON jobs(order_id, type);
`;

export function connectDb(databasePath) {
  if (databasePath !== ':memory:') {
    fs.mkdirSync(path.dirname(path.resolve(databasePath)), { recursive: true });
  }
  const db = new Database(databasePath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  db.pragma('busy_timeout = 5000');
  ensureSchema(db);
  return db;
}

export function ensureSchema(db) {
  db.exec(SCHEMA);
}

/**
Runs `fn` inside a BEGIN IMMEDIATE transaction. better-sqlite3 keeps this
fully synchronous, so concurrent requests in one process cannot interleave
with the transaction body.
*/
export function withTransaction(db, fn) {
  if (!db.__marmeto_tx_stack) db.__marmeto_tx_stack = 0;
  if (db.__marmeto_tx_stack > 0) {
    return fn();
  }
  db.__marmeto_tx_stack += 1;
  db.exec('BEGIN IMMEDIATE');
  try {
    const result = fn();
    db.exec('COMMIT');
    return result;
  } catch (error) {
    try {
      db.exec('ROLLBACK');
    } catch {
      /* transaction may already be committed/aborted */
    }
    throw error;
  } finally {
    db.__marmeto_tx_stack -= 1;
  }
}

export function isoNow() {
  return Date.now();
}

export function newId() {
  return crypto.randomUUID();
}

export function closeDb(db) {
  try {
    db.close();
  } catch {
    /* already closed */
  }
}
