import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { createConfig } from '../src/config.js';
import { connectDb, closeDb } from '../src/db.js';
import { buildServer, ingestOrderWebhook } from '../src/server.js';
import { createCourierClient } from '../src/courier.js';
import { createWorker } from '../src/worker.js';

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Fast, jitter-free config so retry scheduling is deterministic. */
export function makeTestConfig(overrides = {}) {
  return createConfig({
    courierTimeoutMs: 120,
    courierMaxAttempts: 3,
    retryBaseDelayMs: 20,
    retryMaxDelayMs: 400,
    retryJitterMs: 0,
    workerPollIntervalMs: 10,
    workerLeaseMs: 250,
    uncertaintyDelayMs: 50,
    ...overrides,
  });
}

export function makeDb(overrides = {}) {
  const config = makeTestConfig(overrides);
  const db = connectDb(':memory:');
  return { db, config };
}

/** Starts the Express app on an ephemeral port. */
export function startTestServer(db, config) {
  const app = buildServer({ config, db });
  const server = app.listen(0);
  return new Promise((resolve) => {
    server.on('listening', () => {
      const port = server.address().port;
      resolve({
        server,
        port,
        url: `http://localhost:${port}`,
        close: () => new Promise((done) => server.close(done)),
      });
    });
  });
}

/** Serializes + signs a payload like a real Shopify sender would. */
export function signBody(config, bodyString) {
  return crypto
    .createHmac('sha256', config.webhookSecret)
    .update(bodyString)
    .digest('base64');
}

export function makeOrderBody(orderId, extra = {}) {
  return JSON.stringify({
    id: orderId,
    email: 'customer@example.com',
    shipping_address: { city: 'Pune', address1: 'High Street' },
    line_items: [{ sku: 'SKU1', quantity: 1 }],
    ...extra,
  });
}

export async function postWebhook(serverUrl, { body, signature, webhookId }) {
  const headers = { 'content-type': 'application/json' };
  if (webhookId !== undefined) headers['x-shop-webhook-id'] = webhookId;
  if (signature !== undefined) headers['x-shop-hmac-sha256'] = signature;
  return fetch(`${serverUrl}/webhooks/orders-create`, {
    method: 'POST',
    headers,
    body,
  });
}

/** Convenience direct ingest (bypasses HTTP). */
export function ingest(db, config, orderId, webhookId = 'wh-test') {
  const order = {
    id: String(orderId),
    payload: {
      id: orderId,
      shipping_address: { city: 'Pune' },
      line_items: [{ sku: 'SKU1', quantity: 1 }],
    },
    shippingAddress: { city: 'Pune' },
    lineItems: [{ sku: 'SKU1', quantity: 1 }],
  };
  return ingestOrderWebhook(db, config, order, webhookId);
}

/**
Deterministic scriptable courier stub. POST/GET handlers are swappable per
test; defaults create/return recorded shipments like the mock courier.

  courier.state             counters, recorded bodies and shipments
  courier.handlers.onPost   (req, res, body, state) => custom response
  courier.handlers.onList   (req, res, reference, state) => custom response
*/
export function startStubCourier() {
  const state = { post: 0, get: 0, bodies: [], shipments: [] };
  const handlers = { onPost: null, onList: null };
  const server = http.createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const url = new URL(req.url, 'http://stub');
    if (req.method === 'POST' && url.pathname === '/shipments') {
      state.post += 1;
      const body = JSON.parse(Buffer.concat(chunks).toString() || '{}');
      state.bodies.push(body);
      if (handlers.onPost) return handlers.onPost(req, res, body, state);
      const shipmentId = `SHP-${state.post}`;
      state.shipments.push({ ...body, shipmentId, createdAt: Date.now() });
      res.writeHead(201, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ shipmentId, reference: body.reference }));
    }
    if (req.method === 'GET' && url.pathname === '/shipments') {
      state.get += 1;
      const reference = url.searchParams.get('reference');
      if (handlers.onList) return handlers.onList(req, res, reference, state);
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(
        JSON.stringify(state.shipments.filter((s) => s.reference === reference))
      );
    }
    res.writeHead(404);
    res.end();
  });
  return new Promise((resolve) => {
    server.listen(0, () => {
      const port = server.address().port;
      resolve({
        server,
        port,
        url: `http://localhost:${port}`,
        state,
        handlers,
        close: () => new Promise((done) => server.close(done)),
        client: null,
      });
    });
  });
}

/** A courier client pointed at the stub. */
export function clientFor(config, courierUrl) {
  return createCourierClient({ ...config, courierBaseUrl: courierUrl });
}

export function createTestWorker(db, config, courierUrl) {
  return createWorker(db, config, clientFor(config, courierUrl), {});
}

/** Drives the worker until `untilFn()` passes or the timeout expires. */
export async function driveWorker(worker, untilFn, { tick = 8, timeoutMs = 4000 } = {}) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    await worker.processNextJob();
    if (untilFn()) return true;
    await sleep(tick);
  }
  return untilFn();
}

export function closeQuietly(db) {
  if (db) closeDb(db);
}

export function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'marmeto-test-'));
}
