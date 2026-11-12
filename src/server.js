import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { withTransaction, isoNow, newId } from './db.js';
import {
  extractSignatureHeader,
  extractWebhookIdHeader,
  verifyWebhookSignature,
  parseAndValidateOrderPayload,
} from './webhook-verify.js';
import { buildTestWebhook, deliverTestWebhooks } from './webhook-sender.js';
import { createCourierClient } from './courier.js';
import { reconcile } from './reconcile.js';
import { replayDeadLetterJob } from './queue.js';

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)));

/**
Processes a verified order webhook inside a single immediate transaction:

1. `INSERT OR IGNORE` the webhook delivery id. If it collides, the webhook
   was delivered before and is acknowledged as a duplicate without any new
   work (no order row changes, no new shipment job).
2. `INSERT OR IGNORE` the order. The primary key on orders(id) enforces the
   "same order delivered under a different webhook id" case at the database
   level; a collision means another delivery for this order already
   scheduled the coordinated shipment workflow.
3. Fresh order: persist order + pending shipment + exactly one queue job
   (unique index on jobs(order_id, type)) in the same transaction.

Database constraints, not application queries, are the source of truth for
idempotency, and the transaction is synchronous, so concurrent requests
cannot interleave and create duplicate jobs.
*/
export function ingestOrderWebhook(db, config, order, webhookId, now = isoNow()) {
  return withTransaction(db, () => {
    // The order row is persisted first (FK for the delivery row), with
    // INSERT OR IGNORE so a collision on orders.id proves the same order
    // arrived under a different webhook id and its workflow already exists.
    const orderInfo = db
      .prepare(
        `INSERT OR IGNORE INTO orders
           (id, payload, shipping_address, line_items, created_at)
         VALUES (?, ?, ?, ?, ?)`
      )
      .run(
        order.id,
        JSON.stringify(order.payload),
        JSON.stringify(order.shippingAddress),
        JSON.stringify(order.lineItems),
        now
      );
    if (orderInfo.changes === 0) {
      return { duplicate: true, reason: 'order_id_already_seen', jobId: null };
    }

    // A conflicting INSERT means this exact webhook id was delivered before;
    // acknowledge it without scheduling any new shipment work.
    const deliveryInfo = db
      .prepare(
        `INSERT INTO webhook_deliveries (webhook_id, order_id, status, received_at)
         VALUES (?, ?, 'accepted', ?)
         ON CONFLICT(webhook_id) DO NOTHING`
      )
      .run(webhookId, order.id, now);
    if (deliveryInfo.changes === 0) {
      return { duplicate: true, reason: 'webhook_id_already_seen', jobId: null };
    }

    db.prepare(
      `INSERT INTO shipments
         (order_id, status, courier_shipment_id, detail, confirmed_at,
          created_at, updated_at)
       VALUES (?, 'pending', NULL, '', NULL, ?, ?)`
    ).run(order.id, now, now);

    const jobId = newId();
    db.prepare(
      `INSERT INTO jobs
         (id, type, order_id, state, attempts, max_attempts,
          next_attempt_at, created_at, updated_at)
       VALUES (?, 'shipment', ?, 'pending', 0, ?, ?, ?, ?)`
    ).run(jobId, order.id, config.courierMaxAttempts, now, now, now);

    return { duplicate: false, reason: 'accepted', jobId };
  });
}

export function buildServer({ config, db }) {
  const app = express();
  const ingest = (order, webhookId) =>
    ingestOrderWebhook(db, config, order, webhookId);

  app.get('/health', (req, res) => res.json({ ok: true }));

  // Raw body is required so the HMAC is computed over the exact bytes
  // Shopify sent, not on a re-serialized object.
  const rawBodyParser = express.raw({ type: '*/*', limit: '1mb' });

  app.post('/webhooks/orders-create', rawBodyParser, async (req, res) => {
    const signature = extractSignatureHeader(req);
    const valid = verifyWebhookSignature(config.webhookSecret, req.body, signature);
    if (!valid) {
      return res.status(401).json({ error: 'invalid or missing HMAC signature' });
    }

    const webhookId = extractWebhookIdHeader(req);
    if (typeof webhookId !== 'string' || webhookId.length === 0 || webhookId.length > 512) {
      return res.status(400).json({ error: 'missing or invalid webhook id header (X-Shop-Webhook-Id / X-Shopify-Webhook-Id)' });
    }

    const parsed = parseAndValidateOrderPayload(req.body);
    if (!parsed.ok) {
      return res.status(400).json({ error: `invalid order payload: ${parsed.reason}` });
    }

    const result = ingest(parsed.order, webhookId);
    return res.status(200).json({
      status: 'ok',
      duplicate: result.duplicate,
      jobId: result.jobId,
    });
  });

  mountAdminRoutes(app, { config, db, ingest });

  return app;
}

/**
Local operations dashboard: static frontend plus a small read/action API for
demonstrating and operating the system. Intended for local development —
the endpoints are unauthenticated by design and the server should never be
exposed publicly with them enabled (see README limitations).
*/
export function mountAdminRoutes(app, { config, db, ingest }) {
  const publicDir = path.join(ROOT, 'public');
  app.use(express.static(publicDir, { index: false }));
  app.use('/admin', express.json({ type: 'application/json' }));

  const courierClient = () =>
    createCourierClient({ ...config, courierTimeoutMs: Math.min(config.courierTimeoutMs, 3000) });

  app.get('/', (req, res) => res.sendFile(path.join(publicDir, 'index.html')));

  app.get('/admin/api/overview', async (req, res) => {
    const jobs = {};
    for (const row of db.prepare('SELECT state, COUNT(*) c FROM jobs GROUP BY state').all()) {
      jobs[row.state] = row.c;
    }
    const shipments = {};
    for (const row of db.prepare('SELECT status, COUNT(*) c FROM shipments GROUP BY status').all()) {
      shipments[row.status] = row.c;
    }
    let courier = { reachable: false };
    try {
      const response = await fetch(`${config.courierBaseUrl.replace(/\/+$/, '')}/health`, {
        signal: AbortSignal.timeout(1500),
      });
      courier = { reachable: response.ok };
    } catch {
      courier = { reachable: false };
    }
    res.json({
      courier,
      courierBaseUrl: config.courierBaseUrl,
      courierControlEnabled: true,
      stats: {
        orders: db.prepare('SELECT COUNT(*) c FROM orders').get().c,
        deliveries: db.prepare('SELECT COUNT(*) c FROM webhook_deliveries').get().c,
        jobs,
        shipments,
      },
    });
  });

  app.get('/admin/api/orders', (req, res) => {
    const limit = Math.min(Number.parseInt(req.query.limit ?? '50', 10) || 50, 200);
    const orders = db
      .prepare(
        `SELECT o.id, o.created_at,
                s.status AS shipment_status, s.courier_shipment_id,
                s.detail AS shipment_detail, s.confirmed_at,
                j.state AS job_state, j.attempts, j.max_attempts,
                j.next_attempt_at, j.last_error
         FROM orders o
         LEFT JOIN shipments s ON s.order_id = o.id
         LEFT JOIN jobs j ON j.order_id = o.id AND j.type = 'shipment'
         ORDER BY o.created_at DESC LIMIT ?`
      )
      .all(limit);
    const deliveries = db
      .prepare(
        `SELECT d.webhook_id, d.order_id, d.status, d.received_at
         FROM webhook_deliveries d
         ORDER BY d.received_at DESC LIMIT ?`
      )
      .all(limit);
    res.json({ orders, deliveries });
  });

  app.get('/admin/api/dead-letters', (req, res) => {
    const jobs = db
      .prepare(
        `SELECT * FROM jobs WHERE state = 'dead_letter' ORDER BY updated_at DESC LIMIT 50`
      )
      .all();
    res.json({ jobs });
  });

  app.post('/admin/api/send-webhook', async (req, res) => {
    const { mode, seed, orderId, webhookId } = req.body || {};
    if (!['valid', 'duplicate', 'dup-order', 'invalid'].includes(mode)) {
      return res.status(400).json({ error: 'mode must be valid | duplicate | dup-order | invalid' });
    }
    // Send through this very server instance (full signed pipeline), using
    // the port it is actually bound to right now.
    const selfUrl = `http://localhost:${req.socket.localPort}/webhooks/orders-create`;
    const deliveries = buildTestWebhook({ mode, seed, secret: config.webhookSecret, orderId, webhookId });
    const results = await deliverTestWebhooks(selfUrl, deliveries);
    return res.json({ mode, results });
  });

  app.post('/admin/api/reconcile', async (req, res) => {
    try {
      const summary = await reconcile(db, courierClient(), config, {});
      return res.json(summary);
    } catch (error) {
      return res.status(500).json({ error: error.message });
    }
  });

  app.post('/admin/api/jobs/:jobId/replay', (req, res) => {
    const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(req.params.jobId);
    if (!job) return res.status(404).json({ error: 'job not found' });
    if (job.state !== 'dead_letter') {
      return res.status(409).json({ error: `job is ${job.state}; only dead_letter jobs can be replayed` });
    }
    const replayed = replayDeadLetterJob(db, job.id, Date.now());
    if (!replayed) {
      return res.status(409).json({ error: 'job changed state concurrently; refused' });
    }
    return res.json({ replayed: true, jobId: job.id, orderId: job.order_id });
  });

  app.post('/admin/api/courier/mode', async (req, res) => {
    const { mode } = req.body || {};
    const allowed = ['', 'success', 'error_500', 'rate_limit_429', 'timeout_wait'];
    if (!allowed.includes(mode)) {
      return res.status(400).json({ error: `mode must be one of: ${allowed.join(' | ')}` });
    }
    try {
      const response = await fetch(`${config.courierBaseUrl.replace(/\/+$/, '')}/_test/mode`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ mode }),
        signal: AbortSignal.timeout(1500),
      });
      const json = await response.json();
      return res.status(response.status).json(json);
    } catch (error) {
      return res.status(502).json({ error: `courier unreachable: ${error.message}` });
    }
  });
}

export function startServer({ config, db, app }) {
  const server = app.listen(config.webhookServerPort, () => {
    console.log(`[webhook-server] listening on :${config.webhookServerPort}`);
  });
  return server;
}
