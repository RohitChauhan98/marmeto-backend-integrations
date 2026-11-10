import express from 'express';
import { withTransaction, isoNow, newId } from './db.js';
import {
  SIGNATURE_HEADER,
  WEBHOOK_ID_HEADER,
  verifyWebhookSignature,
  parseAndValidateOrderPayload,
} from './webhook-verify.js';

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
    const signature = req.get(SIGNATURE_HEADER);
    const valid = verifyWebhookSignature(config.webhookSecret, req.body, signature);
    if (!valid) {
      return res.status(401).json({ error: 'invalid or missing HMAC signature' });
    }

    const webhookId = req.get(WEBHOOK_ID_HEADER);
    if (typeof webhookId !== 'string' || webhookId.length === 0 || webhookId.length > 512) {
      return res.status(400).json({ error: 'missing or invalid X-Shop-Webhook-Id header' });
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

  return app;
}

export function startServer({ config, db, app }) {
  const server = app.listen(config.webhookServerPort, () => {
    console.log(`[webhook-server] listening on :${config.webhookServerPort}`);
  });
  return server;
}
