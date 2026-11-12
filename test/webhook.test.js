import test from 'node:test';
import assert from 'node:assert/strict';
import {
  makeDb,
  makeTestConfig,
  startTestServer,
  signBody,
  makeOrderBody,
  postWebhook,
  ingest,
  closeQuietly,
} from './helpers.js';
import { ingestOrderWebhook } from '../src/server.js';

test('valid HMAC signature is accepted and persists order + pending job', async () => {
  const { db, config } = makeDb();
  const server = await startTestServer(db, config);
  try {
    const body = makeOrderBody(90001);
    const res = await postWebhook(server.url, {
      body,
      signature: signBody(config, body),
      webhookId: 'wh-valid-1',
    });
    assert.equal(res.status, 200);
    const json = await res.json();
    assert.equal(json.duplicate, false);
    assert.ok(json.jobId);

    assert.equal(db.prepare('SELECT COUNT(*) c FROM orders').get().c, 1);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 1);
    const job = db.prepare('SELECT * FROM jobs').get();
    assert.equal(job.state, 'pending');
    assert.equal(job.type, 'shipment');
    // No courier work happened before the acknowledgement: the job is only
    // queued, the shipment row is pending.
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, 'pending');
  } finally {
    await server.close();
    closeQuietly(db);
  }
});

test('invalid or missing signature returns 401 and creates no order or job', async () => {
  const { db, config } = makeDb();
  const server = await startTestServer(db, config);
  try {
    const body = makeOrderBody(90002);

    const wrongSecret = await postWebhook(server.url, {
      body,
      signature: signBody({ webhookSecret: 'other-secret' }, body),
      webhookId: 'wh-bad-1',
    });
    assert.equal(wrongSecret.status, 401);

    const tampered = await postWebhook(server.url, {
      body: `${body} `, // signature is over the exact original bytes only
      signature: signBody(config, body),
      webhookId: 'wh-bad-2',
    });
    assert.equal(tampered.status, 401);

    const missing = await fetch(`${server.url}/webhooks/orders-create`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-shop-webhook-id': 'wh-bad-3' },
      body,
    });
    assert.equal(missing.status, 401);

    const malformed = await postWebhook(server.url, {
      body,
      signature: '!!!not-base64!!!',
      webhookId: 'wh-bad-4',
    });
    assert.equal(malformed.status, 401);

    const truncated = await postWebhook(server.url, {
      body,
      signature: Buffer.from('short').toString('base64'), // wrong digest length
      webhookId: 'wh-bad-5',
    });
    assert.equal(truncated.status, 401);

    assert.equal(db.prepare('SELECT COUNT(*) c FROM orders').get().c, 0);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 0);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM webhook_deliveries').get().c, 0);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM shipments').get().c, 0);
  } finally {
    await server.close();
    closeQuietly(db);
  }
});

test('valid signature but invalid JSON or invalid payload gets 4xx and no rows', async () => {
  const { db, config } = makeDb();
  const server = await startTestServer(db, config);
  try {
    const invalidJson = 'this is not json';
    let res = await postWebhook(server.url, {
      body: invalidJson,
      signature: signBody(config, invalidJson),
      webhookId: 'wh-json-1',
    });
    assert.equal(res.status, 400);

    const missingId = JSON.stringify({
      shipping_address: { city: 'Pune' },
      line_items: [{ sku: 'X', quantity: 1 }],
    });
    res = await postWebhook(server.url, {
      body: missingId,
      signature: signBody(config, missingId),
      webhookId: 'wh-json-2',
    });
    assert.equal(res.status, 400);

    const missingItems = JSON.stringify({ id: 999, shipping_address: {} });
    res = await postWebhook(server.url, {
      body: missingItems,
      signature: signBody(config, missingItems),
      webhookId: 'wh-json-3',
    });
    assert.equal(res.status, 400);

    const missingHeader = JSON.stringify({ id: 998, shipping_address: {}, line_items: [] });
    res = await postWebhook(server.url, {
      body: missingHeader,
      signature: signBody(config, missingHeader),
    });
    assert.equal(res.status, 400);

    assert.equal(db.prepare('SELECT COUNT(*) c FROM orders').get().c, 0);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 0);
  } finally {
    await server.close();
    closeQuietly(db);
  }
});

test('real Shopify header names (X-Shopify-*) are accepted end to end', async () => {
  const { db, config } = makeDb();
  const server = await startTestServer(db, config);
  try {
    // Exactly what a Shopify development store sends on orders/create.
    const body = makeOrderBody(90007);
    const res = await fetch(`${server.url}/webhooks/orders-create`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-shopify-topic': 'orders/create',
        'x-shopify-hmac-sha256': signBody(config, body),
        'x-shopify-webhook-id': 'b3a1f2c4-d5e6-7890-abcd-ef0123456789',
        'x-shopify-shop-domain': 'test-shop.myshopify.com',
      },
      body,
    });
    assert.equal(res.status, 200);
    assert.equal((await res.json()).duplicate, false);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM orders').get().c, 1);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 1);

    // Shopify redelivers the same webhook id -> duplicate ack, no new work.
    const res2 = await fetch(`${server.url}/webhooks/orders-create`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-shopify-hmac-sha256': signBody(config, body),
        'x-shopify-webhook-id': 'b3a1f2c4-d5e6-7890-abcd-ef0123456789',
      },
      body,
    });
    assert.equal((await res2.json()).duplicate, true);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 1);
  } finally {
    await server.close();
    closeQuietly(db);
  }
});

test('duplicate webhook id creates no duplicate work', async () => {
  const { db, config } = makeDb();
  const server = await startTestServer(db, config);
  try {
    const body = makeOrderBody(90003);
    const sig = signBody(config, body);
    const first = await postWebhook(server.url, { body, signature: sig, webhookId: 'wh-dup-x' });
    assert.equal((await first.json()).duplicate, false);
    const second = await postWebhook(server.url, { body, signature: sig, webhookId: 'wh-dup-x' });
    assert.equal(second.status, 200);
    assert.equal((await second.json()).duplicate, true);

    assert.equal(db.prepare('SELECT COUNT(*) c FROM orders').get().c, 1);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 1);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM shipments').get().c, 1);
    const deliveries = db.prepare('SELECT * FROM webhook_deliveries').all();
    assert.equal(deliveries.length, 1);
    assert.equal(deliveries[0].status, 'accepted');
  } finally {
    await server.close();
    closeQuietly(db);
  }
});

test('same order id with a different webhook id creates no duplicate work', async () => {
  const { db, config } = makeDb();
  const server = await startTestServer(db, config);
  try {
    const body = makeOrderBody(90004);
    const body2 = makeOrderBody(90004, { created_at: '2020-01-01T00:00:00Z' });
    await postWebhook(server.url, {
      body,
      signature: signBody(config, body),
      webhookId: 'wh-order-a',
    });
    const res = await postWebhook(server.url, {
      body: body2,
      signature: signBody(config, body2),
      webhookId: 'wh-order-b',
    });
    assert.equal((await res.json()).duplicate, true);

    assert.equal(db.prepare('SELECT COUNT(*) c FROM orders').get().c, 1);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 1);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM webhook_deliveries').get().c, 1);
  } finally {
    await server.close();
    closeQuietly(db);
  }
});

test('concurrent webhook requests cannot create duplicate shipment workflows', async () => {
  const { db, config } = makeDb();
  const server = await startTestServer(db, config);
  try {
    // 6 concurrent HTTP deliveries for the same order: same webhook id for
    // some, different webhook ids for others.
    const bodies = [
      makeOrderBody(90005),
      makeOrderBody(90005),
      makeOrderBody(90005),
      makeOrderBody(90005, { created_at: '2024-01-01T00:00:00Z' }),
      makeOrderBody(90005, { created_at: '2024-01-02T00:00:00Z' }),
      makeOrderBody(90005, { created_at: '2024-01-03T00:00:00Z' }),
    ];
    const wid = ['wh-c-1', 'wh-c-1', 'wh-c-1', 'wh-c-2', 'wh-c-3', 'wh-c-4'];
    const responses = await Promise.all(
      bodies.map((body, i) =>
        postWebhook(server.url, { body, signature: signBody(config, body), webhookId: wid[i] })
      )
    );
    for (const res of responses) {
      assert.equal(res.status, 200);
    }
    const jsons = await Promise.all(responses.map((r) => r.json()));
    assert.equal(jsons.filter((j) => !j.duplicate).length, 1);

    assert.equal(db.prepare('SELECT COUNT(*) c FROM orders').get().c, 1);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 1);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM shipments').get().c, 1);
  } finally {
    await server.close();
    closeQuietly(db);
  }
});

test('concurrent direct ingests of the same order from distinct webhook ids create one job', async () => {
  const { db, config } = makeDb();
  try {
    const results = await Promise.all(
      Array.from({ length: 12 }, (_, i) => {
        // better-sqlite3 transactions are synchronous and serialized, so
        // this exercises constraint-level idempotency under a burst.
        return ingest(db, config, 90006, `wh-burst-${i}`);
      })
    );
    assert.equal(results.filter((r) => !r.duplicate).length, 1);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM orders').get().c, 1);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 1);
  } finally {
    closeQuietly(db);
  }
});
