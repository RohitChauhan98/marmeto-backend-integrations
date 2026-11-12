import test from 'node:test';
import assert from 'node:assert/strict';
import {
  makeDb,
  startStubCourier,
  clientFor,
  createTestWorker,
  driveWorker,
  startTestServer,
  ingest,
  sleep,
  closeQuietly,
} from './helpers.js';
import { getJobByOrder } from '../src/queue.js';

test('dashboard API: overview and orders reflect ingested state', async () => {
  const { db, config } = makeDb({ courierBaseUrl: 'http://localhost:59998' });
  const server = await startTestServer(db, config);
  try {
    await ingest(db, config, 94001, 'wh-dash-1');
    const overview = await (await fetch(`${server.url}/admin/api/overview`)).json();
    assert.equal(overview.stats.orders, 1);
    assert.equal(overview.stats.jobs.pending, 1);
    assert.equal(overview.courier.reachable, false);

    const orders = await (await fetch(`${server.url}/admin/api/orders`)).json();
    assert.equal(orders.orders.length, 1);
    assert.equal(orders.orders[0].id, '94001');
    assert.equal(orders.orders[0].job_state, 'pending');
    assert.equal(orders.deliveries[0].webhook_id, 'wh-dash-1');

    // Dashboard page + assets served.
    const page = await fetch(`${server.url}/`);
    assert.equal(page.status, 200);
    assert.match(await page.text(), /Marmeto Shipment Gateway/);
    const js = await fetch(`${server.url}/app.js`);
    assert.equal(js.status, 200);
    const css = await fetch(`${server.url}/style.css`);
    assert.equal(css.status, 200);
  } finally {
    await server.close();
    closeQuietly(db);
  }
});

test('dashboard API: send-webhook endpoint drives the signed pipeline end to end', async () => {
  const { db, config } = makeDb({ courierBaseUrl: 'http://localhost:59998' });
  const server = await startTestServer(db, config);
  try {
    const res = await fetch(`${server.url}/admin/api/send-webhook`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'duplicate', seed: 'dash-dup' }),
    });
    const json = await res.json();
    assert.equal(json.results.length, 2);
    assert.equal(json.results[0].status, 200);
    assert.equal(json.results[0].body.duplicate, false);
    assert.equal(json.results[1].body.duplicate, true);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 1);

    const invalid = await fetch(`${server.url}/admin/api/send-webhook`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'invalid', seed: 'dash-bad' }),
    });
    const invalidJson = await invalid.json();
    assert.equal(invalidJson.results[0].status, 401);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM orders').get().c, 1);

    const bogus = await fetch(`${server.url}/admin/api/send-webhook`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'nonsense' }),
    });
    assert.equal(bogus.status, 400);
  } finally {
    await server.close();
    closeQuietly(db);
  }
});

test('dashboard API: reconcile and dead-letter replay actions work', async () => {
  const { db, config } = makeDb();
  const server = await startTestServer(db, config);
  const courier = await startStubCourier();
  try {
    // Force a dead-letter.
    courier.handlers.onPost = (req, res) => {
      res.writeHead(400);
      res.end();
    };
    config.courierBaseUrl = courier.url;
    await ingest(db, config, 94002);
    const worker = createTestWorker(db, config, courier.url);
    await driveWorker(worker, () => getJobByOrder(db, '94002').state === 'dead_letter');

    // Replay via the dashboard endpoint, using a happy courier.
    courier.handlers.onPost = (req, res, body) => {
      const shipmentId = `SHP-${courier.state.post}`;
      courier.state.shipments.push({ ...body, shipmentId, createdAt: Date.now() });
      res.writeHead(201, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ shipmentId, reference: body.reference }));
    };
    const jobId = getJobByOrder(db, '94002').id;
    const replayRes = await fetch(`${server.url}/admin/api/jobs/${jobId}/replay`, { method: 'POST' });
    assert.equal(replayRes.status, 200);

    await driveWorker(worker, () => getJobByOrder(db, '94002').state === 'completed');
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, 'confirmed');

    // Replaying a non-dead-letter job is refused with 409.
    const again = await fetch(`${server.url}/admin/api/jobs/${jobId}/replay`, { method: 'POST' });
    assert.equal(again.status, 409);

    // Reconcile action runs (idempotent; everything is confirmed already).
    const recon = await (await fetch(`${server.url}/admin/api/reconcile`, { method: 'POST' })).json();
    assert.equal(recon.ordersExamined, 0);
    assert.equal(recon.errorsEncountered, 0);

    // Unknown job id -> 404.
    const missing = await fetch(`${server.url}/admin/api/jobs/nope/replay`, { method: 'POST' });
    assert.equal(missing.status, 404);
  } finally {
    await server.close();
    await courier.close();
    closeQuietly(db);
  }
});

test('mock courier runtime control switches failure modes without restart', async () => {
  const { createMockCourierApp, MockCourierStore } = await import('../scripts/mock-courier.js');
  const store = new MockCourierStore(null);
  const app = createMockCourierApp({ store, controlEnabled: true, retryAfterMs: 1500, timeoutDelayMs: 50 });
  const server = app.listen(0);
  const port = server.address().port;
  const base = `http://localhost:${port}`;
  const post = async (path, body) =>
    fetch(`${base}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
  try {
    let res = await post('/_test/mode', { mode: 'error_500' });
    assert.equal(res.status, 200);
    res = await post('/shipments', { reference: 'o-c1', address: {}, items: [1] });
    assert.equal(res.status, 500);

    res = await post('/_test/mode', { mode: 'rate_limit_429' });
    res = await post('/shipments', { reference: 'o-c1', address: {}, items: [1] });
    assert.equal(res.status, 429);
    assert.equal(res.headers.get('retry-after'), '2');

    res = await post('/_test/mode', { mode: '' }); // back to probabilistic
    res = await post('/_test/mode', { mode: 'bogus' });
    assert.equal(res.status, 400);

    // control disabled: no runtime endpoints exist
    const lockedApp = createMockCourierApp({ store, controlEnabled: false });
    const lockedServer = lockedApp.listen(0);
    const lockedRes = await fetch(`http://localhost:${lockedServer.address().port}/_test/mode`, { method: 'POST' });
    assert.equal(lockedRes.status, 404);
    lockedServer.close();
  } finally {
    server.close();
  }
});
