import test from 'node:test';
import assert from 'node:assert/strict';
import {
  makeDb,
  startStubCourier,
  createTestWorker,
  driveWorker,
  sleep,
  ingest,
  closeQuietly,
} from './helpers.js';
import { getJobByOrder } from '../src/queue.js';
import { SHIPMENT_STATES } from '../src/shipments.js';

test('timeout after the courier created the shipment recovers the existing shipment', async () => {
  const { db, config } = makeDb({
    courierTimeoutMs: 80, // client gives up quickly
    uncertaintyDelayMs: 40,
  });
  const courier = await startStubCourier();
  try {
    // Simulates the mock courier: persists the shipment, then delays the
    // response past the client's timeout.
    courier.handlers.onPost = async (req, res, body, state) => {
      const shipmentId = `SHP-${state.post}`;
      state.shipments.push({ ...body, shipmentId, createdAt: Date.now() });
      await sleep(300);
      res.writeHead(201, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ shipmentId, reference: body.reference }));
    };

    await ingest(db, config, 92001);
    const worker = createTestWorker(db, config, courier.url);

    const done = await driveWorker(
      worker,
      () => getJobByOrder(db, '92001').state === 'completed',
      { timeoutMs: 3000 }
    );
    assert.equal(done, true);

    // Exactly one create call was made to the courier.
    assert.equal(courier.state.post, 1);
    // The ambiguous first response triggered exactly one recovery lookup.
    assert.equal(courier.state.get, 1);
    // The existing shipment was adopted, no duplicate was created.
    assert.equal(courier.state.shipments.length, 1);
    const shipment = db.prepare('SELECT * FROM shipments').get();
    assert.equal(shipment.status, 'confirmed');
    assert.equal(shipment.courier_shipment_id, 'SHP-1');
    const job = getJobByOrder(db, '92001');
    assert.equal(job.state, 'completed');
    assert.match(shipment.detail || '', /recovered|confirmed/);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('a late success response from an earlier attempt cannot overwrite a later confirmation', async () => {
  const { db, config } = makeDb();
  const courier = await startStubCourier();
  try {
    await ingest(db, config, 92002);

    const lateResponsePromises = [];
    courier.handlers.onPost = (req, res, body, state) => {
      const shipmentId = `SHP-${state.post}`;
      state.shipments.push({ ...body, shipmentId, createdAt: Date.now() });
      const promise = new Promise((resolve) => {
        setTimeout(() => {
          res.writeHead(201, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ shipmentId, reference: body.reference }));
          resolve();
        }, 60);
      });
      lateResponsePromises.push(promise);
      return;
    };

    const worker = createTestWorker(db, config, courier.url);
    // Drive one attempt; the response is delayed, so the job stays
    // "processing" until the courier answers.
    const runPromise = worker.processNextJob();
    await sleep(10);
    // A concurrent actor (recovery worker) confirms the shipment with a
    // different id while the first attempt is still in flight.
    const { confirmShipment } = await import('../src/shipments.js');
    const confirmed = confirmShipment(db, '92002', 'SHP-concurrent-winner', 'recovered concurrently', Date.now());
    assert.equal(confirmed, 'applied');
    await runPromise;

    // First attempt's late response must not reset the confirmation.
    for (const p of lateResponsePromises) await p.catch(() => {});
    const shipment = db.prepare('SELECT * FROM shipments').get();
    assert.equal(shipment.status, 'confirmed');
    assert.equal(shipment.courier_shipment_id, 'SHP-concurrent-winner');
    assert.match(shipment.detail, /late result suppressed/);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});
