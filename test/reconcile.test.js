import test from 'node:test';
import assert from 'node:assert/strict';
import { reconcile } from '../src/reconcile.js';
import { replayDeadLetterJob, getJobByOrder, countJobs } from '../src/queue.js';
import {
  makeDb,
  startStubCourier,
  clientFor,
  createTestWorker,
  driveWorker,
  ingest,
  sleep,
  closeQuietly,
} from './helpers.js';
import { SHIPMENT_STATES } from '../src/shipments.js';

test('reconciliation recovers an existing shipment for an order affected by a timeout', async () => {
  const { db, config } = makeDb({ uncertaintyDelayMs: 40 });
  const courier = await startStubCourier();
  try {
    // First create attempt: courier persists, then delays past the timeout.
    courier.handlers.onPost = async (req, res, body, state) => {
      const shipmentId = `SHP-${state.post}`;
      state.shipments.push({ ...body, shipmentId, createdAt: Date.now() });
      await sleep(400);
      res.writeHead(201, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ shipmentId, reference: body.reference }));
    };
    await ingest(db, config, 93001);
    const worker = createTestWorker(db, config, courier.url);
    await worker.processNextJob(); // leaves the workflow uncertain
    await sleep(20);
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, 'uncertain');
    assert.equal(courier.state.post, 1);

    // No worker running: reconciliation must recover the existing shipment.
    const summary = await reconcile(db, clientFor(config, courier.url), config, {});
    assert.equal(summary.ordersExamined, 1);
    assert.equal(summary.shipmentsRecovered, 1);
    assert.equal(summary.errorsEncountered, 0);

    const shipment = db.prepare('SELECT * FROM shipments').get();
    assert.equal(shipment.status, 'confirmed');
    assert.equal(shipment.courier_shipment_id, 'SHP-1');
    // The recovered job was completed; no residual requeues.
    assert.equal(getJobByOrder(db, '93001').state, 'completed');
    // No new create happened.
    assert.equal(courier.state.post, 1);
    assert.equal(courier.state.shipments.length, 1);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('reconciliation schedules a safe retry only after a lookup proves none exists', async () => {
  const { db, config } = makeDb({ uncertaintyDelayMs: 40, courierMaxAttempts: 2 });
  const courier = await startStubCourier();
  try {
    courier.handlers.onPost = (req, res) => {
      res.writeHead(500);
      res.end();
    };
    await ingest(db, config, 93002);
    const worker = createTestWorker(db, config, courier.url);
    await driveWorker(worker, () => getJobByOrder(db, '93002').state === 'dead_letter');
    assert.equal(countJobs(db, 'dead_letter'), 1);
    const postsBefore = courier.state.post;

    // Reconcile: lookup finds nothing, so the existing job is requeued
    // (never a new job, never a fresh shipment row).
    const summary = await reconcile(db, clientFor(config, courier.url), config, {});
    assert.equal(summary.shipmentsRecovered, 0);
    assert.equal(summary.jobsRequeued, 1);
    const job = getJobByOrder(db, '93002');
    assert.equal(job.state, 'retrying');
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, 'lookup_empty');
    assert.ok(job.next_attempt_at > Date.now());

    // The worker can then create the shipment successfully.
    courier.handlers.onPost = (req, res, body) => {
      const shipmentId = `SHP-${courier.state.post}`;
      courier.state.shipments.push({ ...body, shipmentId, createdAt: Date.now() });
      res.writeHead(201, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ shipmentId, reference: body.reference }));
    };
    await driveWorker(worker, () => getJobByOrder(db, '93002').state === 'completed');
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, 'confirmed');
    assert.equal(courier.state.shipments.filter((s) => s.reference === '93002').length, 1);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('reconciliation preserves the uncertain state when the courier lookup fails', async () => {
  const { db, config } = makeDb();
  const courier = await startStubCourier();
  try {
    courier.handlers.onPost = (req, res) => {
      res.writeHead(400); // fatal -> dead-letter
      res.end();
    };
    await ingest(db, config, 93003);
    const worker = createTestWorker(db, config, courier.url);
    await driveWorker(worker, () => getJobByOrder(db, '93003').state === 'dead_letter');
    db.prepare("UPDATE shipments SET status = 'uncertain' WHERE order_id = '93003'").run();

    courier.handlers.onList = (req, res) => {
      res.writeHead(500);
      res.end();
    };
    const summary = await reconcile(db, clientFor(config, courier.url), config, {});
    assert.equal(summary.errorsEncountered, 1);
    assert.equal(summary.shipmentsRecovered, 0);
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, 'uncertain');
    // The dead-letter job was NOT blindly re-posted.
    assert.equal(getJobByOrder(db, '93003').state, 'dead_letter');
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('repeated reconciliation does not create duplicate jobs or shipments', async () => {
  const { db, config } = makeDb({ uncertaintyDelayMs: 30 });
  const courier = await startStubCourier();
  try {
    // An order that has nothing at the courier yet.
    await ingest(db, config, 93004);
    await sleep(5);
    const summary1 = await reconcile(db, clientFor(config, courier.url), config, {});
    assert.equal(summary1.ordersExamined, 1);
    const jobsAfterFirst = db.prepare('SELECT COUNT(*) c FROM jobs').get().c;
    const shipmentsAfterFirst = db.prepare('SELECT COUNT(*) c FROM shipments').get().c;
    const postsAfterFirst = courier.state.post;

    const summary2 = await reconcile(db, clientFor(config, courier.url), config, {});
    const summary3 = await reconcile(db, clientFor(config, courier.url), config, {});
    assert.equal(summary2.ordersExamined, summary3.ordersExamined);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, jobsAfterFirst);
    assert.equal(db.prepare('SELECT COUNT(*) c FROM shipments').get().c, shipmentsAfterFirst);
    assert.equal(courier.state.post, postsAfterFirst, 'reconciliation never POSTs');
    assert.equal(courier.state.shipments.length, 0);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('reconciliation skips orders with an actively leased workflow', async () => {
  const { db, config } = makeDb({ uncertaintyDelayMs: 30 });
  const courier = await startStubCourier();
  try {
    await ingest(db, config, 93005);
    // A worker claims the job with a long lease and "works" for a while.
    const { claimNextJob } = await import('../src/queue.js');
    const claimed = claimNextJob(db, {
      now: Date.now(),
      leaseMs: 60000,
      owner: 'active-worker',
    });
    assert.ok(claimed);
    courier.handlers.onList = (req, res, reference, state) => {
      // Should never be called for a leased order.
      state.leasedLookup = true;
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('[]');
    };
    const summary = await reconcile(db, clientFor(config, courier.url), config, {});
    assert.equal(summary.ordersExamined, 1);
    assert.equal(courier.state.get, 0);
    assert.match(summary.details.join('\n'), /worker lease active/);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('reconciliation completes without racing the worker (safe concurrent run)', async () => {
  const { db, config } = makeDb({ uncertaintyDelayMs: 30 });
  const courier = await startStubCourier();
  try {
    await ingest(db, config, 93006);
    const worker = createTestWorker(db, config, courier.url);
    // Run the worker's flow and reconciliation simultaneously.
    const results = await Promise.all([
      (async () => {
        await driveWorker(worker, () => countJobs(db, 'completed') >= 1, { timeoutMs: 4000 });
        return getJobByOrder(db, '93006').state;
      })(),
      reconcile(db, clientFor(config, courier.url), config, {}),
    ]);
    assert.equal(results[0], 'completed');
    const shipment = db.prepare('SELECT * FROM shipments').get();
    assert.equal(shipment.status, 'confirmed');
    assert.equal(courier.state.shipments.filter((s) => s.reference === '93006').length, 1);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});
