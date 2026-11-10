import test from 'node:test';
import assert from 'node:assert/strict';
import {
  makeDb,
  makeTestConfig,
  startStubCourier,
  clientFor,
  createTestWorker,
  driveWorker,
  sleep,
  ingest,
  closeQuietly,
} from './helpers.js';
import { claimNextJob, countJobs, replayDeadLetterJob, listDeadLetterJobs, getJobByOrder } from '../src/queue.js';
import { SHIPMENT_STATES } from '../src/shipments.js';

test('HTTP 500 causes exponential backoff retries, persisting attempts, then dead_letter', async () => {
  const { db, config } = makeDb();
  const courier = await startStubCourier();
  try {
    courier.handlers.onPost = (req, res) => {
      res.writeHead(500);
      res.end(JSON.stringify({ error: 'boom' }));
    };
    await ingest(db, config, 91001);
    const worker = createTestWorker(db, config, courier.url);

    let jobRow = getJobByOrder(db, '91001');
    await driveWorker(worker, () => {
      jobRow = getJobByOrder(db, '91001');
      return jobRow.state === 'dead_letter';
    });
    jobRow = getJobByOrder(db, '91001');

    assert.equal(jobRow.state, 'dead_letter');
    assert.equal(jobRow.attempts, 3);
    assert.equal(jobRow.max_attempts, 3);
    const history = JSON.parse(jobRow.error_history);
    assert.equal(history.length, 3);
    assert.ok(/courier responded 500/.test(jobRow.last_error));
    assert.equal(courier.state.post, 3);
    // The courier never created anything (definitive 500s), so the shipment
    // is in a safe retryable-creation state, never stuck in ambiguity.
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, SHIPMENT_STATES.LOOKUP_EMPTY);
    assert.match(
      db.prepare('SELECT detail FROM shipments').get().detail,
      /nothing was created/
    );
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('HTTP 500 scheduling persists next_attempt_at with exponential spacing', async () => {
  const { db, config } = makeDb();
  const courier = await startStubCourier();
  try {
    courier.handlers.onPost = (req, res) => {
      res.writeHead(500);
      res.end();
    };
    await ingest(db, config, 91002);
    const worker = createTestWorker(db, config, courier.url);
    await worker.processNextJob();
    const job = getJobByOrder(db, '91002');
    assert.equal(job.state, 'retrying');
    assert.ok(job.next_attempt_at > Date.now(), 'retry scheduled in the future');
    assert.ok(job.last_error.includes('500'));
    assert.equal(JSON.parse(job.error_history).length, 1);
    // backoff for first retry == base delay (jitter disabled)
    assert.ok(job.next_attempt_at - Date.now() <= config.retryBaseDelayMs + config.retryBaseDelayMs);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('HTTP 429 respects Retry-After and never retries earlier than requested', async () => {
  const { db, config } = makeDb({ courierMaxAttempts: 5 });
  const courier = await startStubCourier();
  try {
    let posts = 0;
    courier.handlers.onPost = async (req, res) => {
      posts += 1;
      if (posts === 1) {
        res.writeHead(429, { 'retry-after': '2' });
        res.end();
      } else {
        const shipmentId = `SHP-after-429-${posts}`;
        courier.state.shipments.push({ shipmentId, reference: '91003', createdAt: Date.now() });
        res.writeHead(201, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ shipmentId, reference: '91003' }));
      }
    };
    await ingest(db, config, 91003);
    const worker = createTestWorker(db, config, courier.url);
    const before = Date.now();
    await worker.processNextJob();
    const job = getJobByOrder(db, '91003');
    assert.equal(job.state, 'retrying');
    assert.ok(
      job.next_attempt_at - before >= 2000,
      `Retry-After honored: next attempt is >= 2s in the future (delta=${job.next_attempt_at - before})`
    );
    // Worker does not sleep through it: the job is in the DB, pilot stays async.
    assert.equal(countJobs(db, 'retrying'), 1);
    assert.equal(countJobs(db, 'dead_letter'), 0);

    // Travel to the scheduled attempt (simulated clock) and complete.
    db.prepare('UPDATE jobs SET next_attempt_at = ? WHERE id = ?').run(Date.now(), job.id);
    await driveWorker(
      worker,
      () => getJobByOrder(db, '91003').state === 'completed'
    );
    assert.equal(getJobByOrder(db, '91003').state, 'completed');
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, 'confirmed');
    assert.equal(posts, 2);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('dead-letter replay is safe: same workflow row, full budget restored', async () => {
  const { db, config } = makeDb();
  const courier = await startStubCourier();
  try {
    let failing = true;
    courier.handlers.onPost = (req, res, body) => {
      if (failing) {
        res.writeHead(500);
        res.end();
      } else {
        const shipmentId = `SHP-${courier.state.post}`;
        courier.state.shipments.push({ ...body, shipmentId, createdAt: Date.now() });
        res.writeHead(201, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ shipmentId, reference: body.reference }));
      }
    };
    await ingest(db, config, 91004);
    const worker = createTestWorker(db, config, courier.url);
    await driveWorker(worker, () => getJobByOrder(db, '91004').state === 'dead_letter');
    const deadJob = getJobByOrder(db, '91004');
    assert.equal(deadJob.state, 'dead_letter');
    assert.equal(deadJob.attempts, 3);

    // Replay through the same workflow row.
    const replayed = replayDeadLetterJob(db, deadJob.id, Date.now());
    assert.equal(replayed, true);
    const rolling = getJobByOrder(db, '91004');
    assert.equal(rolling.state, 'retrying');
    assert.equal(rolling.attempts, 0);

    failing = false;
    await driveWorker(worker, () => getJobByOrder(db, '91004').state === 'completed');
    assert.equal(getJobByOrder(db, '91004').state, 'completed');
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, 'confirmed');
    // Exactly 3 failing POSTs + exactly 1 successful POST (no extra work).
    assert.equal(courier.state.post, 4);

    // The courier holds exactly one shipment for this reference.
    assert.equal(courier.state.shipments.filter((s) => s.reference === '91004').length, 1);

    // Replaying again is a no-op (no longer dead-lettered).
    const secondAttempt = replayDeadLetterJob(db, deadJob.id, Date.now());
    assert.equal(secondAttempt, false);
    assert.equal(courier.state.post, 4);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('replay on an already-confirmed shipment does not create another shipment', async () => {
  const { db, config } = makeDb();
  const courier = await startStubCourier();
  try {
    await ingest(db, config, 91005);
    const worker = createTestWorker(db, config, courier.url);
    await driveWorker(worker, () => getJobByOrder(db, '91005').state === 'completed');
    const job = getJobByOrder(db, '91005');
    assert.equal(job.state, 'completed');
    const postsAfterConfirm = courier.state.post;

    // Simulate an operator replay against any state by forcing dead_letter.
    db.prepare("UPDATE jobs SET state = 'dead_letter' WHERE id = ?").run(job.id);
    assert.equal(replayDeadLetterJob(db, job.id, Date.now()), true);
    await driveWorker(worker, () => getJobByOrder(db, '91005').state === 'completed');
    assert.equal(getJobByOrder(db, '91005').state, 'completed');
    // No additional POST: the worker saw the confirmed shipment and finished.
    assert.equal(courier.state.post, postsAfterConfirm);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('worker crash after claim (expired lease) recovers the job and completes exactly once', async () => {
  const { db, config } = makeDb({ workerLeaseMs: 60 });
  const courier = await startStubCourier();
  try {
    await ingest(db, config, 91006);
    // Simulate a worker that claimed the job then crashed before work.
    const stolen = claimNextJob(db, { now: Date.now(), leaseMs: 60, owner: 'ghost-worker' });
    assert.ok(stolen);
    assert.equal(stolen.state, 'processing');
    await sleep(90); // lease expires

    let concurrentDupes = 0;
    const worker = createTestWorker(db, config, courier.url);
    // Two workers race to claim; only one may win the recovered job.
    const [r1, r2] = await Promise.all([worker.processNextJob(), worker.processNextJob()]);
    concurrentDupes = (r1.claimed ? 1 : 0) + (r2.claimed ? 1 : 0);
    assert.equal(concurrentDupes, 1, 'only one worker claims the expired-lease job');

    await driveWorker(worker, () => {
      const job = getJobByOrder(db, '91006');
      return job.state === 'completed';
    });
    const job = getJobByOrder(db, '91006');
    assert.equal(job.state, 'completed');
    assert.ok(job.attempts >= 2, `attempts reflect the reclaimed run (attempts=${job.attempts})`);
    assert.equal(courier.state.post, 1);
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, 'confirmed');
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('worker crash mid-POST (ambiguous recovered by lookup, not a second POST)', async () => {
  const { db, config } = makeDb({ workerLeaseMs: 60 });
  const courier = await startStubCourier();
  try {
    await ingest(db, config, 91007);
    // Simulate: worker claimed, marked shipment processing, and died right
    // before (or during) the POST. Shipment status is 'processing'.
    const stolen = claimNextJob(db, { now: Date.now(), leaseMs: 60, owner: 'ghost' });
    assert.ok(stolen);
    db.prepare("UPDATE shipments SET status = 'processing' WHERE order_id = '91007'").run();
    await sleep(90); // lease expires; the POST outcome is unknown

    const worker = createTestWorker(db, config, courier.url);
    await driveWorker(worker, () => getJobByOrder(db, '91007').state === 'completed');
    const job = getJobByOrder(db, '91007');
    assert.equal(job.state, 'completed');
    // Recovery used a lookup first, then created the shipment exactly once.
    assert.ok(courier.state.get >= 1, 'recovery lookup happened');
    assert.equal(courier.state.post, 1);
    assert.equal(db.prepare('SELECT status FROM shipments').get().status, 'confirmed');
    assert.ok(/^SHP-1$/.test(db.prepare('SELECT courier_shipment_id FROM shipments').get().courier_shipment_id));
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});
