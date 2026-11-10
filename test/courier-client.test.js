import test from 'node:test';
import assert from 'node:assert/strict';
import {
  makeTestConfig,
  makeDb,
  closeQuietly,
  startStubCourier,
  clientFor,
  sleep,
} from './helpers.js';

test('courier client: 201 response is parsed as created/ok', async () => {
  const courier = await startStubCourier();
  try {
    const downloader = clientFor(makeTestConfig(), courier.url);
    const res = await downloader.createShipment('o-1', { city: 'Pune' }, [{ sku: 'A' }]);
    assert.equal(res.kind, 'ok');
    assert.match(res.data.shipmentId, /^SHP-/);
    assert.ok(courier.state.bodies[0].reference === 'o-1');
  } finally {
    await courier.close();
  }
});

test('courier client: 500 is retryable, 400 is fatal', async () => {
  const courier = await startStubCourier();
  try {
    courier.handlers.onPost = (req, res, body, state) => {
      res.writeHead(state.post % 2 === 1 ? 500 : 400);
      res.end(JSON.stringify({ error: 'nope' }));
    };
    const downloader = clientFor(makeTestConfig(), courier.url);
    const r1 = await downloader.createShipment('o-2', {}, []);
    assert.equal(r1.kind, 'retryable');
    assert.equal(r1.status, 500);
    const r2 = await downloader.createShipment('o-2', {}, []);
    assert.equal(r2.kind, 'fatal');
    assert.equal(r2.status, 400);
  } finally {
    await courier.close();
  }
});

test('courier client: 429 Retry-After parsing with safe fallbacks', async () => {
  const { db, config } = makeDb({ retryBaseDelayMs: 500 });
  const courier = await startStubCourier();
  try {
    let attempt = 0;
    courier.handlers.onPost = (req, res) => {
      attempt += 1;
      if (attempt === 1) {
        res.writeHead(429, { 'retry-after': '2' });
        res.end();
      } else if (attempt === 2) {
        res.writeHead(429, { 'retry-after': '0.75' });
        res.end();
      } else if (attempt === 3) {
        res.writeHead(429, { 'retry-after': 'not-a-number' });
        res.end();
      } else if (attempt === 4) {
        res.writeHead(429); // header missing
        res.end();
      } else {
        res.writeHead(429, { 'retry-after': '99999' }); // absurd => fallback
        res.end();
      }
    };
    const downloader = clientFor(config, courier.url);
    const specs = [
      { in: 'seconds 2', out: 2000 },
      { in: 'fractional 0.75', out: 750 },
      { kind: 'fallback1' },
      { kind: 'fallback2' },
      { kind: 'fallback3' },
    ];
    const out = await downloader.createShipment('o-3', {}, []);
    assert.equal(out.retryAfterMs, 2000);
    assert.equal(out.kind, 'retry_after');
    assert.equal((await downloader.createShipment('o-3', {}, [])).retryAfterMs, 750);
    // invalid values fall back to a bounded safe delay (>= base delay floor)
    const fallback1 = await downloader.createShipment('o-3', {}, []);
    assert.equal(fallback1.retryAfterMs, Math.max(500, 1000));
    const fallback2 = await downloader.createShipment('o-3', {}, []);
    assert.equal(fallback2.retryAfterMs, 1000);
    const fallback3 = await downloader.createShipment('o-3', {}, []);
    assert.equal(fallback3.retryAfterMs, 1000);
  } finally {
    await courier.close();
    closeQuietly(db);
  }
});

test('courier client: request timeout AFTER the server handled the request is uncertain', async () => {
  const courier = await startStubCourier();
  try {
    // Respond only after well beyond the client timeout; the request was
    // received, so the outcome can never be proven "not created".
    courier.handlers.onPost = async (req, res) => {
      await sleep(400);
      res.writeHead(201, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ shipmentId: 'SHP-late', reference: 'o-4' }));
    };
    const downloader = clientFor(makeTestConfig({ courierTimeoutMs: 80 }), courier.url);
    const res = await downloader.createShipment('o-4', {}, []);
    assert.equal(res.kind, 'uncertain');
    assert.ok(/timeout/.test(res.message));
  } finally {
    await courier.close();
  }
});

test('courier client: connection refused is retryable (request never started)', async () => {
  // No server on this port on purpose.
  const config = makeTestConfig();
  const downloader = clientFor(config, 'http://localhost:59999');
  const res = await downloader.createShipment('o-5', {}, []);
  assert.equal(res.kind, 'retryable');
  assert.match(res.message, /ECONNREFUSED|before request reached courier/);
});

test('courier client: lookup list method returns parsed array', async () => {
  const courier = await startStubCourier();
  try {
    courier.state.shipments.push({ shipmentId: 'SHP-x', reference: 'o-6', createdAt: 1 });
    const downloader2 = clientFor(makeTestConfig(), courier.url);
    const res = await downloader2.listShipments('o-6');
    assert.equal(res.kind, 'ok');
    assert.equal(res.data.length, 1);
    const empty = await downloader2.listShipments('o-unknown');
    assert.equal(empty.data.length, 0);
  } finally {
    await courier.close();
  }
});
