import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import express from 'express';

export class MockCourierStore {
  constructor(filePath) {
    this.filePath = filePath || null;
    this.data = { shipments: [] };
    if (this.filePath && fs.existsSync(this.filePath)) {
      try {
        this.data = JSON.parse(fs.readFileSync(this.filePath, 'utf8'));
      } catch {
        this.data = { shipments: [] };
      }
    }
  }

  persist() {
    if (!this.filePath) return;
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
    fs.writeFileSync(this.filePath, JSON.stringify(this.data, null, 2));
  }

  create(reference, address, items) {
    const shipment = {
      shipmentId: `SHP-${crypto.randomUUID()}`,
      reference,
      address,
      items,
      createdAt: Date.now(),
    };
    this.data.shipments.push(shipment);
    this.persist();
    return shipment;
  }

  /** Latest-first list is not required; return in creation order. */
  findByReference(reference) {
    return this.data.shipments
      .filter((s) => s.reference === reference)
      .sort((a, b) => a.createdAt - b.createdAt);
  }
}

const MODES = new Set(['success', 'error_500', 'rate_limit_429', 'timeout_wait']);

function pickMode(options) {
  if (options.headers && MODES.has(options.headers['x-test-fail-mode'])) {
    return { mode: options.headers['x-test-fail-mode'], deterministic: true };
  }
  if (options.globalMode && MODES.has(options.globalMode)) {
    return { mode: options.globalMode, deterministic: true };
  }
  const r = Math.random();
  if (r < 0.2) return { mode: 'error_500', deterministic: false };
  if (r < 0.3) return { mode: 'rate_limit_429', deterministic: false };
  if (r < 0.4) return { mode: 'timeout_wait', deterministic: false };
  return { mode: 'success', deterministic: false };
}

function parseIntWithDefault(value, fallback) {
  const n = /\d+(?:\.\d+)?/.exec(String(value || ''));
  if (!n) return fallback;
  const parsed = Number(n[0]);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

export function createMockCourierApp({ store, globalMode, retryAfterMs, timeoutDelayMs, controlEnabled } = {}) {
  // Runtime failure-mode control (used by the dashboard in dev): when
  // enabled, POST /_test/mode switches the global deterministic mode.
  const control = { globalMode: globalMode || '' };
  const app = express();
  app.use(express.json({ type: '*/*' }));

  if (controlEnabled) {
    app.post('/_test/mode', (req, res) => {
      const { mode } = req.body || {};
      if (mode !== '' && !MODES.has(mode)) {
        return res.status(400).json({ error: `mode must be one of: ${[...MODES, ''].join(' | ')}` });
      }
      control.globalMode = mode || '';
      return res.status(200).json({ mode: control.globalMode || 'probabilistic' });
    });
    app.get('/_test/mode', (req, res) => {
      return res.status(200).json({ mode: control.globalMode || 'probabilistic' });
    });
  }

  app.post('/shipments', (req, res) => {
    const { reference, address, items } = req.body || {};
    if (!reference || typeof reference !== 'string') {
      return res.status(400).json({ error: 'reference (string) is required' });
    }
    if (!address || typeof address !== 'object') {
      return res.status(400).json({ error: 'address (object) is required' });
    }
    if (!Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: 'items (non-empty array) is required' });
    }
    const chosen = pickMode({
      headers: req.headers,
      globalMode: controlEnabled ? control.globalMode : globalMode,
    });
    if (chosen.deterministic) {
      res.set('X-Test-Mode', chosen.mode);
    }
    switch (chosen.mode) {
      case 'error_500':
        return res.status(500).json({ error: 'internal courier error' });
      case 'rate_limit_429': {
        const seconds = Math.max(Math.ceil(parseIntWithDefault(retryAfterMs, 2000) / 1000), 1);
        res.set('Retry-After', String(seconds));
        return res.status(429).json({ error: 'rate limited', retryAfterSeconds: seconds });
      }
      case 'timeout_wait': {
        const shipment = store.create(reference, address, items);
        const delay = parseIntWithDefault(timeoutDelayMs, 10000);
        const timer = setTimeout(() => {
          res.status(201).json({ shipmentId: shipment.shipmentId, reference });
        }, delay);
        if (typeof timer.unref === 'function') timer.unref();
        return undefined;
      }
      default: {
        const shipment = store.create(reference, address, items);
        return res.status(201).json({ shipmentId: shipment.shipmentId, reference });
      }
    }
  });

  app.get('/shipments', (req, res) => {
    const reference = req.query.reference;
    if (!reference || typeof reference !== 'string') {
      return res.status(400).json({ error: 'reference query parameter is required' });
    }
    return res.status(200).json(store.findByReference(reference));
  });

  app.get('/health', (req, res) => res.status(200).json({ ok: true }));
  return app;
}

export function main() {
  const port = parseIntWithDefault(process.env.MOCK_COURIER_PORT, 4600);
  const store = new MockCourierStore(process.env.MOCK_COURIER_DATA_PATH || './data/mock-courier-shipments.json');
  const app = createMockCourierApp({
    store,
    globalMode: process.env.MOCK_COURIER_MODE || '',
    retryAfterMs: process.env.MOCK_COURIER_RETRY_AFTER_MS,
    timeoutDelayMs: process.env.MOCK_COURIER_TIMEOUT_DELAY_MS,
    controlEnabled: process.env.MOCK_COURIER_CONTROL === '1',
  });
  const server = app.listen(port, () => {
    if (!process.env.MOCK_COURIER_QUIET) {
      console.log(`[mock-courier] listening on :${port} data=${store.filePath || '(in-memory)'} mode=${process.env.MOCK_COURIER_MODE || 'probabilistic'}`);
    }
  });
  const shutdown = () => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1500).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

// Only run the standalone server when executed directly.
const isMain = process.argv[1] && (process.argv[1].endsWith('mock-courier.js') || process.argv[1].endsWith('mock-courier'));
if (isMain) main();
