import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)));

/**
Loads KEY=VALUE pairs from .env (when present) without overriding variables
that are already set in the environment.
*/
function loadDotEnvFile(env = process.env) {
  const file = path.join(ROOT, '.env');
  if (!fs.existsSync(file)) return;
  const text = fs.readFileSync(file, 'utf8');
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq <= 0) continue;
    const key = line.slice(0, eq).trim();
    let value = line.slice(eq + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (env[key] === undefined || env[key] === '') env[key] = value;
  }
}

let loaded = false;

export function loadTestEnv() {
  if (!loaded) {
    loadDotEnvFile();
    loaded = true;
  }
}

function intOrDefault(value, fallback) {
  const n = Number.parseInt(value ?? '', 10);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
}

/**
Central configuration. The base values come from environment variables
(after optional .env loading); tests can pass camelCase `overrides` such as
`{ courierMaxAttempts: 3, databasePath: '/tmp/x.db' }` which take absolute
precedence without mutating process.env.
*/
export function createConfig(overrides = {}) {
  loadTestEnv();
  const env = process.env;
  const courierPort = intOrDefault(
    overrides.mockCourierPort ?? env.MOCK_COURIER_PORT,
    4600
  );
  const config = {
    webhookServerPort: intOrDefault(
      overrides.webhookServerPort ?? env.WEBHOOK_SERVER_PORT,
      4550
    ),
    mockCourierPort: courierPort,
    webhookSecret: String(
      overrides.webhookSecret ?? env.SHOPIFY_WEBHOOK_SECRET ?? 'dev-secret-change-me'
    ),
    databasePath:
      overrides.databasePath ?? env.DATABASE_PATH ?? './data/app.db',
    mockCourierDataPath:
      overrides.mockCourierDataPath ??
      env.MOCK_COURIER_DATA_PATH ??
      './data/mock-courier-shipments.json',
    courierBaseUrl:
      overrides.courierBaseUrl ??
      env.COURIER_BASE_URL ??
      `http://localhost:${courierPort}`,
    courierTimeoutMs: intOrDefault(
      overrides.courierTimeoutMs ?? env.COURIER_TIMEOUT_MS,
      5000
    ),
    courierMaxAttempts: intOrDefault(
      overrides.courierMaxAttempts ?? env.COURIER_MAX_ATTEMPTS,
      5
    ),
    retryBaseDelayMs: intOrDefault(
      overrides.retryBaseDelayMs ?? env.COURIER_RETRY_BASE_DELAY_MS,
      500
    ),
    retryMaxDelayMs: intOrDefault(
      overrides.retryMaxDelayMs ?? env.COURIER_RETRY_MAX_DELAY_MS,
      30000
    ),
    retryJitterMs: intOrDefault(
      overrides.retryJitterMs ?? env.COURIER_RETRY_JITTER_MS,
      250
    ),
    workerPollIntervalMs: intOrDefault(
      overrides.workerPollIntervalMs ?? env.WORKER_POLL_INTERVAL_MS,
      1000
    ),
    workerLeaseMs: intOrDefault(
      overrides.workerLeaseMs ?? env.WORKER_LEASE_MS,
      60000
    ),
    uncertaintyDelayMs: intOrDefault(
      overrides.uncertaintyDelayMs ?? env.UNCERTAINTY_DELAY_MS,
      5000
    ),
  };
  return config;
}
