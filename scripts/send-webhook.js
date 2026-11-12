#!/usr/bin/env node
import { createConfig } from '../src/config.js';
import { buildTestWebhook, deliverTestWebhooks } from '../src/webhook-sender.js';

/**
Sends Shopify-style, HMAC-signed orders/create webhooks to the local
endpoint via the shared sender module (same serialized bytes for signature
and transmission; secret from the environment).

modes:
  valid           one webhook with a fresh order + webhook id
  duplicate       the SAME webhook id delivered twice for the same order
  dup-order       the same order id delivered under two different webhook ids
  invalid         a well-formed webhook with intentionally wrong signature
*/
function usage() {
  console.log(`usage: npm run send-webhook -- <valid|duplicate|dup-order|invalid> [options]
  --url <url>            webhook endpoint (default http://localhost:<PORT>/webhooks/orders-create)
  --seed <string>        deterministic id source
  --order-id <id>        explicit order id
  --webhook-id <id>      explicit webhook id`);
}

function argValue(name, fallback) {
  const i = process.argv.indexOf(name);
  return i >= 0 && i + 1 < process.argv.length && process.argv[i + 1] !== undefined
    ? process.argv[i + 1]
    : fallback;
}

const config = createConfig();
const mode = process.argv[2] || 'valid';
if (!['valid', 'duplicate', 'dup-order', 'invalid'].includes(mode)) {
  usage();
  process.exit(1);
}
const url = argValue(
  '--url',
  `http://localhost:${config.webhookServerPort}/webhooks/orders-create`
);

const deliveries = buildTestWebhook({
  mode,
  seed: argValue('--seed', null),
  secret: config.webhookSecret,
  orderId: argValue('--order-id', null),
  webhookId: argValue('--webhook-id', null),
});

let exitCode = 0;
const results = await deliverTestWebhooks(url, deliveries);
deliveries.forEach((delivery, i) => {
  const result = results[i];
  console.log(
    `POST ${url} (delivery ${i + 1}/${deliveries.length}, webhook id ${delivery.webhookId})`
  );
  console.log(`  status: ${result.status}`);
  console.log(`  response: ${JSON.stringify(result.body)}`);
  if (delivery.invalidSignature && result.status !== 401) exitCode = 1;
  if (!delivery.invalidSignature && result.status !== 200) exitCode = 1;
});
process.exit(exitCode);
