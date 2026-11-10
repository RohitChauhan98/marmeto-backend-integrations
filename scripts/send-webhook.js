#!/usr/bin/env node
import crypto from 'node:crypto';
import { createConfig } from '../src/config.js';

/**
Sends Shopify-style, HMAC-signed orders/create webhooks to the local
endpoint. The exact same serialized body string is used for both the
signature and the transmission. The secret is read from the environment.

modes:
  valid           one webhook with a fresh order + webhook id
  duplicate       the SAME webhook id delivered twice for the same order
  dup-order       the same order id delivered under two different webhook ids
  invalid         a well-formed webhook with intentionally wrong signature

All senders accept --seed for deterministic, repeatable IDs, and
--order-id/--webhook-id overrides.
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
const url = argValue(
  '--url',
  `http://localhost:${config.webhookServerPort}/webhooks/orders-create`
);
const seed = argValue('--seed', null);
const explicitOrderId = argValue('--order-id', null);
const explicitWebhookId = argValue('--webhook-id', null);

const stable = (seedString, prefix) => {
  if (!seedString) return `${prefix}-${crypto.randomUUID()}`;
  const digest = crypto
    .createHash('sha256')
    .update(`${prefix}:${seedString}`)
    .digest('hex');
  return `${prefix}-${digest.slice(0, 10)}`;
};

const orderPayload = (orderId) => ({
  id: orderId,
  email: 'customer@example.com',
  shipping_address: {
    first_name: 'Rohit',
    last_name: 'Test',
    address1: 'Balewadi High Street',
    city: 'Pune',
    province: 'Maharashtra',
    zip: '411057',
    country: 'IN',
  },
  line_items: [
    { sku: 'MARM-TSHIRT-BLK-M', title: 'Marmeto Tee', quantity: 1, price: '599.00' },
    { sku: 'MARM-CAP-KHK', title: 'Marmeto Cap', quantity: 2, price: '299.00' },
  ],
  created_at: new Date().toISOString(),
});

function orderNumberFrom(orderId) {
  const digits = String(orderId).replace(/\D/g, '');
  return digits ? Number(digits.slice(-12)) : 701157000 + Math.floor(Math.random() * 899999);
}

let sendCount = 1;
let webhookIdEntries = [];
let orderIdEntries = [];
let forcedBadSignature = false;

if (mode === 'valid') {
  const oid = explicitOrderId ?? orderNumberFrom(stable(seed, 'order'));
  webhookIdEntries = [explicitWebhookId ?? stable(seed, 'webhook')];
  orderIdEntries = [oid];
} else if (mode === 'duplicate') {
  // Same order id + same webhook id, delivered twice.
  const oid = explicitOrderId ?? orderNumberFrom(stable(seed ?? 'dup-webhook', 'order'));
  const wid = explicitWebhookId ?? stable(seed ?? 'dup-webhook', 'webhook');
  sendCount = 2;
  webhookIdEntries = [wid, wid];
  orderIdEntries = [oid, oid];
} else if (mode === 'dup-order') {
  // Same order id under two different webhook ids.
  const oid = explicitOrderId ?? orderNumberFrom(stable(seed ?? 'dup-order', 'order'));
  sendCount = 2;
  webhookIdEntries = [
    explicitWebhookId ?? stable(seed ?? 'dup-order', 'webhook'),
    stable(`${seed ?? 'dup-order'}-second`, 'webhook'),
  ];
  orderIdEntries = [oid, oid];
} else if (mode === 'invalid') {
  const oid = explicitOrderId ?? orderNumberFrom(stable(seed ?? 'invalid', 'order'));
  webhookIdEntries = [explicitWebhookId ?? stable(seed ?? 'invalid', 'webhook')];
  orderIdEntries = [oid];
  forcedBadSignature = true;
} else {
  usage();
  process.exit(1);
}

let exitCode = 0;
for (let i = 0; i < sendCount; i += 1) {
  const payload = orderPayload(orderIdEntries[i]);
  const bodyString = JSON.stringify(payload);
  const signature = forcedBadSignature
    ? crypto.createHmac('sha256', config.webhookSecret).update('not the sent body').digest('base64')
    : crypto
        .createHmac('sha256', config.webhookSecret)
        .update(Buffer.from(bodyString, 'utf8'))
        .digest('base64');

  console.log(
    `POST ${url} (delivery ${i + 1}/${sendCount}, webhook id ${webhookIdEntries[i]})`
  );
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-shop-hmac-sha256': signature,
      'x-shop-webhook-id': webhookIdEntries[i],
    },
    body: bodyString,
  });
  const text = await res.text();
  console.log(`  status: ${res.status}`);
  console.log(`  response: ${text}`);

  if (forcedBadSignature && res.status !== 401) exitCode = 1;
  if (!forcedBadSignature && res.status !== 200) exitCode = 1;
}
process.exit(exitCode);
