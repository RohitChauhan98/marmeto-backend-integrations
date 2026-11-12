import crypto from 'node:crypto';

/**
Shared builder for test/demonstration webhooks. Produces the exact serialized
body plus its base64 HMAC-SHA256 signature, guaranteeing the same bytes are
used for signing and transmission. Deterministic ids derive from `seed`.

Modes:
- valid:     one fresh order + webhook id
- duplicate: the SAME webhook id delivered twice for the same order
- dup-order: the same order id delivered under two different webhook ids
- invalid:   one delivery whose signature is computed over different bytes
*/
export function buildTestWebhook({ mode, seed, secret, orderId, webhookId }) {
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

  const orderNumberFrom = (orderId) => {
    const digits = String(orderId).replace(/\D/g, '');
    return digits ? Number(digits.slice(-12)) : 701157000 + Math.floor(Math.random() * 899999);
  };

  const sign = (bodyString) =>
    crypto.createHmac('sha256', secret).update(Buffer.from(bodyString, 'utf8')).digest('base64');

  let deliveries = [];
  let forcedBadSignature = false;

  if (mode === 'valid') {
    const oid = orderId ?? orderNumberFrom(stable(seed, 'order'));
    const wid = webhookId ?? stable(seed, 'webhook');
    deliveries = [{ orderId: oid, webhookId: wid }];
  } else if (mode === 'duplicate') {
    const oid = orderId ?? orderNumberFrom(stable(seed ?? 'dup-webhook', 'order'));
    const wid = webhookId ?? stable(seed ?? 'dup-webhook', 'webhook');
    deliveries = [
      { orderId: oid, webhookId: wid },
      { orderId: oid, webhookId: wid },
    ];
  } else if (mode === 'dup-order') {
    const oid = orderId ?? orderNumberFrom(stable(seed ?? 'dup-order', 'order'));
    deliveries = [
      { orderId: oid, webhookId: webhookId ?? stable(seed ?? 'dup-order', 'webhook') },
      { orderId: oid, webhookId: stable(`${seed ?? 'dup-order'}-second`, 'webhook') },
    ];
  } else if (mode === 'invalid') {
    const oid = orderId ?? orderNumberFrom(stable(seed ?? 'invalid', 'order'));
    deliveries = [{ orderId: oid, webhookId: webhookId ?? stable(seed ?? 'invalid', 'webhook') }];
    forcedBadSignature = true;
  } else {
    throw new Error(`unknown webhook mode: ${mode}`);
  }

  return deliveries.map(({ orderId: oid, webhookId: wid }) => {
    const payload = orderPayload(oid);
    const bodyString = JSON.stringify(payload);
    const signature = forcedBadSignature
      ? crypto.createHmac('sha256', secret).update('not the sent body').digest('base64')
      : sign(bodyString);
    return { payload, bodyString, signature, webhookId: wid, orderId: String(oid), invalidSignature: forcedBadSignature };
  });
}

/** Delivers pre-built webhooks to the endpoint; returns per-delivery results. */
export async function deliverTestWebhooks(url, deliveries) {
  const results = [];
  for (const delivery of deliveries) {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-shop-hmac-sha256': delivery.signature,
        'x-shop-webhook-id': delivery.webhookId,
      },
      body: delivery.bodyString,
    });
    let json = null;
    try {
      json = await res.json();
    } catch {
      json = null;
    }
    results.push({ status: res.status, body: json });
  }
  return results;
}
