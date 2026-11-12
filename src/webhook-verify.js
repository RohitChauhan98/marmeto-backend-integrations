import crypto from 'node:crypto';

export const SIGNATURE_HEADER = 'x-shop-hmac-sha256';
export const WEBHOOK_ID_HEADER = 'x-shop-webhook-id';

/**
Real Shopify sends `X-Shopify-Hmac-Sha256` / `X-Shopify-Webhook-Id`; the
assignment spec defines `X-Shop-Hmac-Sha256` / `X-Shop-Webhook-Id`. Both
spellings are accepted so the service works against an actual Shopify
development store and the spec's sender.
*/
export const SIGNATURE_HEADERS = ['x-shop-hmac-sha256', 'x-shopify-hmac-sha256'];
export const WEBHOOK_ID_HEADERS = ['x-shop-webhook-id', 'x-shopify-webhook-id'];

export function extractSignatureHeader(req) {
  for (const name of SIGNATURE_HEADERS) {
    const value = req.get(name);
    if (value) return value;
  }
  return undefined;
}

export function extractWebhookIdHeader(req) {
  for (const name of WEBHOOK_ID_HEADERS) {
    const value = req.get(name);
    if (value) return value;
  }
  return undefined;
}

/**
Verifies a Shopify-style base64 HMAC-SHA256 signature over the exact raw
request body. Returns true only when both digests exist, have the expected
32-byte length, and are equal using a constant-time comparison.
*/
export function verifyWebhookSignature(secret, rawBodyBuffer, signatureHeader) {
  if (typeof secret !== 'string' || secret.length === 0) return false;
  if (!Buffer.isBuffer(rawBodyBuffer) || rawBodyBuffer.length === 0) return false;
  if (typeof signatureHeader !== 'string' || signatureHeader.length === 0) {
    return false;
  }
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(signatureHeader)) return false;

  let providedDigest;
  try {
    providedDigest = Buffer.from(signatureHeader, 'base64');
  } catch {
    return false;
  }
  const expectedDigest = crypto.createHmac('sha256', secret).update(rawBodyBuffer).digest();
  if (providedDigest.length !== expectedDigest.length) return false;
  return crypto.timingSafeEqual(providedDigest, expectedDigest);
}

/** Extracts and validates the order fields the service needs. */
export function parseAndValidateOrderPayload(rawBodyBuffer) {
  let payload;
  try {
    payload = JSON.parse(rawBodyBuffer.toString('utf8'));
  } catch {
    return { ok: false, reason: 'invalid_json' };
  }
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    return { ok: false, reason: 'payload_not_object' };
  }
  const orderId = payload.id;
  const orderIdValid =
    typeof orderId === 'string' ||
    (typeof orderId === 'number' && Number.isInteger(orderId));
  if (!orderIdValid || String(orderId).length === 0) {
    return { ok: false, reason: 'missing_order_id' };
  }
  if (!payload.shipping_address || typeof payload.shipping_address !== 'object') {
    return { ok: false, reason: 'missing_shipping_address' };
  }
  if (!Array.isArray(payload.line_items) || payload.line_items.length === 0) {
    return { ok: false, reason: 'missing_line_items' };
  }
  return {
    ok: true,
    order: {
      id: String(payload.id),
      payload,
      shippingAddress: payload.shipping_address,
      lineItems: payload.line_items,
    },
  };
}
