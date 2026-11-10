/**
Courier API client. Every method performs exactly one HTTP request with an
AbortController-bounded timeout and returns a classified result instead of
throwing. Retries, backoff, and rate-limit waits are scheduled by the
persistent queue, not by spinning inside this client, so network work NEVER
happens while holding a database transaction or worker lease window.

Result kinds:
- { kind: 'created', data }                      2xx response parsed
- { kind: 'ok', data }                           2xx for GET lookups
- { kind: 'retry_after', retryAfterMs, status }  429 with parsed Retry-After
- { kind: 'retryable', status, message }         5xx, request never started
- { kind: 'uncertain', message }                 timeout / broken mid-flight
- { kind: 'fatal', status, message }             other 4xx

Network error classification matters for correctness: a timeout or a
connection reset after the request was sent means the courier MIGHT have
created the shipment, so the outcome is 'uncertain'. Errors that guarantee
the request never reached the courier (ECONNREFUSED, ENOTFOUND) are plain
'retryable'.
*/

const GUARANTEED_NOT_STARTED_CODES = new Set([
  'ECONNREFUSED',
  'ECONNREFUSED2',
  'ENOTFOUND',
  'EAI_AGAIN',
  'ENETUNREACH',
  'EHOSTUNREACH',
  'EACCES',
  'EADDRNOTAVAIL',
  'ECONNABORTED',
]);

function parseRetryAfterHeader(value, fallbackMs) {
  if (value !== undefined && value !== null && value !== '') {
    const seconds = Number(value.trim());
    if (Number.isFinite(seconds) && seconds >= 0 && seconds <= 3600) {
      return Math.round(seconds * 1000);
    }
    const asDate = Date.parse(value);
    if (!Number.isNaN(asDate)) {
      const delta = asDate - Date.now();
      if (delta > -1000 && delta <= 3600_000) return Math.max(delta, 0);
    }
  }
  // Safe fallback when the header is missing, malformed, or absurd.
  return fallbackMs;
}

export function createCourierClient(config) {
  const base = String(config.courierBaseUrl || '').replace(/\/+$/, '');

  function request(method, path, { body, timeoutMs } = {}) {
    const controller = new AbortController();
    const effectiveTimeout = timeoutMs ?? config.courierTimeoutMs;
    const timer = setTimeout(
      () => controller.abort(new DOMException('courier request timeout', 'TimeoutError')),
      effectiveTimeout
    );
    const startedAt = Date.now();
    let timedOut = false;
    const fetchPromise = fetch(`${base}${path}`, {
      method,
      headers: {
        'content-type': 'application/json',
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    });

    return (async () => {
      let response;
      try {
        response = await fetchPromise;
      } catch (error) {
        const code = error?.cause?.code || error?.code || '';
        const timedOutNow =
          timedOut ||
          error?.name === 'AbortError' ||
          error?.name === 'TimeoutError';
        clearTimeout(timer);
        if (timedOutNow) {
          // The request may have reached the courier; the outcome is unknown.
          return {
            kind: 'uncertain',
            message: `timeout after ${Date.now() - startedAt}ms`,
            timedOut: true,
          };
        }
        if (code && GUARANTEED_NOT_STARTED_CODES.has(code)) {
          return {
            kind: 'retryable',
            status: code,
            message: `network error before request reached courier: ${code}`,
          };
        }
        // Conservative default: treat unknown network failures as uncertain.
        return {
          kind: 'uncertain',
          message: `network error: ${code || error?.name || 'unknown'} ${error?.message || ''}`,
        };
      }
      clearTimeout(timer);

      if (response.status === 429) {
        const retryAfterMs = parseRetryAfterHeader(
          response.headers.get('retry-after'),
          Math.max(config.retryBaseDelayMs, 1000)
        );
        // Drain the body so the socket can be reused/closed cleanly.
        try {
          await response.text();
        } catch {
          /* ignore */
        }
        return { kind: 'retry_after', retryAfterMs, status: 429 };
      }
      if (response.status >= 500) {
        try {
          await response.text();
        } catch {
          /* ignore */
        }
        return {
          kind: 'retryable',
          status: response.status,
          message: `courier responded ${response.status}`,
        };
      }
      if (response.status >= 400) {
        try {
          await response.text();
        } catch {
          /* ignore */
        }
        return {
          kind: 'fatal',
          status: response.status,
          message: `courier refused request with ${response.status}`,
        };
      }

      let data = null;
      try {
        data = await response.json();
      } catch {
        data = null;
      }
      return { kind: 'ok', data, status: response.status };
    })();
  }

  return {
    createShipment: (reference, address, items) =>
      request('POST', '/shipments', {
        body: { reference, address, items },
        timeoutMs: config.courierTimeoutMs,
      }),
    listShipments: (reference) =>
      request('GET', `/shipments?reference=${encodeURIComponent(reference)}`, {
        timeoutMs: config.courierTimeoutMs,
      }),
  };
}
