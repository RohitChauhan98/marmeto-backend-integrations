# Marmeto Backend Integrations — Shipment Gateway

A Node.js service that receives Shopify-style `orders/create` webhooks and
creates shipments through an unreliable, rate-limited, non-idempotent courier
API — safely, despite duplicates, retries, timeouts, crashes, and restarts.

```
┌──────────┐  HMAC-signed   ┌─────────────────┐   SQLite    ┌────────────────┐    HTTP     ┌───────────────┐
│  Shopify │ ─────────────► │  Webhook server  │ ──enqueue──►│  Worker (poll) │ ──────────► │ Courier API   │
│ (sender) │  orders/create │  + idempotency   │    jobs     │  orchestration │  timeout →  │ (mock-courier)│
└──────────┘                │  over webhook id │ ◄────────── │  + lookup/retry│  recovery   │ 20% 500 / 10% │
                            └─────────────────┘  job states  └────────────────┘ ◄────────── │ 429 / 10% wait│
                                                 & leases                                   └───────────────┘
                     scripts/reconcile.js — periodic sweep for unconfirmed orders via courier lookup
```

## Project overview

- **POST /webhooks/orders-create** verifies the `X-Shop-Hmac-Sha256` header
  (base64 HMAC-SHA256 over the exact raw request body, `SHOPIFY_WEBHOOK_SECRET`),
  validates the payload, and persists the order + one shipment workflow job
  inside a single SQLite transaction before acknowledging. It responds in
  well under a second and never waits for courier work.
- **A persistent SQLite queue** (same database file) drives one coordinated
  shipment workflow per order, with leases, attempt counts, scheduled
  retries, and a dead-letter state. It survives restarts.
- **A background worker** claims jobs atomically, calls the courier client,
  and runs the orchestration state machine (create → ambiguity recovery →
  confirm/dead-letter).
- **scripts/mock-courier.js** simulates the courier: 20% HTTP 500, 10% HTTP
  429 with `Retry-After: 2`, 10% create-then-delay-10s (timeout), rest instant
  success. Every creation produces a new shipment id; duplicate references
  are allowed (the courier is deliberately non-idempotent). Deterministic
  failure injection via `MOCK_COURIER_MODE` / `X-Test-Fail-Mode` is available
  for tests without changing the default probabilistic behavior.
- **scripts/reconcile.js** sweeps orders without confirmed shipments and
  recovers them through courier lookups. **scripts/dead-letter.js** inspects
  and safely replays failed jobs.

## Prerequisites & installation

- Node.js ≥ 20 (tested on Node 24). No external services — SQLite is embedded.
  (`better-sqlite3` ships prebuilt binaries; nothing else is required.)

```bash
npm install
```

## Environment configuration

Copy `.env.example` to `.env` and adjust as needed. All values are
environment variables with sensible defaults, so a plain `npm run dev` works.

| Variable | Default | Purpose |
| --- | --- | --- |
| `WEBHOOK_SERVER_PORT` | `4550` | HTTP server port |
| `MOCK_COURIER_PORT` | `4600` | Mock courier port |
| `MOCK_COURIER_MODE` | *(unset → probabilistic)* | deterministic mode: `success` \| `error_500` \| `rate_limit_429` \| `timeout_wait` |
| `MOCK_COURIER_RETRY_AFTER_MS` | `2000` | Retry-After value sent by the mock on 429 |
| `MOCK_COURIER_TIMEOUT_DELAY_MS` | `10000` | response delay in the timeout scenario |
| `MOCK_COURIER_DATA_PATH` | `./data/mock-courier-shipments.json` | mock courier persistence |
| `SHOPIFY_WEBHOOK_SECRET` | `dev-secret-change-me` | HMAC shared secret (set a real one in production) |
| `DATABASE_PATH` | `./data/app.db` | shared SQLite path (server + worker) |
| `COURIER_BASE_URL` | `http://localhost:4600` | courier API base URL |
| `COURIER_TIMEOUT_MS` | `5000` | per-request `AbortController` timeout |
| `COURIER_MAX_ATTEMPTS` | `5` | attempt budget before `dead_letter` |
| `COURIER_RETRY_BASE_DELAY_MS` | `500` | exponential backoff base |
| `COURIER_RETRY_MAX_DELAY_MS` | `30000` | backoff cap (jitter is additive) |
| `COURIER_RETRY_JITTER_MS` | `250` | random jitter range added to each backoff |
| `WORKER_POLL_INTERVAL_MS` | `1000` | idle poll interval |
| `WORKER_LEASE_MS` | `60000` | processing lease; expired leases are recovered |
| `UNCERTAINTY_DELAY_MS` | `5000` | wait after an ambiguous timeout before acting again |

The database is created and migrated automatically on first use.

## One-command startup

```bash
npm run dev
# spawns: mock-courier (:4600), webhook server (:4550), worker (shared db)
# Ctrl+C tears everything down cleanly
```

## Webhook endpoint and example payload

```bash
# simple health check
curl http://localhost:4550/health
```

`POST /webhooks/orders-create`

```json
{
  "id": 701157001,
  "email": "customer@example.com",
  "shipping_address": { "first_name": "Rohit", "city": "Pune", "zip": "411057", "country": "IN" },
  "line_items": [ { "sku": "MARM-TSHIRT-BLK-M", "quantity": 1, "price": "599.00" } ]
}
```

Headers: `X-Shop-Hmac-Sha256: <base64 HMAC-SHA256 of the raw body>` and
`X-Shop-Webhook-Id: <unique delivery id>`. The signature is computed over the
exact bytes sent (the service re-serializes nothing). Duplicate deliveries
(with the same webhook id, or the same order id under another webhook id) get
`200 {"status":"ok","duplicate":true}` and schedule no additional work.

Responses: `401` missing/malformed/invalid signature; `400` invalid JSON,
invalid payload, or missing webhook id; `200` otherwise.

## Signed webhook and duplicate-delivery commands

```bash
npm run send-webhook -- valid --seed s1            # one valid webhook
npm run send-webhook -- duplicate --seed s1        # SAME webhook id twice, same order
npm run send-webhook -- dup-order --seed s1        # same order id, two different webhook ids
npm run send-webhook -- invalid --seed s1          # correctly formatted but wrong signature (401)

# optional flags: --url, --order-id, --webhook-id
# the script reads SHOPIFY_WEBHOOK_SECRET from the environment (or .env)
```

## Database design and idempotency constraints

Tables (single shared SQLite database, WAL mode):

- **orders** — `id PRIMARY KEY`, payload, shipping address, line items.
- **webhook_deliveries** — `webhook_id PRIMARY KEY`, order id, status,
  received_at; indexes on order id.
- **shipments** — `order_id PRIMARY KEY` (one row per order), status
  (`pending → processing → uncertain / lookup_empty → confirmed | failed`),
  courier shipment id, detail notes, confirmed_at.
- **jobs** — `id PRIMARY KEY`, type (`shipment`), unique index on
  `(order_id, type)` ⇒ one coordinated workflow per order; state
  (`pending / processing / retrying / completed / dead_letter`), attempts,
  max_attempts, `next_attempt_at`, `lease_owner`, `lease_expires_at`,
  `last_error`, `error_history` (full JSON attempt log), timestamps; index on
  `(state, next_attempt_at)`.

Both idempotency scenarios are enforced by database constraints inside one
`BEGIN IMMEDIATE` transaction per delivery (full synchronous, so concurrent
HTTP requests cannot interleave):

1. *Same webhook id twice*: `INSERT ... ON CONFLICT(webhook_id) DO NOTHING`
   on `webhook_deliveries` returns 0 changes → duplicate ack, no new work.
2. *Same order id, different webhook id*: `INSERT OR IGNORE` into `orders`
   hits the primary key → duplicate ack, no new work.
   `jobs(order_id, type)` is additionally UNIQUE, so even a bug elsewhere
   could never persist two shipment workflows for one order.

`confirmed` shipment rows are protected by guarded updates
(`WHERE status != 'confirmed'`): a delayed 201 from an earlier timed-out
attempt, a stale worker, duplicate deliveries, or a reconciliation run can
never reset or overwrite an already-confirmed shipment.

## Queue design and its trade-offs

- Jobs live in SQLite — no broker infrastructure, survives restarts by
  construction, cheap reads/writes, transactional with the rest of the
  domain data. Trade-off: fine for this scale; a production rollout would use
  a real queue (SQS/BullMQ) for horizontal scaling, visibility timeouts
  independent of the database, and fan-out.
- Claiming is an atomic `SELECT + guarded UPDATE` inside a transaction with
  the eligibility conditions (`pending/retrying` due → `processing` with
  expired lease) embedded in the UPDATE's WHERE clause, so two workers can
  never claim the same job.
- Leases: workers stamp `lease_owner`/`lease_expires_at`; reclaim is only
  legal past expiry; all end-of-attempt writes are conditional on still
  owning the lease, so stale workers cannot corrupt state. The lease is
  extended to ≥ 3× the courier timeout before any network call.
- Network requests never happen inside transactions; workers never sleep on
  behalf of a job — the wait is persisted as `next_attempt_at` (the poll loop
  picks something else up meanwhile).
- Completed jobs remain in the table for auditing; the unique index is on
  `(order_id, type)`, not on state, so replays must reuse the same row (they
  do).

## Retry, backoff, timeout, and rate-limit behavior

The courier client (`src/courier.js`) performs exactly one HTTP request per
attempt with an `AbortController` timeout and classifies the outcome:

| Outcome | Meaning | Action |
| --- | --- | --- |
| 2xx | created / list returned | confirm shipment, complete job |
| `429` | rate limited | schedules a retry **no earlier than `Retry-After`** (seconds or HTTP-date; missing/invalid/absurd → safe fallback of `max(base, 1s)`); job goes to `retrying` with the persisted wait |
| `5xx` | courier error, nothing created | exponential backoff `base·2^(attempts-1)` capped at `max`, plus jitter |
| other 4xx | courier refused (fatal) | shipment `failed`, job `dead_letter` |
| timeout / connection died mid-flight | **ambiguous** — the shipment may exist | shipment `uncertain`, schedule a recovery lookup after `UNCERTAINTY_DELAY_MS` |
| connection refused / DNS error | request never reached the courier | plain retryable (not ambiguous): backoff |

Attempt counting spans the whole workflow (create attempts and recovery
lookups share the same budget, one per claim). The final failure moves the
job to `dead_letter` with its attempt count and the complete error history
preserved.

### Ambiguous timeout recovery (the critical requirement)

A timeout is never treated as "not created". After an ambiguous outcome:

1. shipment status becomes `uncertain` and a recovery lookup is scheduled
   after the uncertainty window;
2. the worker runs `GET /shipments?reference=<orderId>`;
3. found → adopt the courier shipment id (earliest created if several),
   mark confirmed, complete the job;
4. none found → the wait continues (status `lookup_empty`) and a fresh
   create attempt is scheduled after the uncertainty window — the grace
   period is what makes it reasonable that the earlier ambiguous request
   has fully resolved;
5. lookup failed → keep `uncertain`, retry recovery later with backoff.

A crashed worker mid-POST produces the same path: the job's expired lease is
reclaimed, the leftover `processing` shipment status forces a lookup before
any new POST. Several shipments found for one reference are never followed
by another create; the earliest is adopted and the duplicates are recorded
in the shipment detail for investigation; reconciliation follows the same
deterministic policy.

**Known race (documented, not claimed away):** between a recovery lookup that
returns "none" and the subsequent POST, an unclearable sliver exists with a
non-idempotent courier (e.g., the courier answers the lookup slowly, then the
late first attempt creates a shipment). The system never overwrites a
confirmed record, flags any duplicates it observes, and converges to exactly
one shipment in practice, but absolute exactly-once creation against a
non-cooperating courier API is impossible; note the lookup-vs-late-POST race
explicitly in any threat model.

## Dead-letter inspection and replay

```bash
npm run dead-letter -- list          # show failed jobs with attempts + history
npm run dead-letter -- replay <jobId>  # requeue safely
```

Replay reuses the existing workflow row (resetting the attempt budget is an
explicit operator action from `dead_letter` only). If the shipment was
already confirmed in the meantime, the replay resolves without another
courier call; if the outcome was ambiguous, the replay first recovers via
lookup instead of blindly re-POSTing.

## Reconciliation

```bash
npm run reconcile                    # one pass; RECONCILE_VERBOSE=1 for a state dump
```

For every order without a confirmed shipment (crashed workers, dead letters,
timeouts, missing rows): skip jobs whose lease is still actively held; query
the courier by order reference; recover & confirm existing shipments
(earliest-first, duplicates flagged); schedule a safe retry on the existing
job only when the lookup proves nothing exists; preserve `uncertain` states
when the courier lookup fails; self-heal missing rows and workflow jobs. All
writes are conditionally-guarded, so repeat runs and concurrent runs with
the worker are safe (verified by tests).

## Automated tests

```bash
npm test
```

`node:test`, 28 tests, ~1s total, all deterministic: every flow runs against
 locally scriptable courier stubs (no randomness, no external network) with
in-memory databases and small injected timing values. Covered scenarios:

1. valid HMAC accepted, order + pending job persisted
2. invalid / missing / malformed / wrong-length signature → 401, no rows
3. invalid JSON & invalid payloads → 400, no rows
4. duplicate webhook id → duplicate ack, single order/job
5. same order id with another webhook id → duplicate ack, single order/job
6. concurrent webhook requests (HTTP burst) → one shipment workflow
7. HTTP 500 → exponential backoff, attempts persisted, dead-letter at budget
8. HTTP 429 → Retry-After respected exactly; missing header → safe fallback
9. timeout-after-creation → existing shipment recovered, `POST` count stays 1
10. late success responses cannot overwrite a concurrent confirmation
11. reconciliation recovers existing shipments (and executes safely when run
    concurrently with the worker)
12. repeated reconciliation creates no duplicate jobs/shipments and never
    POSTs
13. dead-letter replay is safe (same workflow, confirmed-state no-op)
14. worker crash after claim (expired lease) → job recovered, completes once
15. worker crash mid-POST → ambiguity later resolved via lookup
16. courier client unit behavior: classification, Retry-After parsing

(`npm run test` uses the glob `test/**/*.test.js`; helpers live in
`test/helpers.js`.)

## Design decisions

- **Synchronous SQLite (`better-sqlite3`) transactions**: webhook
  idempotency demands "check & insert atomically"; synchronous transactions
  make that statically free of interleaving bugs inside one process, and WAL
  mode supports safe multi-process sharing (server + worker).
- **Database-constraint idempotency** instead of check-then-act queries: the
  PK/unique indexes are the last line of defense; the app logic is only a
  fast path.
- **Queue in SQLite** — smallest reliable primitive for the assignment scale
  with crash recovery via leases.
- **One workflow job per order** — retries, replays, and reconciliation all
  manipulate the same row, so no path can create a second workflow.
- **Classification-first courier client** — a single attempt per call plus a
  persistent queue for waits, instead of in-process retry loops: the wait
  survives restarts, never blocks the worker, and Retry-After windows are
  persisted.
- **Ambiguous-timeout lookup protocol** — the only way to be safe against a
  non-idempotent courier is to consult it after uncertainty rather than
  blindly re-POST.
- **Raw-body HMAC verification** with strict header/lens checks before
  `timingSafeEqual`.

## What I would improve with another day

- Real broker behind the same interface (SQS/BullMQ) and horizontal worker
  scaling.
- Prometheus/OpenTelemetry metrics + structured logs and alerting on
  dead-letter arrivals and `uncertain` backlog age.
- An admin HTTP surface (list/replay dead letters, per-order state) instead
  of pure CLI.
- Property/chaos tests: random fault sequences, concurrent reconcilers,
  crash-at-every-await fuzzing.
- A Shopify `/webhooks` topics fan-out layer and subscription management.
- Fix the remaining lookup-vs-POST race by requiring courier-side
  idempotency keys (see limitations).

## Deliberately left out

- Authentication beyond HMAC on the webhook endpoint (Shopify-style).
- Persistent courier shipments list would be its own storage in reality; the
  mock keeps a JSON file for simplicity.
- Multi-tenant / multi-store support (one secret).
- Metrics, tracing, TLS, and a production web server config.
- Migration tooling beyond a single idempotent schema (auto-created).

## Known limitations

- **Exactly-once shipment creation is not guaranteed and cannot be**, with a
  non-idempotent courier: an ambiguous timeout → "no shipment found" →
  re-create sequence can race with a late first request that the courier
  accepts afterwards. Mitigations in place: grace window before re-creating,
  confirm-only-once guards (`status != 'confirmed'`), duplicate
  detection & flagging, and reconciliation adopting instead of re-posting.
  The correct fix requires a courier that honors an idempotency key or
  exposes a "create-or-adopt" call.
- Lease theft is bounded but possible in clocks-skewed multi-host setups
  (single-process dev has no such skew).
- The webhook responds only after SQLite persistence (sub-millisecond
  typically); if the disk stalls, the response is still bound by the
  WAL write, not any courier clock.
- `data/` and secrets are gitignored; set real values for `SHOPIFY_WEBHOOK_SECRET`
  in any shared environment.
