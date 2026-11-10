import { getJobByOrder } from './queue.js';
import {
  createShipmentRow,
  getShipment,
  updateShipmentState,
  adoptShipmentFromList,
  SHIPMENT_STATES,
} from './shipments.js';
import { withTransaction } from './db.js';

/**
Reconciliation pass: find orders without a confirmed shipment (ambiguous
timeouts, worker crashes, dead-lettered jobs, missing workflow rows) and
recover them exclusively through the existing one-per-order job workflow.
It never creates additional jobs, never blindly resends uncertain create
requests, and never overwrites a confirmed shipment.

Per eligible order:
- a worker holding an unexpired lease causes a skip (never race the worker);
- the courier is queried by order reference;
- shipment exists -> confirm (earliest created wins; duplicates flagged for
  investigation) and mark the existing job completed, so repeated runs are
  no-ops;
- no shipment -> schedule one safe create retry on the existing job row
  after the uncertainty window; the retry is only scheduled because the
  lookup has proven the courier has no shipment;
- courier lookup failure -> preserve the current state (uncertain stays
  uncertain) and count the error.

Safe to run repeatedly and concurrently with the worker: every write is a
conditional UPDATE that can only win against an expired lease or a
non-processing state, shipment writes are guarded against confirmed rows,
and courier lookups happen outside transactions.
*/
export async function reconcile(db, courier, config, { nowFn = Date.now } = {}) {
  const summary = {
    ordersExamined: 0,
    shipmentsRecovered: 0,
    jobsRequeued: 0,
    errorsEncountered: 0,
    details: [],
  };

  // Phase 1: transactional self-heal of rows; collect eligible orders.
  const eligible = withTransaction(db, () => {
    const targets = [];
    for (const order of db.prepare('SELECT * FROM orders').all()) {
      let shipment = getShipment(db, order.id);
      if (shipment && shipment.status === SHIPMENT_STATES.CONFIRMED) continue;
      summary.ordersExamined += 1;

      let job = getJobByOrder(db, order.id);
      if (!job) {
        db.prepare(
          `INSERT OR IGNORE INTO jobs
             (id, type, order_id, state, attempts, max_attempts,
              next_attempt_at, created_at, updated_at)
           VALUES ('recon-' || hex(randomblob(16)), 'shipment', ?,
                   'pending', 0, ?, ?, ?, ?)`
        ).run(order.id, config.courierMaxAttempts, nowFn(), nowFn(), nowFn());
        job = getJobByOrder(db, order.id);
        summary.jobsRequeued += 1;
        summary.details.push(`${order.id}: recreated missing workflow job`);
      }
      if (!shipment) {
        createShipmentRow(db, order.id, nowFn());
        shipment = getShipment(db, order.id);
      }

      const now = nowFn();
      const busy =
        job.state === 'processing' &&
        job.lease_expires_at !== null &&
        job.lease_expires_at > now;
      if (busy) {
        summary.details.push(`${order.id}: worker lease active; skipped`);
        continue;
      }
      targets.push({ order });
    }
    return targets;
  });

  // Phase 2: courier lookups happen outside any transaction.
  for (const { order } of eligible) {
    await recoverOne(db, order, courier, config, summary, nowFn);
  }
  return summary;
}

async function recoverOne(db, order, courier, config, summary, nowFn) {
  const now = nowFn();
  const lookup = await courier.listShipments(order.id);

  if (lookup.kind === 'ok' && Array.isArray(lookup.data)) {
    if (lookup.data.length > 0) {
      const { adopted, result } = adoptShipmentFromList(
        db,
        order.id,
        lookup.data,
        'recovered by reconciliation',
        now
      );
      if (result === 'applied') summary.shipmentsRecovered += 1;
      const info = db
        .prepare(
          `UPDATE jobs
           SET state = 'completed',
               lease_owner = NULL,
               lease_expires_at = NULL,
               updated_at = ?
           WHERE id = ?
             AND (
               state != 'processing' OR
               (lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
             )`
        )
        .run(now, getJobByOrder(db, order.id).id, now);
      summary.details.push(
        `${order.id}: recovered shipment ${adopted.shipmentId}${info.changes === 1 ? '' : ' (job left untouched; worker retried first)'}`
      );
      return;
    }

    // The courier definitively has nothing for this reference, so one more
    // create attempt is permitted by the recovery policy.
    updateShipmentState(
      db,
      order.id,
      SHIPMENT_STATES.LOOKUP_EMPTY,
      'reconciliation lookup found no shipment; scheduling safe retry after uncertainty window',
      now
    );
    const info = db
      .prepare(
        `UPDATE jobs
         SET state = 'retrying',
             attempts = 0,
             next_attempt_at = ?,
             lease_owner = NULL,
             lease_expires_at = NULL,
             updated_at = ?
         WHERE id = ?
           AND (
             state != 'processing' OR
             (lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
           )`
      )
      .run(now + config.uncertaintyDelayMs, now, getJobByOrder(db, order.id).id, now);
    if (info.changes === 1) {
      summary.jobsRequeued += 1;
      summary.details.push(`${order.id}: no shipment at courier; job requeued safely`);
    } else {
      summary.details.push(`${order.id}: worker reclaimed job before reconciliation could requeue`);
    }
    return;
  }

  summary.errorsEncountered += 1;
  summary.details.push(
    `${order.id}: courier lookup failed (${lookup.message || lookup.kind}); state preserved`
  );
  const shipment = getShipment(db, order.id);
  if (shipment && shipment.status === SHIPMENT_STATES.PROCESSING) {
    // A crashed worker's POST may exist; its outcome remains unknown.
    updateShipmentState(
      db,
      order.id,
      SHIPMENT_STATES.UNCERTAIN,
      'reconciliation lookup failed; retaining ambiguous outcome',
      now
    );
  }
}
