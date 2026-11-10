import {
  claimNextJob,
  completeJob,
  scheduleJobRetry,
  deadLetterJob,
  computeRetryDelayMs,
} from './queue.js';
import {
  createShipmentRow,
  getShipment,
  updateShipmentState,
  confirmShipment,
  adoptShipmentFromList,
  SHIPMENT_STATES,
} from './shipments.js';

/**
Shipment orchestration worker.

One `shipment` job per order (database-unique) drives the entire workflow:
create attempt -> ambiguity recovery lookups -> retries -> confirmation or
dead-letter. All state transitions for the order go through this job, so
retries, dead-letter replay, and reconciliation share one coordinated
workflow instead of creating independent shipment requests.

Claims are short-lived leases on a `processing` state; network calls happen
outside transactions, with the lease extended to comfortably cover the
courier timeout before any request is issued. If a worker crashes mid-POST,
the expired lease lets another worker pick the job up, and the shipment
status (`processing` = possibly in flight) forces an ambiguity lookup before
any new POST.
*/
export function createWorker(db, config, courier, { nowFn = Date.now, sleep = defaultSleep } = {}) {
  const owner = `worker-${process.pid}-${Math.random().toString(16).slice(2, 8)}`;

  function extendLease(job, untilMs) {
    db.prepare(
      `UPDATE jobs SET lease_expires_at = ?, updated_at = ?
       WHERE id = ? AND state = 'processing' AND lease_owner = ?`
    ).run(untilMs, nowFn(), job.id, job.lease_owner);
  }

  async function attemptCreate(job, shipment, order, now) {
    updateShipmentState(
      db,
      job.order_id,
      SHIPMENT_STATES.PROCESSING,
      `create attempt #${job.attempts} started`,
      now
    );
    // Lease must outlive the full courier timeout, otherwise another worker
    // could reclaim while the first request is still in flight.
    extendLease(job, now + Math.max(config.workerLeaseMs, config.courierTimeoutMs * 3));

    const res = await courier.createShipment(
      job.order_id,
      JSON.parse(order.shipping_address),
      JSON.parse(order.line_items)
    );

    if (res.kind === 'ok') {
      const shipmentId = res.data && res.data.shipmentId;
      if (!shipmentId) {
        // 2xx without a usable shipment id: outcome is unknown.
        updateShipmentState(
          db,
          job.order_id,
          SHIPMENT_STATES.UNCERTAIN,
          `courier returned ${res.status} without a shipmentId`,
          now
        );
        return scheduleAmbiguityLookup(job, now, 'no shipment id in response');
      }
      const outcome = confirmShipment(
        db,
        job.order_id,
        shipmentId,
        `confirmed from create attempt #${job.attempts}`,
        nowFn()
      );
      return completeJob(db, job, nowFn(), `shipment ${shipmentId} confirmed (${outcome})`);
    }
    if (res.kind === 'retry_after') {
      // Honor Retry-After exactly; never retry earlier than requested.
      return scheduleJobRetry(db, job, {
        nextAttemptAt: nowFn() + res.retryAfterMs,
        note: `rate limited (429); honoring Retry-After=${res.retryAfterMs}ms`,
        now: nowFn(),
      });
    }
    if (res.kind === 'retryable') {
      return scheduleJobRetry(db, job, {
        delayMs: computeRetryDelayMs(config, job.attempts),
        note: `${res.message} (status ${res.status})`,
        now: nowFn(),
      });
    }
    if (res.kind === 'fatal') {
      updateShipmentState(
        db,
        job.order_id,
        SHIPMENT_STATES.FAILED,
        res.message,
        now
      );
      return deadLetterJob(db, job, nowFn(), `${res.message} (status ${res.status})`);
    }
    // res.kind === 'uncertain': the POST may have created a shipment.
    updateShipmentState(
      db,
      job.order_id,
      SHIPMENT_STATES.UNCERTAIN,
      `create request outcome unknown (${res.message}); will consult courier before creating again`,
      now
    );
    return scheduleAmbiguityLookup(job, now, `uncertain: ${res.message}`);
  }

  function scheduleAmbiguityLookup(job, now, note) {
    return scheduleJobRetry(db, job, {
      delayMs: config.uncertaintyDelayMs,
      note,
      now: nowFn(),
    });
  }

  /**
  Ambiguity recovery: consult GET /shipments?reference=<orderId> instead of
  blindly creating another shipment. Only when the courier answers that no
  shipment exists does the workflow schedule a fresh create attempt (after
  an uncertainty grace window). Lookup failures keep the uncertain state.
  */
  async function recoverFromAmbiguity(job, shipment, order, now) {
    extendLease(job, nowFn() + Math.max(config.workerLeaseMs, config.courierTimeoutMs * 3));
    const res = await courier.listShipments(job.order_id);
    if (res.kind === 'ok' && Array.isArray(res.data)) {
      updateShipmentState(
        db,
        job.order_id,
        SHIPMENT_STATES.PROCESSING,
        `recovery lookup executed (${res.data.length} found)`,
        now
      );
      if (res.data.length === 0) {
        updateShipmentState(
          db,
          job.order_id,
          SHIPMENT_STATES.LOOKUP_EMPTY,
          `recovery lookup found no shipment; create may be retried after uncertainty window`,
          now
        );
        return scheduleJobRetry(db, job, {
          delayMs: config.uncertaintyDelayMs,
          note: 'recovery lookup found none; scheduling safe create retry',
          now: nowFn(),
        });
      }
      const { adopted, duplicates, result } = adoptShipmentFromList(
        db,
        job.order_id,
        res.data,
        `recovered shipment from courier lookup`,
        nowFn()
      );
      return completeJob(
        db,
        job,
        nowFn(),
        `shipment ${adopted.shipmentId} confirmed by recovery lookup (${result}, duplicates=${duplicates})`
      );
    }

    const note =
      res.kind === 'retry_after'
        ? `recovery lookup rate limited; honoring Retry-After=${res.retryAfterMs}ms`
        : `recovery lookup failed (${res.message}); retaining uncertain state`;
    if (shipment.status === SHIPMENT_STATES.PROCESSING) {
      // A crashed worker's POST outcome is still unknown: stay ambiguous.
      updateShipmentState(
        db,
        job.order_id,
        SHIPMENT_STATES.UNCERTAIN,
        note,
        now
      );
    } // else status stays 'uncertain'
    const delay =
      res.kind === 'retry_after'
        ? res.retryAfterMs
        : computeRetryDelayMs(config, job.attempts);
    return scheduleJobRetry(db, job, { delayMs: delay, note, now: nowFn() });
  }

  async function processClaimedJob(job, now) {
    let shipment = getShipment(db, job.order_id);
    const order = db.prepare('SELECT * FROM orders WHERE id = ?').get(job.order_id);
    if (!shipment || !order) {
      // Defensive self-heal: the workflow rows are recreated from the order
      // payload rather than dropping the job.
      if (!order) {
        return deadLetterJob(db, job, nowFn(), 'order row missing; cannot create shipment');
      }
      createShipmentRow(db, job.order_id, now);
      shipment = getShipment(db, job.order_id);
    }

    if (shipment.status === SHIPMENT_STATES.CONFIRMED) {
      return completeJob(
        db,
        job,
        nowFn(),
        `shipment ${shipment.courier_shipment_id} already confirmed; nothing to do`
      );
    }
    if (
      shipment.status === SHIPMENT_STATES.PROCESSING ||
      shipment.status === SHIPMENT_STATES.UNCERTAIN
    ) {
      return recoverFromAmbiguity(job, shipment, order, now);
    }
    return attemptCreate(job, shipment, order, now);
  }

  /** Claims and processes at most one eligible job. Returns a summary. */
  async function processNextJob() {
    const now = nowFn();
    const job = claimNextJob(db, { now, leaseMs: config.workerLeaseMs, owner });
    if (!job) return { claimed: false };
    let outcome;
    try {
      outcome = await processClaimedJob(job, now);
    } catch (error) {
      outcome = scheduleJobRetry(db, job, {
        delayMs: computeRetryDelayMs(config, job.attempts),
        note: `worker exception: ${error.message}`,
        now: nowFn(),
      });
    }
    return { claimed: true, jobId: job.id, orderId: job.order_id, outcome };
  }

  async function runLoop(signal) {
    while (!signal || !signal.aborted) {
      const result = await processNextJob();
      if (!result.claimed) {
        await sleep(config.workerPollIntervalMs);
      }
    }
  }

  return { owner, processNextJob, runLoop };
}

function defaultSleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
