import { withTransaction, newId } from './db.js';

export const JOB_TYPE_SHIPMENT = 'shipment';

export const JOB_STATES = {
  PENDING: 'pending',
  PROCESSING: 'processing',
  RETRYING: 'retrying',
  COMPLETED: 'completed',
  DEAD_LETTER: 'dead_letter',
};

/**
Exponential backoff: base * 2^(failedAttempts-1), capped at maxDelay, plus
random jitter so concurrent jobs do not retry in lockstep.
`failedAttempts` is the number of attempts that have failed so far.
*/
export function computeRetryDelayMs(config, failedAttempts) {
  const exp = Math.max(failedAttempts - 1, 0);
  const raw = config.retryBaseDelayMs * 2 ** exp;
  const capped = Math.min(raw, config.retryMaxDelayMs);
  const jitter =
    config.retryJitterMs > 0 ? Math.random() * config.retryJitterMs : 0;
  return Math.round(capped + jitter);
}

export function enqueueShipmentJob(db, orderId, config, now) {
  const result = withTransaction(db, () => {
    const stmt = db.prepare(
      `INSERT OR IGNORE INTO jobs
         (id, type, order_id, state, attempts, max_attempts,
          next_attempt_at, created_at, updated_at)
       VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, ?)`
    );
    const info = stmt.run(
      newId(),
      JOB_TYPE_SHIPMENT,
      orderId,
      config.courierMaxAttempts,
      now,
      now,
      now
    );
    return info.changes === 1;
  });
  return result;
}

/**
Atomically claims the next eligible job. Eligible means:
- pending/retrying with a due next_attempt_at, or
- processing but past its lease expiry (worker crash recovery).

The SELECT and UPDATE run inside a single immediate transaction, so two
concurrent workers can never claim the same job.
*/
export function claimNextJob(db, { now, leaseMs, owner }) {
  return withTransaction(db, () => {
    const candidate = db
      .prepare(
        `SELECT * FROM jobs
         WHERE (state IN ('pending', 'retrying') AND next_attempt_at <= ?)
            OR (state = 'processing'
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at <= ?)
         ORDER BY next_attempt_at ASC, created_at ASC
         LIMIT 1`
      )
      .get(now, now);
    if (!candidate) return null;

    const info = db
      .prepare(
        `UPDATE jobs
         SET state = 'processing',
             attempts = attempts + 1,
             lease_owner = ?,
             lease_expires_at = ?,
             updated_at = ?
         WHERE id = ?
           AND (
             (state IN ('pending', 'retrying') AND next_attempt_at <= ?) OR
             (state = 'processing'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at <= ?)
           )`
      )
      .run(owner, now + leaseMs, now, candidate.id, now, now);
    if (info.changes !== 1) return null;

    return { ...candidate, attempts: candidate.attempts + 1, lease_owner: owner };
  });
}

/** Marks a job completed, but only while the claimant still owns the lease. */
export function completeJob(db, job, now, completionNote) {
  const info = db
    .prepare(
      `UPDATE jobs
       SET state = 'completed',
           lease_owner = NULL,
           lease_expires_at = NULL,
           last_error = COALESCE(?, last_error),
           updated_at = ?
       WHERE id = ? AND state = 'processing' AND lease_owner = ?`
    )
    .run(completionNote ?? job.last_error, now, job.id, job.lease_owner);
  return info.changes === 1;
}

/**
Moves a job to dead_letter (attempt budget exhausted). The caller is a
worker that currently owns the lease.
*/
export function deadLetterJob(db, job, now, note) {
  const history = appendError(db, job, note, now, { throughLeaseOwner: job.lease_owner });
  const info = db
    .prepare(
      `UPDATE jobs
       SET state = 'dead_letter',
           lease_owner = NULL,
           lease_expires_at = NULL,
           last_error = ?,
           error_history = ?,
           updated_at = ?
       WHERE id = ? AND state = 'processing' AND lease_owner = ?`
    )
    .run(note, history, now, job.id, job.lease_owner);
  return info.changes === 1;
}

/**
Schedules the next attempt. Sets `retrying` and persists next_attempt_at.
When the attempt budget (max_attempts) is exhausted the job moves to
dead_letter instead, keeping its error details and attempt count.
Returns 'sched_retrying' | 'dead_letter'.
*/
export function scheduleJobRetry(db, job, { delayMs, note, now, nextAttemptAt }) {
  const history = appendError(db, job, note, now, { throughLeaseOwner: job.lease_owner });
  if (job.attempts >= job.max_attempts) {
    const info = db
      .prepare(
        `UPDATE jobs
         SET state = 'dead_letter',
             lease_owner = NULL,
             lease_expires_at = NULL,
             next_attempt_at = next_attempt_at,
             last_error = ?,
             error_history = ?,
             updated_at = ?
         WHERE id = ? AND state = 'processing' AND lease_owner = ?`
      )
      .run(note, history, now, job.id, job.lease_owner);
    return info.changes === 1 ? 'dead_letter' : 'lost';
  }
  const at = nextAttemptAt ?? now + Math.max(delayMs, 0);
  const info = db
    .prepare(
      `UPDATE jobs
       SET state = 'retrying',
           lease_owner = NULL,
           lease_expires_at = NULL,
           next_attempt_at = ?,
           last_error = ?,
           error_history = ?,
           updated_at = ?
       WHERE id = ? AND state = 'processing' AND lease_owner = ?`
    )
    .run(at, note, history, now, job.id, job.lease_owner);
  return info.changes === 1 ? 'sched_retrying' : 'lost';
}

function appendError(db, job, note, now) {
  let history;
  try {
    history = JSON.parse(job.error_history || '[]');
  } catch {
    history = [];
  }
  history.push({
    attempt: job.attempts,
    at: now,
    note: String(note).slice(0, 2000),
  });
  return JSON.stringify(history);
}

/**
Operator / reconciliation requeue of a dead-lettered job. The existing
workflow row is reused (one coordinated workflow per order); attempts reset
to zero so the replay gets a full budget. Returns true when a dead-letter
job was transitioned.
*/
export function replayDeadLetterJob(db, jobId, now, delayMs = 0) {
  const info = db
    .prepare(
      `UPDATE jobs
       SET state = 'retrying',
           attempts = 0,
           next_attempt_at = ?,
           lease_owner = NULL,
           lease_expires_at = NULL,
           updated_at = ?
       WHERE id = ? AND state = 'dead_letter'`
    )
    .run(now + Math.max(delayMs, 0), now, jobId);
  return info.changes === 1;
}

export function getJob(db, jobId) {
  return db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
}

export function getJobByOrder(db, orderId) {
  return db
    .prepare(`SELECT * FROM jobs WHERE order_id = ? AND type = 'shipment'`)
    .get(orderId);
}

export function listDeadLetterJobs(db, limit = 100) {
  return db
    .prepare(
      `SELECT * FROM jobs WHERE state = 'dead_letter'
       ORDER BY updated_at DESC LIMIT ?`
    )
    .all(limit);
}

export function countJobs(db, state) {
  return db
    .prepare(`SELECT COUNT(*) AS c FROM jobs WHERE state = ?`)
    .get(state).c;
}
