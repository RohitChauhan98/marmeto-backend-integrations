import { withTransaction, isoNow } from './db.js';

export const SHIPMENT_STATES = {
  PENDING: 'pending',
  PROCESSING: 'processing',
  UNCERTAIN: 'uncertain',
  LOOKUP_EMPTY: 'lookup_empty',
  CONFIRMED: 'confirmed',
  FAILED: 'failed',
};

export function createShipmentRow(db, orderId, now, status = SHIPMENT_STATES.PENDING) {
  db.prepare(
    `INSERT OR IGNORE INTO shipments
       (order_id, status, courier_shipment_id, detail, confirmed_at,
        created_at, updated_at)
     VALUES (?, ?, NULL, '', NULL, ?, ?)`
  ).run(orderId, status, now, now);
}

export function getShipment(db, orderId) {
  return db.prepare('SELECT * FROM shipments WHERE order_id = ?').get(orderId);
}

function appendDetail(existing, note) {
  const prefix = existing ? `${existing} | ` : '';
  return `${prefix}${String(note).slice(0, 1000)}`;
}

/**
Status update guarded so a confirmed shipment is never modified: any failed
or ambiguous outcome from an earlier attempt cannot overwrite a shipment
that a later operation already confirmed.
*/
export function updateShipmentState(db, orderId, status, note, now, fields = {}) {
  const info = db
    .prepare(
      `UPDATE shipments
       SET status = ?,
           detail = ?,
           courier_shipment_id = COALESCE(?, courier_shipment_id),
           updated_at = ?
       WHERE order_id = ? AND status != 'confirmed'`
    )
    .run(status, note, fields.courierShipmentId ?? null, now, orderId);
  return info.changes === 1;
}

/**
Confirms a shipment for an order. Returns:
- 'applied': this call persisted the confirmation
- 'already_confirmed': a prior confirmation exists under a different id
- 'duplicate_discrepancy': already confirmed with the same id (no-op)

The update is conditional on `status != 'confirmed'`, so whichever
operation confirms first wins and later, stale results keep losing.
*/
export function confirmShipment(db, orderId, courierShipmentId, note, now = isoNow()) {
  return withTransaction(db, () => {
    const existing = getShipment(db, orderId);
    if (existing && existing.status === SHIPMENT_STATES.CONFIRMED) {
      if (existing.courier_shipment_id === courierShipmentId) {
        return 'duplicate_discrepancy';
      }
      db.prepare(
        `UPDATE shipments
         SET detail = ?, updated_at = ?
         WHERE order_id = ?`
      ).run(
        appendDetail(
          existing.detail,
          `late result suppressed: shipment already confirmed as ${existing.courier_shipment_id}; late id ${courierShipmentId} discarded`
        ),
        now,
        orderId
      );
      return 'already_confirmed';
    }
    const info = db
      .prepare(
        `UPDATE shipments
         SET status = 'confirmed',
             courier_shipment_id = ?,
             detail = COALESCE(?, detail),
             confirmed_at = ?,
             updated_at = ?
         WHERE order_id = ? AND status != 'confirmed'`
      )
      .run(courierShipmentId, note, now, now, orderId);
    return info.changes === 1 ? 'applied' : 'already_confirmed';
  });
}

/**
Given multiple shipments found at the courier for one reference (the courier
is not idempotent and allowed duplicate creations), adopts a deterministic
recovery policy: keep the earliest-created shipment as the confirmed one and
record the duplicate condition for investigation. No additional shipment is
created.
*/
export function adoptShipmentFromList(db, orderId, courierShipments, note, now) {
  const sorted = [...courierShipments].sort((a, b) => {
    const ca = a.createdAt ?? 0;
    const cb = b.createdAt ?? 0;
    if (ca !== cb) return ca - cb;
    return String(a.shipmentId).localeCompare(String(b.shipmentId));
  });
  const adopted = sorted[0];
  const dupes = sorted.slice(1);
  let detail = selectExistingDetail(db, orderId);
  if (dupes.length > 0) {
    detail = appendDetail(
      detail,
      `duplicate shipments detected at courier: adopted ${adopted.shipmentId}; duplicates ${dupes
        .map((s) => s.shipmentId)
        .join(',')} recorded for investigation`
    );
  } else {
    detail = appendDetail(detail, note);
  }
  const result = confirmShipment(db, orderId, adopted.shipmentId, detail, now);
  return { adopted, duplicates: dupes.length, result };
}

function selectExistingDetail(db, orderId) {
  const row = getShipment(db, orderId);
  return row?.detail || '';
}
