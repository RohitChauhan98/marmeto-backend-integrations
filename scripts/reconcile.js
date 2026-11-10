#!/usr/bin/env node
import { createConfig } from '../src/config.js';
import { connectDb, closeDb } from '../src/db.js';
import { createCourierClient } from '../src/courier.js';
import { reconcile } from '../src/reconcile.js';
import { SHIPMENT_STATES } from '../src/shipments.js';

const config = createConfig(process.argv.slice(2).length ? { courierBaseUrl: argvCourierUrl() } : {});
const db = connectDb(config.databasePath);
const courier = createCourierClient(config);

const summary = await reconcile(db, courier, config, {});

console.log('--- reconciliation summary ---');
console.log(`orders examined:      ${summary.ordersExamined}`);
console.log(`shipments recovered:  ${summary.shipmentsRecovered}`);
console.log(`jobs requeued:        ${summary.jobsRequeued}`);
console.log(`errors encountered:   ${summary.errorsEncountered}`);
for (const detail of summary.details) {
  console.log(`  - ${detail}`);
}
if (process.env.RECONCILE_VERBOSE) {
  for (const row of db
    .prepare('SELECT order_id, status, courier_shipment_id FROM shipments')
    .all()) {
    console.log(`  [state] ${row.order_id}: ${row.status} ${row.courier_shipment_id || ''}`);
  }
}
closeDb(db);

function argvCourierUrl() {
  const i = process.argv.indexOf('--courier-base-url');
  return i >= 0 ? process.argv[i + 1] : undefined;
}
