#!/usr/bin/env node
import { createConfig } from '../src/config.js';
import { connectDb, closeDb } from '../src/db.js';
import { listDeadLetterJobs, replayDeadLetterJob, getJob } from '../src/queue.js';

const [command, arg] = process.argv.slice(2);
const config = createConfig();
const db = connectDb(config.databasePath);

if (command === 'list') {
  const limit = Number.parseInt(arg ?? '', 10) || 50;
  const jobs = listDeadLetterJobs(db, limit);
  if (jobs.length === 0) {
    console.log('no dead-letter jobs');
  } else {
    for (const job of jobs) {
      console.log(`job ${job.id}`);
      console.log(`  order:     ${job.order_id}`);
      console.log(`  attempts:  ${job.attempts}/${job.max_attempts}`);
      const at = new Date(job.updated_at).toISOString();
      console.log(`  updated:   ${at}`);
      console.log(`  error:     ${job.last_error}`);
      let history = [];
      try { history = JSON.parse(job.error_history || '[]'); } catch { /* ignore */ }
      for (const h of history.slice(-5)) {
        console.log(`    - attempt ${h.attempt}: ${h.note}`);
      }
    }
  }
} else if (command === 'replay') {
  if (!arg) {
    console.error('usage: npm run dead-letter -- replay <jobId>');
    process.exit(1);
  }
  const job = getJob(db, arg);
  if (!job) {
    console.error(`job ${arg} not found`);
    process.exit(1);
  }
  if (job.state !== 'dead_letter') {
    console.error(`job ${arg} is not in dead_letter (state=${job.state}); refusing`);
    process.exit(1);
  }
  const orderShipment = db
    .prepare('SELECT status, courier_shipment_id FROM shipments WHERE order_id = ?')
    .get(job.order_id);
  const now = Date.now();
  const replayed = replayDeadLetterJob(db, arg, now);
  if (!replayed) {
    console.error(`job ${arg} could not be replayed (state changed concurrently)`);
    process.exit(1);
  }
  console.log(`job ${arg} requeued on its existing workflow (state=retrying, attempts reset)`);
  if (orderShipment) {
    console.log(
      `  shipment status: ${orderShipment.status}${orderShipment.courier_shipment_id ? ` (${orderShipment.courier_shipment_id})` : ''}`
    );
  }
  if (orderShipment && orderShipment.status === 'confirmed') {
    console.log('  note: shipment is already confirmed; the worker will resolve the job without another create.');
  }
  if (orderShipment && (orderShipment.status === 'uncertain' || orderShipment.status === 'processing')) {
    console.log('  note: outcome was ambiguous; replay first recovers via courier lookup instead of re-POSTing.');
  }
} else {
  console.error('usage: npm run dead-letter -- list [limit] | replay <jobId>');
  process.exit(1);
}

closeDb(db);
