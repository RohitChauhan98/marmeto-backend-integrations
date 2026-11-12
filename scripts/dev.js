#!/usr/bin/env node
/**
Development orchestrator: starts the mock courier, the webhook server, and
the background worker as child processes sharing one database, then tears
everything down cleanly on SIGINT/SIGTERM/child failures.
*/
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)));

const children = [];
let shuttingDown = false;

function start(label, args, extraEnv = {}) {
  const child = spawn(process.execPath, args, {
    cwd: ROOT,
    env: { ...process.env, ...extraEnv },
    stdio: ['ignore', 'inherit', 'inherit'],
  });
  child.on('exit', (code, signal) => {
    console.log(`[dev] ${label} exited (code=${code ?? 'null'} signal=${signal ?? 'null'})`);
    if (!shuttingDown && code !== 0) {
      console.log('[dev] child failed; shutting everything down');
      shutdown(1);
    }
  });
  child.label = label;
  children.push(child);
  return child;
}

function shutdown(exitCode = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('\n[dev] shutting down...');
  for (const child of children) {
    if (child.exitCode === null && !child.killed) {
      child.kill('SIGTERM');
    }
  }
  const forceTimer = setTimeout(() => {
    for (const child of children) {
      if (child.exitCode === null) child.kill('SIGKILL');
    }
    process.exit(exitCode);
  }, 3000);
  forceTimer.unref();
  const graceTimer = setInterval(() => {
    if (children.every((c) => c.exitCode !== null || c.killed)) {
      clearInterval(graceTimer);
      process.exit(exitCode);
    }
  }, 100);
  graceTimer.unref?.();
}

process.on('SIGINT', () => shutdown(0));
process.on('SIGTERM', () => shutdown(0));
// Some terminals / wrappers lose the process group on HUP.
process.on('SIGHUP', () => shutdown(0));

console.log('[dev] starting mock courier, webhook server, and worker');
console.log(`[dev] dashboard: http://localhost:${process.env.WEBHOOK_SERVER_PORT || 4550}/`);
start('mock-courier', ['scripts/mock-courier.js'], { MOCK_COURIER_CONTROL: '1' });
// Give the courier a moment to bind before the server/worker hit it.
setTimeout(() => {
  start('webhook-server', ['src/main.js']);
  start('worker', ['src/worker-main.js']);
}, 400);
