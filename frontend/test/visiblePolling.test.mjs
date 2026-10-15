import { test } from 'node:test';
import assert from 'node:assert/strict';
import { visiblePolling } from '../src/visiblePolling.ts';

function environment() {
  const listeners = new Set();
  const timers = new Map();
  let next = 0;
  const visibility = {
    hidden: false,
    addEventListener: (_, listener) => listeners.add(listener),
    removeEventListener: (_, listener) => listeners.delete(listener),
  };
  return {
    options: { visibility, intervalMs: 10_000, schedule: (callback, delay) => { timers.set(++next, { callback, delay }); return next; }, cancel: id => timers.delete(id) },
    change(hidden) { visibility.hidden = hidden; for (const listener of listeners) listener(); },
    run(delay) { for (const [id, timer] of [...timers]) if (timer.delay === delay) { timers.delete(id); timer.callback(); } },
    listeners, timers,
  };
}
const settled = async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); };

test('visible result polling continues after a completed result and stops while hidden', async () => {
  const env = environment();
  let reads = 0;
  const stop = visiblePolling(async () => { reads++; }, env.options);
  await settled();
  assert.equal(reads, 1);
  env.run(10_000);
  await settled();
  assert.equal(reads, 2);
  env.change(true);
  env.run(10_000);
  await settled();
  assert.equal(reads, 2);
  env.change(false);
  await settled();
  assert.equal(reads, 3);
  stop();
  assert.equal(env.listeners.size, 0);
  assert.equal(env.timers.size, 0);
});

test('a failed read does not prevent the next monitoring poll', async () => {
  const env = environment();
  let reads = 0;
  const stop = visiblePolling(async () => { reads++; if (reads === 1) throw new Error('Temporary storage failure'); }, env.options);
  await settled();
  env.run(10_000);
  await settled();
  assert.equal(reads, 2);
  stop();
});

test('visibility return aborts hidden work and waits before refetching without overlap', async () => {
  const env = environment();
  let reads = 0;
  let signal;
  let finish;
  const stop = visiblePolling(async currentSignal => {
    reads++;
    signal = currentSignal;
    if (reads === 1) await new Promise(resolve => { finish = resolve; });
  }, env.options);
  env.change(true);
  assert.equal(signal.aborted, true);
  env.change(false);
  assert.equal(reads, 1);
  finish();
  await settled();
  env.run(0);
  await settled();
  assert.equal(reads, 2);
  stop();
});

test('unmount cancels a request and prevents a late completion from scheduling another poll', async () => {
  const env = environment();
  let signal;
  let finish;
  const stop = visiblePolling(async currentSignal => {
    signal = currentSignal;
    await new Promise(resolve => { finish = resolve; });
  }, env.options);
  stop();
  assert.equal(signal.aborted, true);
  finish();
  await settled();
  assert.equal(env.timers.size, 0);
});
