import { test } from 'node:test';
import assert from 'node:assert/strict';
import { rowDetails } from '../src/rowPresentation.ts';

test('terminal success never shows historical queued admission or publishing phase', () => {
  assert.equal(
    rowDetails({
      status: 'COMPLETED',
      admission: 'QUEUED',
      phase: 'PUBLISHING',
    }),
    '',
  );
});
test('cached and shared results remain admission explanations without active phases', () => {
  assert.equal(
    rowDetails({
      status: 'COMPLETED',
      admission: 'CACHED',
      phase: 'PUBLISHING',
    }),
    'Cached result',
  );
  assert.equal(
    rowDetails({
      status: 'COMPLETED',
      admission: 'COALESCED',
      phase: 'PUBLISHING',
    }),
    'Shared run',
  );
});
test('only running work shows its current phase', () => {
  assert.equal(rowDetails({ status: 'RUNNING', admission: 'QUEUED', phase: 'PUBLISHING' }), 'Publishing result');
  assert.equal(rowDetails({ status: 'QUEUED', admission: 'QUEUED', phase: 'PUBLISHING' }), 'Waiting to start');
});
test('terminal errors never imply execution is continuing', () => {
  for (const status of ['FAILED', 'TIMED_OUT']) {
    assert.equal(
      rowDetails({
        status,
        admission: 'QUEUED',
        phase: 'DISCOVERING',
        error: { message: 'Source unavailable.' },
      }),
      'Source unavailable.',
    );
  }
});

test('persisted pending inputs explain that they are waiting without claiming to be running', () => {
  assert.equal(rowDetails({ status: 'PENDING', admission: null }), 'Saved · waiting for admission capacity');
});

test('a failed update keeps its error and explicitly explains the accepted download', () => {
  const error = { message: 'Could not revalidate the installation guide.' };
  const message = rowDetails({
    status: 'FAILED',
    admission: 'QUEUED',
    artifactUrl: '/artifacts/site/llms.txt',
    error,
  });

  assert.match(message, /Could not revalidate/);
  assert.match(message, /An accepted file is still available/);
  assert.equal(rowDetails({ status: 'FAILED', admission: 'QUEUED', error }), error.message);
});
