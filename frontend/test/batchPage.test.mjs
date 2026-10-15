import { test } from 'node:test';
import assert from 'node:assert/strict';
import { batchStatusUrl, batchDownloadUrl, canDownloadPage, resultOffset } from '../src/batchPage.ts';

test('result and ZIP requests use the same bounded original-position range', () => {
  assert.equal(batchStatusUrl('batch/id', 200), '/api/batches/batch%2Fid?offset=200&limit=100');
  assert.equal(batchDownloadUrl('batch/id', 200), '/api/batches/batch%2Fid/download?offset=200&limit=100');
});

test('shared page locations reject malformed and out-of-range offsets', () => {
  assert.equal(resultOffset('?batch=some-id&offset=900'), 900);
  for (const search of ['', '?offset=-1', '?offset=1.5', '?offset=NaN', '?offset=1000']) {
    assert.equal(resultOffset(search), 0);
  }
});

test('a terminal page can be downloaded without waiting for the whole batch', () => {
  assert.equal(canDownloadPage([{ status: 'COMPLETED', artifactUrl: '/file' }, { status: 'FAILED' }]), true);
  assert.equal(canDownloadPage([{ status: 'EXPIRED', artifactUrl: '/current-file' }]), true);
  for (const status of ['PENDING', 'ADMITTING', 'QUEUED', 'RUNNING']) {
    assert.equal(canDownloadPage([{ status: 'COMPLETED', artifactUrl: '/file' }, { status }]), false);
  }
  assert.equal(canDownloadPage([{ status: 'FAILED' }]), false);
  assert.equal(canDownloadPage([]), false);
});
