import { test } from 'node:test';
import assert from 'node:assert/strict';
import { filteredRows, resultGroup, statusLabel, websiteLabel } from '../src/workspaceModel.ts';
import { readRoute, routeUrl } from '../src/navigation.ts';
import { responseError } from '../src/api.ts';

test('status groups describe outcomes independently of cached files and shared work', () => {
  assert.equal(resultGroup({ status: 'COMPLETED', admission: 'COALESCED' }), 'ready');
  assert.equal(resultGroup({ status: 'RUNNING', admission: 'COALESCED' }), 'processing');
  for (const status of ['PENDING', 'ADMITTING', 'QUEUED']) assert.equal(resultGroup({ status }), 'processing');
  for (const status of ['FAILED', 'TIMED_OUT', 'EXPIRED']) {
    assert.equal(resultGroup({ status, artifactUrl: '/last-accepted-file' }), 'attention');
  }
  assert.equal(statusLabel('EXPIRED'), 'Details expired');
});

test('page filters preserve duplicate positions and combine status with case-insensitive URL search', () => {
  const rows = [
    { index: 100, status: 'COMPLETED', inputUrl: 'https://docs.example/Guide' },
    { index: 101, status: 'RUNNING', inputUrl: 'https://docs.example/Guide' },
    { index: 102, status: 'FAILED', inputUrl: 'https://other.example/' },
    { index: 103, status: 'COMPLETED', inputUrl: 'https://docs.example/Guide' },
  ];
  assert.deepEqual(
    filteredRows(rows, 'ready', ' GUIDE ').map((row) => row.index),
    [100, 103],
  );
  assert.deepEqual(
    filteredRows(rows, 'all', 'docs').map((row) => row.index),
    [100, 101, 103],
  );
  assert.deepEqual(
    filteredRows(rows, 'processing', '').map((row) => row.index),
    [101],
  );
  assert.deepEqual(filteredRows(rows, 'attention', 'docs'), []);
});

test('shared detail links preserve the batch page and encode opaque identifiers', () => {
  const route = { batchId: 'batch/id', identityId: 'crawl+id', offset: 200 };
  const url = routeUrl(route);
  assert.deepEqual(readRoute(new URL(url, 'http://localhost').search), route);
  assert.equal(routeUrl({ ...route, identityId: null }), '/?batch=batch%2Fid&offset=200');
  assert.deepEqual(readRoute('?crawl=known&offset=-1'), { batchId: null, identityId: 'known', offset: 0 });
  assert.equal(routeUrl({ batchId: null, identityId: null, offset: 0 }), '/');
});

test('website labels tolerate inputs that admission will reject without parsing them as markup', () => {
  assert.equal(websiteLabel('https://DOCS.example/guide?topic=one'), 'docs.example');
  assert.equal(websiteLabel('example.com/guide'), 'example.com');
  assert.equal(websiteLabel('<not a website>'), '<not a website>');
});

test('operation errors preserve useful API messages and tolerate non-JSON responses', async () => {
  const jsonError = new Response(JSON.stringify({ error: { message: 'Monitoring capacity is full.' } }), {
    status: 429,
  });
  assert.equal((await responseError(jsonError, 'Request failed')).message, 'Monitoring capacity is full.');
  assert.equal(
    (await responseError(new Response('proxy unavailable', { status: 502 }), 'Request failed')).message,
    'Request failed (502).',
  );
});
