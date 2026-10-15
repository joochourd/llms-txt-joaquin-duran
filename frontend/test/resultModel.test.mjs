import { test } from 'node:test';
import assert from 'node:assert/strict';
import { crawlResult, preservedResultMessage, sourceChangeMessage } from '../src/resultModel.ts';

test('a queued result with omitted collections stays renderable before first acceptance', () => {
  const crawl = crawlResult({
    identityId: 'queued',
    acceptedObservationSequence: 0,
    monitoring: { state: 'RESERVED' },
  });
  assert.equal(crawl.selectedResources.length, 0);
  assert.equal(crawl.warnings.length, 0);
  assert.equal(crawl.coverage, undefined);
});

test('accepted coverage tolerates empty collections omitted by serialization', () => {
  const resource = { resourceKey: 'selected', title: 'Project' };
  const crawl = crawlResult({
    selectedResources: [resource],
    coverage: { pagesAttempted: 3, pagesUseful: 3 },
  });
  assert.deepEqual(crawl.selectedResources, [resource]);
  assert.deepEqual(crawl.coverage.limitReasons, []);
});

test('current results preserve source warnings and finite monitoring controls', () => {
  const monitoring = { state: 'EXPIRED', enabled: false, canEnable: false };
  const crawl = crawlResult({
    monitoring,
    warnings: ['SITEMAP_INVALID'],
    selectedResources: [],
  });
  assert.deepEqual(crawl.warnings, ['SITEMAP_INVALID']);
  assert.equal(crawl.monitoring, monitoring);
  assert.deepEqual(crawl.selectedResources, []);
});

test('an accepted file without a monitoring slot retains its original enable window', () => {
  const monitoring = {
    state: 'NONE',
    enabled: false,
    canEnable: true,
    expiresAt: '2026-10-22T12:00:00.000Z',
  };
  const crawl = crawlResult({
    artifactUrl: '/artifacts/site/llms.txt',
    monitoring,
  });

  assert.equal(crawl.artifactUrl, '/artifacts/site/llms.txt');
  assert.equal(crawl.monitoring.enabled, false);
  assert.equal(crawl.monitoring.canEnable, true);
  assert.equal(crawl.monitoring.expiresAt, monitoring.expiresAt);
});

test('source changes are distinct from generated file changes without retaining history', () => {
  const accepted = '2026-10-15T12:00:00.000Z';
  const previousFile = '2026-10-15T11:00:00.000Z';
  assert.equal(
    sourceChangeMessage({
      sourceChanged: true,
      lastSuccessfulValidationAt: accepted,
      lastOutputUpdatedAt: previousFile,
    }),
    'The selected website pages changed. The generated llms.txt text stayed the same.',
  );
  assert.equal(
    sourceChangeMessage({
      sourceChanged: true,
      lastSuccessfulValidationAt: accepted,
      lastOutputUpdatedAt: accepted,
    }),
    'The latest check detected changes in the selected website pages.',
  );
  assert.equal(
    sourceChangeMessage({ sourceChanged: false }),
    'The selected website pages were unchanged at the latest check.',
  );
  assert.equal(sourceChangeMessage({}), null);
});

test('an unsuccessful update explains preserved downloads without inventing a previous result', () => {
  assert.match(
    preservedResultMessage({
      artifactUrl: '/artifacts/site/llms.txt',
      lastAttemptSummary: 'PRESERVED',
    }),
    /previously accepted file/,
  );
  assert.equal(preservedResultMessage({ lastAttemptSummary: 'PRESERVED' }), null);
  assert.equal(
    preservedResultMessage({
      artifactUrl: '/artifacts/site/llms.txt',
      lastAttemptSummary: 'UNCHANGED',
    }),
    null,
  );
});
