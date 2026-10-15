import { expect, test } from '@playwright/test';
import type { Page } from '@playwright/test';

const fileText =
  '# Example documentation\n\n## Guides\n\n- [Getting started](https://site-0.example/start): Start here.\n';
const artifact = '/artifacts/site-0/llms.txt';

function batchPage(offset = 0) {
  const count = offset === 0 ? 100 : 50;
  return {
    batchId: 'workspace',
    status: 'PROCESSING',
    totalItems: 150,
    admittedItems: 150,
    offset,
    nextOffset: offset === 0 ? 100 : null,
    items: Array.from({ length: count }, (_, position) => {
      const index = offset + position;
      const ready = index < 2 || offset > 0;
      return {
        index,
        inputUrl: `https://site-${index}.example/`,
        identityId: `site-${index}`,
        status: ready ? 'COMPLETED' : 'RUNNING',
        admission: ready ? 'CACHED' : 'QUEUED',
        phase: ready ? undefined : 'DISCOVERING',
        artifactUrl: ready ? `/artifacts/site-${index}/llms.txt` : undefined,
      };
    }),
  };
}

function currentResult() {
  return {
    identityId: 'site-0',
    normalizedSeedUrl: 'https://site-0.example/',
    acceptedObservationSequence: 2,
    currentVersionSequence: 1,
    outputHash: 'current-file',
    artifactUrl: artifact,
    sourceChanged: false,
    lastSuccessfulValidationAt: '2026-10-15T12:00:00.000Z',
    lastOutputUpdatedAt: '2026-10-15T11:00:00.000Z',
    monitoring: {
      state: 'ACTIVE',
      enabled: true,
      canEnable: true,
      expiresAt: '2026-10-22T12:00:00.000Z',
      nextCheckAt: '2026-10-16T12:00:00.000Z',
      failureCount: 0,
    },
    coverage: { pagesAttempted: 12, pagesUseful: 10, limitReasons: [] },
    selectedResources: [
      {
        resourceKey: 'start',
        title: 'Getting started',
        description: 'Install and configure the project.',
        preferredLinkUrl: 'https://site-0.example/start',
        category: 'DOCUMENTATION',
        reasonCodes: ['PRIMARY_NAVIGATION'],
      },
    ],
  };
}

async function mockService(page: Page, totalItems = 150) {
  const reads = { batches: 0, results: 0 };
  await page.route('**/health/readiness', (route) => route.fulfill({ json: { status: 'UP' } }));
  await page.route('**/api/batches/workspace?**', (route) => {
    reads.batches++;
    const offset = Number(new URL(route.request().url()).searchParams.get('offset') ?? 0);
    return route.fulfill({ json: { ...batchPage(offset), totalItems } });
  });
  await page.route('**/api/crawls/site-*', (route) => {
    reads.results++;
    return route.fulfill({ json: currentResult() });
  });
  await page.route('**/artifacts/**', (route) => route.fulfill({ contentType: 'text/plain', body: fileText }));
  return reads;
}

test('preview opens immediately in a modal while the batch continues polling', async ({ page }) => {
  const reads = await mockService(page);
  let releaseFile!: () => void;
  const pendingFile = new Promise<void>((resolve) => {
    releaseFile = resolve;
  });
  await page.route(`**${artifact}`, async (route) => {
    await pendingFile;
    await route.fulfill({ contentType: 'text/plain', body: fileText });
  });
  await page.goto('/?batch=workspace');
  const previewButton = page.getByRole('button', {
    name: 'Preview https://site-0.example/',
    exact: true,
  });
  await previewButton.click();

  const dialog = page.getByRole('dialog', { name: 'llms.txt preview' });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByText('Loading your file…', { exact: true })).toBeVisible();
  await expect.poll(() => reads.batches).toBeGreaterThan(1);
  releaseFile();
  await expect(dialog.locator('pre')).toHaveText(fileText);
  await page.keyboard.press('Escape');
  await expect(dialog).not.toBeVisible();
  await expect(previewButton).toBeFocused();
});

test('preview errors survive successful batch polls and can be retried', async ({ page }) => {
  const reads = await mockService(page);
  let fail = true;
  await page.route(`**${artifact}`, (route) =>
    fail
      ? route.fulfill({
          status: 503,
          json: { message: 'The file is temporarily unavailable.' },
        })
      : route.fulfill({ contentType: 'text/plain', body: fileText }),
  );
  await page.goto('/?batch=workspace');
  await page
    .getByRole('button', {
      name: 'Preview https://site-0.example/',
      exact: true,
    })
    .click();

  const dialog = page.getByRole('dialog');
  await expect(dialog.getByRole('alert')).toContainText('temporarily unavailable');
  const before = reads.batches;
  await expect.poll(() => reads.batches).toBeGreaterThan(before);
  await expect(dialog.getByRole('alert')).toContainText('temporarily unavailable');
  fail = false;
  await dialog.getByRole('button', { name: 'Try again' }).click();
  await expect(dialog.locator('pre')).toHaveText(fileText);
});

test('closing a pending preview prevents its late response from replacing another file', async ({ page }) => {
  await mockService(page);
  let releaseFile!: () => void;
  const pendingFile = new Promise<void>((resolve) => {
    releaseFile = resolve;
  });
  await page.route(`**${artifact}`, async (route) => {
    await pendingFile;
    await route.fulfill({ contentType: 'text/plain', body: '# Old file\n' }).catch(() => {});
  });
  await page.goto('/?batch=workspace');
  await page
    .getByRole('button', {
      name: 'Preview https://site-0.example/',
      exact: true,
    })
    .click();
  await expect(page.getByRole('dialog')).toBeVisible();
  await page.getByRole('button', { name: 'Close preview' }).click();
  await page
    .getByRole('button', {
      name: 'Preview https://site-1.example/',
      exact: true,
    })
    .click();
  releaseFile();

  await expect(page.getByRole('dialog').locator('pre')).toHaveText(fileText);
});

test('details open as a dedicated view and browser back restores the batch and focus', async ({ page }) => {
  await mockService(page);
  await page.goto('/?batch=workspace');
  const details = page.getByRole('button', {
    name: 'View details for https://site-0.example/',
    exact: true,
  });
  await details.click();

  await expect(page.getByRole('heading', { name: 'Website overview' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Your results' })).not.toBeVisible();
  await expect(page.getByRole('heading', { name: 'Website overview' })).toBeFocused();
  await expect(page.getByText('Getting started', { exact: true })).toBeVisible();
  await expect(page).toHaveURL(/crawl=site-0/);
  await page.goBack();
  await expect(page.getByRole('heading', { name: 'Your results' })).toBeVisible();
  await expect(details).toBeFocused();
  await page.goForward();
  await expect(page.getByRole('heading', { name: 'Website overview' })).toBeVisible();
  await page.reload();
  await expect(page.getByRole('heading', { name: 'Website overview' })).toBeVisible();
  await page.getByRole('button', { name: 'Back to results' }).click();
  await expect(page).not.toHaveURL(/crawl=/);
});

test('a rejected refresh stays visible across successful monitoring polls', async ({ page }) => {
  const reads = await mockService(page);
  await page.route('**/api/crawls/site-0/refresh', (route) =>
    route.fulfill({
      status: 429,
      json: {
        message: 'A forced run was recently created. Please retry later.',
      },
    }),
  );
  await page.clock.install();
  await page.goto('/?batch=workspace&crawl=site-0');
  await page.getByRole('button', { name: 'Refresh now' }).click();
  await expect(page.getByRole('alert')).toContainText('recently created');
  const before = reads.results;
  await page.clock.fastForward(16_000);
  await expect.poll(() => reads.results).toBeGreaterThan(before);
  await expect(page.getByRole('alert')).toContainText('recently created');
});

test('page filters do not block details and a finished page downloads before the batch finishes', async ({ page }) => {
  await mockService(page);
  await page.route('**/api/batches/workspace/download?**', (route) =>
    route.fulfill({
      contentType: 'application/zip',
      headers: {
        'Content-Disposition': 'attachment; filename="results-101-150.zip"',
      },
      body: Buffer.from('controlled archive fixture'),
    }),
  );
  await page.goto('/?batch=workspace');
  await page.getByRole('button', { name: 'Ready 2', exact: true }).click();
  await expect(page.getByTestId('result-row')).toHaveCount(2);
  await page.getByRole('searchbox', { name: 'Search this page' }).fill('site-1.example');
  await expect(page.getByTestId('result-row')).toHaveCount(1);
  await expect(page.getByRole('button', { name: 'Download page ZIP' })).toBeDisabled();
  await page
    .getByRole('navigation', { name: 'Result pages (top)' })
    .getByRole('button', { name: 'Next page', exact: true })
    .click();
  await expect(page.getByText('Showing 101–150 of 150')).toBeVisible();
  await expect(page.getByRole('searchbox')).toHaveValue('');
  const download = page.waitForEvent('download');
  await page.getByRole('button', { name: 'Download page ZIP' }).click();
  expect((await download).suggestedFilename()).toBe('results-101-150.zip');
});

test('a site without its first accepted file has an explicit empty state', async ({ page }) => {
  await mockService(page);
  await page.route('**/api/crawls/site-2', (route) =>
    route.fulfill({
      json: {
        identityId: 'site-2',
        normalizedSeedUrl: 'https://site-2.example/',
        acceptedObservationSequence: 0,
        monitoring: {
          state: 'RESERVED',
          enabled: false,
          canEnable: false,
          failureCount: 0,
        },
      },
    }),
  );
  await page.goto('/?batch=workspace&crawl=site-2');

  await expect(page.getByText('No accepted file yet', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Enable monitoring' })).toBeDisabled();
  await expect(page.getByRole('link', { name: 'Download current file' })).toHaveCount(0);
});

test('the input and result workspace fit the viewport without horizontal overflow', async ({ page }) => {
  await mockService(page);
  await page.goto('/?batch=workspace');
  await expect(page.getByRole('heading', { name: 'Your results' })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page
    .getByRole('button', {
      name: 'Preview https://site-0.example/',
      exact: true,
    })
    .click();
  await expect(page.getByRole('dialog')).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.getByRole('button', { name: 'Close preview' }).click();
  await page
    .getByRole('button', {
      name: 'View details for https://site-0.example/',
      exact: true,
    })
    .click();
  await expect(page.getByRole('heading', { name: 'Website overview' })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test('a 250-URL submission preserves inputs and immediately opens its tracking workspace', async ({ page }) => {
  await mockService(page, 250);
  let submitted: unknown;
  await page.route('**/api/batches', async (route) => {
    submitted = route.request().postDataJSON();
    await route.fulfill({
      status: 202,
      json: { ...batchPage(), totalItems: 250 },
    });
  });
  const inputs = Array.from({ length: 250 }, (_, index) => `https://input-${index}.example/`);
  await page.goto('/');
  await expect(page.getByRole('status')).toContainText('Service online');
  await page.getByRole('textbox', { name: 'Website URLs' }).fill(inputs.join('\n'));
  await page.getByRole('button', { name: 'Generate llms.txt' }).click();

  await expect(page.getByRole('heading', { name: 'Your results' })).toBeVisible();
  expect(submitted).toEqual({ urls: inputs });
  await expect(page).toHaveURL(/batch=workspace/);
  await page
    .getByRole('button', {
      name: 'Preview https://site-0.example/',
      exact: true,
    })
    .click();
  await expect(page.getByRole('dialog').locator('pre')).toHaveText(fileText);
});

test('monitoring toggles explicitly and a successful refresh opens asynchronous tracking', async ({ page }) => {
  await mockService(page);
  let monitoring = currentResult().monitoring;
  await page.route('**/api/crawls/site-0', (route) => route.fulfill({ json: { ...currentResult(), monitoring } }));
  await page.route('**/api/crawls/site-0/monitoring', async (route) => {
    expect(route.request().postDataJSON()).toEqual({ enabled: false });
    monitoring = { ...monitoring, enabled: false, state: 'NONE' };
    await route.fulfill({ json: monitoring });
  });
  await page.route('**/api/crawls/site-0/refresh', (route) =>
    route.fulfill({ status: 202, json: { batchId: 'workspace' } }),
  );
  await page.goto('/?batch=workspace&crawl=site-0');
  await page.getByRole('button', { name: 'Disable monitoring' }).click();
  await expect(page.getByRole('button', { name: 'Enable monitoring' })).toBeEnabled();
  await page.getByRole('button', { name: 'Refresh now' }).click();

  await expect(page.getByRole('heading', { name: 'Your results' })).toBeVisible();
  await expect(page).not.toHaveURL(/crawl=/);
});

test('an unsuccessful update retains preview and explains its accepted download in the result view', async ({
  page,
}) => {
  await mockService(page);
  await page.route('**/api/crawls/site-0', (route) =>
    route.fulfill({
      json: { ...currentResult(), lastAttemptSummary: 'PRESERVED' },
    }),
  );
  await page.goto('/?batch=workspace&crawl=site-0');

  await expect(page.getByText('An update could not be published.', { exact: false })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Download accepted file', exact: true })).toHaveAttribute(
    'href',
    artifact,
  );
  await expect(page.locator('.file-content')).toHaveText(fileText);
});

test('connection checks pause in hidden tabs and resume immediately when visible', async ({ page }) => {
  await page.clock.install();
  let healthReads = 0;
  await page.route('**/health/readiness', (route) => {
    healthReads++;
    return route.fulfill({ json: { status: 'UP' } });
  });
  await page.goto('/');
  await expect.poll(() => healthReads).toBe(1);
  await page.evaluate(() => {
    Object.defineProperty(document, 'hidden', {
      configurable: true,
      value: true,
    });
    document.dispatchEvent(new Event('visibilitychange'));
  });
  await page.clock.fastForward(30_000);
  expect(healthReads).toBe(1);

  await page.evaluate(() => {
    Object.defineProperty(document, 'hidden', {
      configurable: true,
      value: false,
    });
    document.dispatchEvent(new Event('visibilitychange'));
  });

  await expect.poll(() => healthReads).toBe(2);
});
