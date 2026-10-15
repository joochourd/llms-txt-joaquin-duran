export const MAX_BATCH_URLS = 1_000;
export const RESULT_PAGE_SIZE = 100;

type DownloadRow = { status: string; artifactUrl?: string | null };

export function resultOffset(search: string): number {
  const raw = new URLSearchParams(search).get('offset');
  if (raw === null || !/^\d+$/.test(raw)) return 0;
  const offset = Number(raw);
  return Number.isSafeInteger(offset) && offset < MAX_BATCH_URLS ? offset : 0;
}

export function batchStatusUrl(batchId: string, offset: number): string {
  return `/api/batches/${encodeURIComponent(batchId)}?offset=${offset}&limit=${RESULT_PAGE_SIZE}`;
}

export function batchDownloadUrl(batchId: string, offset: number): string {
  return `/api/batches/${encodeURIComponent(batchId)}/download?offset=${offset}&limit=${RESULT_PAGE_SIZE}`;
}

export function canDownloadPage(rows: DownloadRow[]): boolean {
  return rows.length > 0
    && rows.every(row => ['COMPLETED', 'FAILED', 'TIMED_OUT', 'EXPIRED'].includes(row.status))
    && rows.some(row => Boolean(row.artifactUrl));
}
