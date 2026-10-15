export type Row = {
  index: number;
  inputUrl: string;
  status: string;
  admission: string | null;
  identityId?: string;
  phase?: string;
  artifactUrl?: string;
  error?: { code: string; message: string };
};

export type Batch = {
  batchId: string;
  status: string;
  items: Row[];
  totalItems: number;
  offset: number;
  admittedItems: number;
  nextOffset?: number | null;
};

export type ResultFilter = 'all' | 'ready' | 'processing' | 'attention';

export function resultGroup(row: Pick<Row, 'status'>): Exclude<ResultFilter, 'all'> {
  if (row.status === 'COMPLETED') return 'ready';
  if (['FAILED', 'TIMED_OUT', 'EXPIRED'].includes(row.status)) return 'attention';
  return 'processing';
}

export function statusLabel(status: string): string {
  const labels: Record<string, string> = {
    PENDING: 'Pending',
    ADMITTING: 'Pending',
    QUEUED: 'Queued',
    RUNNING: 'Processing',
    COMPLETED: 'Ready',
    FAILED: 'Failed',
    TIMED_OUT: 'Timed out',
    EXPIRED: 'Details expired',
  };
  return labels[status] ?? status.toLowerCase().replaceAll('_', ' ');
}

export function filteredRows(rows: Row[], filter: ResultFilter, search: string): Row[] {
  const query = search.trim().toLowerCase();
  return rows.filter(
    (row) => (filter === 'all' || resultGroup(row) === filter) && row.inputUrl.toLowerCase().includes(query),
  );
}

export function websiteLabel(input: string): string {
  try {
    const url = new URL(input.includes('://') ? input : `https://${input.trim()}`);
    return url.host;
  } catch {
    return input;
  }
}
