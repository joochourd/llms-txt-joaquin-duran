type RowState = {
  status: string;
  admission: string | null;
  phase?: string;
  artifactUrl?: string;
  error?: { message: string };
};

/** Admission describes how work was obtained; phase describes only an executing run. */
export function rowDetails(row: RowState): string {
  const parts: string[] = [];
  if (row.admission === 'CACHED') parts.push('Cached result');
  if (row.admission === 'COALESCED') parts.push('Shared run');
  if (row.status === 'PENDING') parts.push('Saved · waiting for admission capacity');
  if (row.status === 'QUEUED') parts.push('Waiting to start');
  if (row.status === 'RUNNING' && row.phase) {
    const phases: Record<string, string> = {
      PREPARING: 'Preparing',
      DISCOVERING: 'Discovering pages',
      ANALYZING: 'Analyzing content',
      PUBLISHING: 'Publishing result',
    };
    parts.push(phases[row.phase] ?? 'Processing');
  }
  if (row.error) parts.push(row.error.message);
  if (row.artifactUrl && ['FAILED', 'TIMED_OUT'].includes(row.status))
    parts.push('An accepted file is still available');
  return parts.join(' · ');
}
