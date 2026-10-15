import { useEffect, useState } from 'react';
import { readJson } from './api';
import { batchStatusUrl } from './batchPage';
import type { Batch } from './workspaceModel';

export function useBatch(batchId: string | null, offset: number) {
  const [batch, setBatch] = useState<Batch | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    setError('');
    if (!batchId) return;
    let stopped = false;
    let terminal = false;
    let running = false;
    let refreshPending = false;
    let timer: ReturnType<typeof setTimeout>;
    let controller: AbortController | undefined;
    const started = Date.now();

    async function poll() {
      if (stopped || document.hidden || terminal) return;
      if (running) {
        refreshPending = true;
        return;
      }
      running = true;
      controller = new AbortController();
      const timeout = setTimeout(() => controller?.abort(), 15_000);
      try {
        const current = await readJson<Batch>(batchStatusUrl(batchId!, offset), controller.signal);
        if (!stopped && !controller.signal.aborted) {
          setBatch(current);
          setError('');
          terminal = current.status === 'COMPLETED';
        }
      } catch (failure) {
        if (!stopped && !document.hidden)
          setError(failure instanceof Error ? failure.message : 'Could not load batch status.');
      } finally {
        clearTimeout(timeout);
        running = false;
        if (!stopped && !terminal && !document.hidden) {
          timer = setTimeout(poll, refreshPending ? 0 : Date.now() - started > 30_000 ? 10_000 : 2_000);
        }
        refreshPending = false;
      }
    }

    function visibilityChanged() {
      clearTimeout(timer);
      if (document.hidden) controller?.abort();
      else {
        terminal = false;
        void poll();
      }
    }

    document.addEventListener('visibilitychange', visibilityChanged);
    void poll();
    return () => {
      stopped = true;
      clearTimeout(timer);
      controller?.abort();
      document.removeEventListener('visibilitychange', visibilityChanged);
    };
  }, [batchId, offset]);

  return { batch, setBatch, error, loading: batch?.batchId !== batchId || batch?.offset !== offset };
}
