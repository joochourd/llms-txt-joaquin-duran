import { useEffect, useState } from 'react';
import { visiblePolling } from './visiblePolling';

export function useConnection() {
  const [connection, setConnection] = useState<'checking' | 'ready' | 'offline'>('checking');

  useEffect(
    () =>
      visiblePolling(
        async (signal) => {
          try {
            const response = await fetch('/health/readiness', {
              signal,
              cache: 'no-store',
            });
            const body = await response.json();
            if (!signal.aborted) setConnection(response.ok && body.status === 'UP' ? 'ready' : 'offline');
          } catch {
            if (!signal.aborted) setConnection('offline');
          }
        },
        { intervalMs: 10_000 },
      ),
    [],
  );

  return connection;
}
