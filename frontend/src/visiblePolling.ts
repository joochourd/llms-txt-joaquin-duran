type Visibility = { hidden: boolean; addEventListener: (event: string, listener: () => void) => void; removeEventListener: (event: string, listener: () => void) => void };
type PollOptions = { visibility?: Visibility; intervalMs?: number; schedule?: typeof setTimeout; cancel?: typeof clearTimeout };

/** Result monitoring continues after batch completion, and never overlaps its own requests. */
export function visiblePolling(task: (signal: AbortSignal) => Promise<void>, options: PollOptions = {}): () => void {
  const visibility = options.visibility ?? document;
  const schedule = options.schedule ?? setTimeout;
  const cancel = options.cancel ?? clearTimeout;
  const interval = options.intervalMs ?? 15_000;
  let stopped = false;
  let running = false;
  let refreshPending = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let controller: AbortController | undefined;

  async function poll() {
    if (stopped || visibility.hidden) return;
    if (running) { refreshPending = true; return; }
    running = true;
    controller = new AbortController();
    const timeout = schedule(() => controller?.abort(), 15_000);
    try {
      await task(controller.signal);
    } catch {
      // The task owns error presentation; a failed read must not stop future monitoring.
    } finally {
      cancel(timeout);
      running = false;
      if (!stopped && !visibility.hidden) {
        timer = schedule(() => { void poll(); }, refreshPending ? 0 : interval);
      }
      refreshPending = false;
    }
  }

  function changed() {
    cancel(timer);
    if (visibility.hidden) controller?.abort();
    else void poll();
  }
  visibility.addEventListener('visibilitychange', changed);
  void poll();
  return () => {
    stopped = true;
    cancel(timer);
    controller?.abort();
    visibility.removeEventListener('visibilitychange', changed);
  };
}
