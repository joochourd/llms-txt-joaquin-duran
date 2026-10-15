import { useEffect, useRef, useState } from 'react';
import { BatchResults } from './BatchResults';
import { CrawlView } from './CrawlView';
import { FilePreview } from './FilePreview';
import type { PreviewFile } from './FilePreview';
import { Icon } from './Icon';
import { MAX_BATCH_URLS } from './batchPage';
import { readJson } from './api';
import { readRoute, routeUrl } from './navigation';
import type { Route } from './navigation';
import { useBatch } from './useBatch';
import { useConnection } from './useConnection';
import type { Batch, Row } from './workspaceModel';

export function App() {
  const [route, setRoute] = useState(() => readRoute(window.location.search));
  const [urls, setUrls] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [submissionError, setSubmissionError] = useState('');
  const [preview, setPreview] = useState<PreviewFile | null>(null);
  const returnRow = useRef<number | null>(null);
  const connection = useConnection();
  const { batch, setBatch, error: batchError, loading } = useBatch(route.batchId, route.offset);
  const inputs = urls
    .split(/\r?\n/)
    .map((value) => value.trim())
    .filter(Boolean);

  function navigate(next: Route, replace = false) {
    const url = routeUrl(next);
    if (url !== window.location.pathname + window.location.search) {
      const state = { fromBatch: !route.identityId && !!next.identityId && !!route.batchId };
      if (replace) window.history.replaceState(state, '', url);
      else window.history.pushState(state, '', url);
    }
    setRoute(next);
  }

  useEffect(() => {
    function restoreLocation() {
      setPreview(null);
      setRoute(readRoute(window.location.search));
    }
    window.addEventListener('popstate', restoreLocation);
    return () => window.removeEventListener('popstate', restoreLocation);
  }, []);

  useEffect(() => {
    if (route.identityId || !route.batchId || loading) return;
    const target =
      returnRow.current === null
        ? document.getElementById('results-title')
        : document.getElementById(`result-details-${returnRow.current}`);
    target?.focus({ preventScroll: true });
    const scrollTarget = returnRow.current === null ? target?.closest('.results-workspace') : target;
    scrollTarget?.scrollIntoView({ block: returnRow.current === null ? 'start' : 'center' });
  }, [route.identityId, route.batchId, route.offset, loading]);

  function viewDetails(row: Row) {
    returnRow.current = row.index;
    navigate({ ...route, identityId: row.identityId! });
  }

  function backToResults() {
    if (window.history.state?.fromBatch) window.history.back();
    else navigate({ ...route, identityId: null }, true);
  }

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (inputs.length < 1 || inputs.length > MAX_BATCH_URLS) {
      setSubmissionError(`Enter between 1 and ${MAX_BATCH_URLS.toLocaleString()} URLs, one per line.`);
      return;
    }
    setSubmitting(true);
    setSubmissionError('');
    try {
      const current = await readJson<Batch>('/api/batches', AbortSignal.timeout(45_000), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ urls: inputs }),
      });
      returnRow.current = null;
      setBatch(current);
      navigate({ batchId: current.batchId, identityId: null, offset: 0 });
    } catch (failure) {
      setSubmissionError(
        failure instanceof Error
          ? failure.message
          : 'Submission could not be confirmed. Some work may already be saved.',
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="app-shell">
      <a className="skip-link" href="#main-content">
        Skip to content
      </a>
      <header className="header">
        <div className="header-inner">
          <a className="wordmark" href="/" aria-label="llms.txt Generator home">
            <span className="mark" aria-hidden="true">
              l/
            </span>
            <span>
              llms<span className="wordmark-light">.txt</span>
              <small>GENERATOR</small>
            </span>
          </a>
          <div className={`connection ${connection}`} role="status">
            <span className="status-dot" />
            {connection === 'ready'
              ? 'Service online'
              : connection === 'checking'
                ? 'Connecting…'
                : 'Service unavailable'}
          </div>
        </div>
      </header>
      <main className="shell" id="main-content">
        {route.identityId ? (
          <>
            <button className="button ghost back-link" type="button" onClick={backToResults}>
              <Icon name="back" />
              {route.batchId ? 'Back to results' : 'Back to generator'}
            </button>
            <CrawlView
              key={route.identityId}
              identityId={route.identityId}
              onBatch={(batchId) => {
                returnRow.current = null;
                navigate({ batchId, identityId: null, offset: 0 });
              }}
            />
          </>
        ) : (
          <>
            <section className="generator-layout" aria-labelledby="title">
              <div className="intro">
                <span className="eyebrow">
                  <span className="tiny-line" />A BETTER MAP OF YOUR WEBSITE
                </span>
                <h1 id="title">
                  Good content.
                  <br />A clearer <span>index.</span>
                </h1>
                <p>
                  Turn your public website into a useful <code>llms.txt</code>. We find key pages, organize what
                  matters, and give you a file ready to publish.
                </p>
                <div className="intro-features">
                  <span>
                    <Icon name="check" />
                    Verified source pages
                  </span>
                  <span>
                    <Icon name="shield" />
                    Respects robots.txt
                  </span>
                  <span>
                    <Icon name="refresh" />
                    Optional automatic checks
                  </span>
                </div>
                <div className="intro-caption">
                  <span className="caption-rule" />
                  One website or a thousand.
                  <br />
                  Each result moves at its own pace.
                </div>
              </div>
              <section className="card submission-card" aria-labelledby="submission-title">
                <div className="card-heading">
                  <span className="section-number">01 / GENERATE</span>
                  <Icon name="globe" />
                </div>
                <h2 id="submission-title">Start with your websites</h2>
                <p className="muted">Add one public URL per line. We’ll take it from here.</p>
                <form onSubmit={(event) => void submit(event)}>
                  <div className="input-label">
                    <label htmlFor="urls">Website URLs</label>
                    <span className={inputs.length > MAX_BATCH_URLS ? 'error' : 'muted'}>
                      {inputs.length.toLocaleString()} / 1,000
                    </span>
                  </div>
                  <textarea
                    id="urls"
                    value={urls}
                    onChange={(event) => setUrls(event.target.value)}
                    rows={6}
                    placeholder={'https://your-website.com\nhttps://docs.your-website.com'}
                    aria-describedby="submission-note"
                    required
                    spellCheck={false}
                  />
                  <p className="input-help" id="submission-note">
                    <Icon name="clock" />
                    Large batches are saved first, then processed as capacity becomes available.
                  </p>
                  <button
                    className="button primary generate-button"
                    type="submit"
                    disabled={
                      submitting || connection !== 'ready' || inputs.length === 0 || inputs.length > MAX_BATCH_URLS
                    }
                  >
                    {submitting ? (
                      <>
                        <span className="spinner" />
                        Adding websites…
                      </>
                    ) : (
                      <>
                        Generate llms.txt
                        <Icon name="arrow" />
                      </>
                    )}
                  </button>
                  {submissionError && (
                    <p className="notice error" role="alert">
                      {submissionError}
                    </p>
                  )}
                </form>
                <p className="submission-footnote">No account needed. Download your files whenever they’re ready.</p>
              </section>
            </section>
            {route.batchId &&
              (batch && batch.batchId === route.batchId ? (
                <BatchResults
                  batch={batch}
                  loading={loading}
                  error={batchError}
                  onPage={(offset) => {
                    returnRow.current = null;
                    navigate({ ...route, offset });
                  }}
                  onDetails={viewDetails}
                  onPreview={setPreview}
                />
              ) : (
                <section className="card empty-state" role={batchError ? 'alert' : 'status'}>
                  {batchError ? (
                    <>
                      <Icon name="warning" />
                      <p>{batchError}</p>
                      <p className="muted">We’ll retry automatically.</p>
                    </>
                  ) : (
                    <>
                      <span className="spinner" />
                      <p>Loading your results…</p>
                    </>
                  )}
                </section>
              ))}
          </>
        )}
      </main>
      <footer className="footer">
        <span>
          llms.txt Generator <span className="footer-divider">/</span> A clearer path to your content.
        </span>
        <span>Generated from public sources. Hosted here, published by you.</span>
      </footer>
      {preview && <FilePreview key={preview.url} file={preview} onClose={() => setPreview(null)} />}
    </div>
  );
}
