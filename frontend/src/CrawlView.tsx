import { useEffect, useRef, useState } from 'react';
import { CopyButton } from './CopyButton';
import { Icon } from './Icon';
import { readJson, responseError } from './api';
import { visiblePolling } from './visiblePolling';
import { crawlResult, preservedResultMessage, sourceChangeMessage } from './resultModel';
import { websiteLabel } from './workspaceModel';
import type { Crawl, Monitoring } from './resultModel';

const date = (value?: string) => (value ? new Date(value).toLocaleString() : 'Not available yet');
const words = (value: string) => value.toLowerCase().replaceAll('_', ' ');
const reasonText: Record<string, string> = {
  PRIMARY_NAVIGATION: 'Linked from primary navigation',
  SECONDARY_NAVIGATION: 'Linked from secondary navigation',
  MAIN_CONTENT: 'Linked from main content',
  SUBSTANTIAL_MAIN_CONTENT: 'Substantial main content',
  PREVIOUSLY_SELECTED: 'Previously selected resource revalidated',
  SITEMAP: 'Discovered through sitemap',
  SEED: 'Website overview',
};

export function CrawlView({ identityId, onBatch }: { identityId: string; onBatch: (batchId: string) => void }) {
  const [crawl, setCrawl] = useState<ReturnType<typeof crawlResult> | null>(null);
  const [text, setText] = useState<string | null>(null);
  const [readError, setReadError] = useState('');
  const [fileError, setFileError] = useState('');
  const [actionError, setActionError] = useState('');
  const [busy, setBusy] = useState<'enable' | 'disable' | 'refresh' | null>(null);
  const [reload, setReload] = useState(0);
  const title = useRef<HTMLHeadingElement>(null);
  const actionRequest = useRef<AbortController | null>(null);
  const base = `/api/crawls/${encodeURIComponent(identityId)}`;

  useEffect(() => {
    title.current?.focus({ preventScroll: true });
    window.scrollTo(0, 0);
    return () => {
      const controller = actionRequest.current;
      actionRequest.current = null;
      controller?.abort();
    };
  }, [identityId]);

  useEffect(() => {
    let hash: string | undefined;
    let stopped = false;
    const stop = visiblePolling(async (signal) => {
      try {
        const current = await readJson<Crawl>(base, signal);
        if (signal.aborted) return;
        setCrawl(crawlResult(current));
        setReadError('');
        if (current.artifactUrl && current.outputHash !== hash) {
          setText(null);
          setFileError('');
          try {
            const response = await fetch(current.artifactUrl, {
              signal,
              cache: 'no-store',
            });
            if (!response.ok) throw await responseError(response, 'The current file could not be loaded');
            const bytes = await response.text();
            if (signal.aborted) return;
            setText(bytes);
            hash = current.outputHash;
          } catch (failure) {
            if (!stopped && !document.hidden)
              setFileError(failure instanceof Error ? failure.message : 'The file could not be loaded.');
          }
        }
      } catch (failure) {
        if (!stopped && !document.hidden)
          setReadError(failure instanceof Error ? failure.message : 'The result could not be updated.');
      }
    });
    return () => {
      stopped = true;
      stop();
    };
  }, [base, reload]);

  async function control(action: 'enable' | 'disable' | 'refresh') {
    const controller = new AbortController();
    actionRequest.current = controller;
    const timeout = setTimeout(() => controller.abort(), 45_000);
    setBusy(action);
    setActionError('');
    try {
      const monitoringAction = action === 'enable' || action === 'disable';
      const body = await readJson<Monitoring | { batchId: string }>(
        base + (monitoringAction ? '/monitoring' : '/refresh'),
        controller.signal,
        {
          method: 'POST',
          ...(monitoringAction
            ? {
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled: action === 'enable' }),
              }
            : {}),
        },
      );
      if (controller.signal.aborted) return;
      if ('batchId' in body) onBatch(body.batchId);
      else setCrawl((current) => (current ? { ...current, monitoring: body } : current));
      setReload((value) => value + 1);
    } catch (failure) {
      if (!controller.signal.aborted)
        setActionError(failure instanceof Error ? failure.message : 'The change could not be confirmed.');
      else if (actionRequest.current === controller)
        setActionError('The request took too long. The change may already have been accepted.');
    } finally {
      clearTimeout(timeout);
      if (actionRequest.current === controller) {
        actionRequest.current = null;
        setBusy(null);
      }
    }
  }

  const monitoring = crawl?.monitoring;
  const attempt = monitoring?.latestAutomaticAttempt;
  const sourceChange = crawl ? sourceChangeMessage(crawl) : null;
  const preservedMessage = crawl ? preservedResultMessage(crawl) : null;

  return (
    <section className="crawl-workspace" aria-labelledby="crawl-title">
      <div className="section-heading detail-heading">
        <div>
          <span className="eyebrow">RESULT & MONITORING</span>
          <h1 id="crawl-title" ref={title} tabIndex={-1}>
            Website overview
          </h1>
          {crawl && (
            <>
              <p className="source-url">{crawl.normalizedSeedUrl}</p>
              {crawl.lastAttemptSummary && <p className="muted">Latest attempt: {words(crawl.lastAttemptSummary)}</p>}
            </>
          )}
        </div>
        {crawl?.artifactUrl && (
          <a className="button primary" href={crawl.artifactUrl} download>
            <Icon name="download" />
            Download current file
          </a>
        )}
      </div>
      {readError && (
        <p className="notice error" role="alert">
          {readError} We’ll retry automatically.
        </p>
      )}
      {!crawl ? (
        <div className="card empty-state" role="status">
          {readError ? (
            <>
              <Icon name="warning" />
              <h2>Result unavailable</h2>
              <button className="button secondary" type="button" onClick={() => setReload((value) => value + 1)}>
                Try again
              </button>
            </>
          ) : (
            <>
              <span className="spinner" />
              Loading website details…
            </>
          )}
        </div>
      ) : (
        <>
          {preservedMessage && (
            <div className="notice" role="status">
              <p>{preservedMessage}</p>
              <a href={crawl.artifactUrl} download>
                Download accepted file
              </a>
            </div>
          )}
          <dl className="result-facts">
            <div>
              <span className="fact-icon">
                <Icon name="check" />
              </span>
              <dt>Last successful check</dt>
              <dd>{date(crawl.lastSuccessfulValidationAt)}</dd>
            </div>
            <div>
              <span className="fact-icon">
                <Icon name="file" />
              </span>
              <dt>File last changed</dt>
              <dd>{date(crawl.lastOutputUpdatedAt)}</dd>
            </div>
            <div>
              <span className="fact-icon">
                <Icon name="clock" />
              </span>
              <dt>Next automatic check</dt>
              <dd>{monitoring?.enabled ? date(monitoring.nextCheckAt) : 'Monitoring inactive'}</dd>
            </div>
          </dl>
          <div className="detail-layout">
            <section className="card file-card" aria-labelledby="current-file-title">
              <div className="card-heading">
                <div className="file-heading">
                  <span className="file-icon">
                    <Icon name="file" />
                  </span>
                  <div>
                    <h2 id="current-file-title">Your llms.txt</h2>
                    <p className="muted">{websiteLabel(crawl.normalizedSeedUrl)}</p>
                  </div>
                </div>
                {crawl.artifactUrl && (
                  <span className="status-pill ready">
                    <span className="status-dot" />
                    Current file
                  </span>
                )}
              </div>
              {crawl.finalSeedUrl && crawl.finalSeedUrl !== crawl.normalizedSeedUrl && (
                <p className="muted source-url">Source redirected to {crawl.finalSeedUrl}</p>
              )}
              {crawl.artifactUrl ? (
                <>
                  {fileError && (
                    <div className="notice error" role="alert">
                      {fileError}
                      <button className="button ghost" type="button" onClick={() => setReload((value) => value + 1)}>
                        Try again
                      </button>
                    </div>
                  )}
                  {text === null ? (
                    <div className="empty-state" role="status">
                      {fileError ? (
                        'File preview is unavailable.'
                      ) : (
                        <>
                          <span className="spinner" />
                          Loading current file…
                        </>
                      )}
                    </div>
                  ) : (
                    <pre className="file-content" tabIndex={0}>
                      {text}
                    </pre>
                  )}
                  <div className="file-footer">
                    <CopyButton text={text ?? ''} disabled={text === null} />
                    <span className="muted">UTF-8 · Markdown</span>
                  </div>
                </>
              ) : (
                <div className="empty-state">
                  <span className="empty-file">
                    <Icon name="file" />
                  </span>
                  <h3>No accepted file yet</h3>
                  <p>
                    Your first file will appear here after a successful crawl. Check the batch for progress or failure
                    details.
                  </p>
                </div>
              )}
              <div className="publish-note">
                <Icon name="globe" />
                <p>
                  Make it yours. Download the file and publish it as <code>/llms.txt</code> on your website. This app
                  hosts a separate copy.
                </p>
              </div>
            </section>
            <aside className="card monitoring-card" aria-labelledby="monitoring-title">
              <span className="section-number">KEEP IT CURRENT</span>
              <div className="card-heading">
                <h2 id="monitoring-title">Automatic checks</h2>
                <span className={`status-pill ${monitoring?.enabled ? 'ready' : 'neutral'}`}>
                  {monitoring?.enabled ? 'Active' : 'Inactive'}
                </span>
              </div>
              <p className="muted">
                {monitoring?.enabled
                  ? 'We check for changes while your monitoring window is active. You can close this browser.'
                  : 'Automatic checks are off. An existing file stays available to download.'}
              </p>
              {monitoring?.expiresAt && (
                <div className="monitoring-window">
                  <Icon name="clock" />
                  <div>
                    <span>Monitoring window ends</span>
                    <strong>{date(monitoring.expiresAt)}</strong>
                  </div>
                </div>
              )}
              {monitoring?.state === 'NONE' && monitoring.canEnable && (
                <p className="muted">
                  No monitoring slot was reserved. You can enable checks if a slot becomes available before this window
                  ends.
                </p>
              )}
              {!crawl.artifactUrl && (
                <p className="muted">
                  Monitoring becomes available after the first accepted result, when capacity allows.
                </p>
              )}
              {monitoring?.state === 'EXPIRED' && (
                <p className="muted">This monitoring window has ended. You can still request a manual refresh.</p>
              )}
              {sourceChange && (
                <div className="source-change">
                  <Icon name="check" />
                  <p>{sourceChange}</p>
                </div>
              )}
              {monitoring && monitoring.failureCount > 0 && (
                <p className="notice">
                  The latest source check was incomplete. Your last accepted file is preserved; automatic retries use a
                  backoff.
                </p>
              )}
              {attempt && (
                <p className="muted">
                  Latest automatic attempt: {words(attempt.status)}
                  {attempt.publicationDecision && ` · ${words(attempt.publicationDecision)}`}
                  {attempt.error && ` · ${attempt.error.message}`} ·{' '}
                  {date(attempt.finishedAt ?? attempt.startedAt ?? attempt.queuedAt)}
                </p>
              )}
              <div className="monitoring-actions">
                <button
                  className="button secondary"
                  type="button"
                  disabled={busy !== null || !crawl.artifactUrl || (!monitoring?.enabled && !monitoring?.canEnable)}
                  onClick={() => void control(monitoring?.enabled ? 'disable' : 'enable')}
                >
                  {busy === 'enable' || busy === 'disable' ? <span className="spinner" /> : <Icon name="shield" />}
                  {monitoring?.enabled ? 'Disable monitoring' : 'Enable monitoring'}
                </button>
                <button
                  className="button primary"
                  type="button"
                  disabled={busy !== null}
                  onClick={() => void control('refresh')}
                >
                  {busy === 'refresh' ? <span className="spinner" /> : <Icon name="refresh" />}
                  {busy === 'refresh' ? 'Requesting refresh…' : 'Refresh now'}
                </button>
              </div>
              <p className="monitoring-footnote">
                Refresh requests a new check, even when a cached file is fresh. Existing work may be shared.
              </p>
              {actionError && (
                <p className="notice error" role="alert">
                  {actionError}
                </p>
              )}
            </aside>
          </div>
          <section className="card resources-card" aria-labelledby="resources-title">
            <div className="card-heading">
              <div>
                <span className="section-number">BEHIND THE FILE</span>
                <h2 id="resources-title">Selected resources</h2>
              </div>
              <span className="count-badge">{crawl.selectedResources.length} pages</span>
            </div>
            <p className="muted">Verified pages selected to represent this website.</p>
            {crawl.coverage && (
              <p className="coverage-note">
                {crawl.coverage.pagesUseful} useful pages from {crawl.coverage.pagesAttempted} attempts.
                {crawl.coverage.limitReasons.length > 0 &&
                  ` Limits reached: ${crawl.coverage.limitReasons.map(words).join(', ')}.`}
              </p>
            )}
            {crawl.warnings.length > 0 && <p className="notice">Warnings: {crawl.warnings.map(words).join(', ')}.</p>}
            {crawl.selectedResources.length === 0 ? (
              <p className="muted">Selected pages will appear after a result is accepted.</p>
            ) : (
              <ul className="resource-list">
                {crawl.selectedResources.map((resource, index) => (
                  <li key={resource.resourceKey}>
                    <span className="resource-number">{String(index + 1).padStart(2, '0')}</span>
                    <div>
                      <a href={resource.preferredLinkUrl} target="_blank" rel="noopener noreferrer">
                        {resource.title}
                        <Icon name="arrow" />
                      </a>
                      <p>{resource.description}</p>
                      <div className="resource-tags">
                        <span>{words(resource.category)}</span>
                        {resource.reasonCodes.slice(0, 3).map((reason) => (
                          <span key={reason}>{reasonText[reason] ?? words(reason)}</span>
                        ))}
                      </div>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </section>
        </>
      )}
    </section>
  );
}
