import { useEffect, useState } from 'react';
import { batchDownloadUrl, canDownloadPage, RESULT_PAGE_SIZE } from './batchPage';
import { responseError } from './api';
import { Icon } from './Icon';
import { rowDetails } from './rowPresentation';
import { filteredRows, resultGroup, statusLabel, websiteLabel } from './workspaceModel';
import type { Batch, ResultFilter, Row } from './workspaceModel';
import type { PreviewFile } from './FilePreview';

type Props = {
  batch: Batch;
  loading: boolean;
  error: string;
  onPage: (offset: number) => void;
  onPreview: (file: PreviewFile) => void;
  onDetails: (row: Row) => void;
};

function PageNavigation({
  batch,
  loading,
  onPage,
  position,
}: Pick<Props, 'batch' | 'loading' | 'onPage'> & { position: string }) {
  return (
    <nav className="result-actions" aria-label={`Result pages (${position})`}>
      <button
        className="button secondary icon-button"
        type="button"
        disabled={loading || batch.offset === 0}
        aria-label="Previous page"
        onClick={() => onPage(Math.max(0, batch.offset - RESULT_PAGE_SIZE))}
      >
        <Icon name="back" />
      </button>
      <span className="page-number">
        {Math.floor(batch.offset / RESULT_PAGE_SIZE) + 1} / {Math.ceil(batch.totalItems / RESULT_PAGE_SIZE)}
      </span>
      <button
        className="button secondary icon-button"
        type="button"
        disabled={loading || batch.nextOffset == null}
        aria-label="Next page"
        onClick={() => onPage(batch.nextOffset ?? 0)}
      >
        <Icon name="arrow" />
      </button>
    </nav>
  );
}

export function BatchResults({ batch, loading, error, onPage, onPreview, onDetails }: Props) {
  const [filter, setFilter] = useState<ResultFilter>('all');
  const [search, setSearch] = useState('');
  const [downloading, setDownloading] = useState(false);
  const [downloadError, setDownloadError] = useState('');

  useEffect(() => {
    setFilter('all');
    setSearch('');
    setDownloadError('');
  }, [batch.batchId, batch.offset]);

  const counts = { all: batch.items.length, ready: 0, processing: 0, attention: 0 };
  batch.items.forEach((row) => {
    counts[resultGroup(row)]++;
  });
  const rows = filteredRows(batch.items, filter, search);
  const downloadReady = !loading && canDownloadPage(batch.items);
  const filters: { value: ResultFilter; label: string }[] = [
    { value: 'all', label: 'All' },
    { value: 'ready', label: 'Ready' },
    { value: 'processing', label: 'In progress' },
    { value: 'attention', label: 'Needs attention' },
  ];

  async function downloadPage() {
    setDownloading(true);
    setDownloadError('');
    try {
      const response = await fetch(batchDownloadUrl(batch.batchId, batch.offset), {
        signal: AbortSignal.timeout(45_000),
        cache: 'no-store',
      });
      if (!response.ok) throw await responseError(response, 'The download could not be prepared');
      const url = URL.createObjectURL(await response.blob());
      const link = document.createElement('a');
      link.href = url;
      link.download = response.headers.get('Content-Disposition')?.match(/filename="([^"]+)"/)?.[1] ?? 'llms-batch.zip';
      document.body.append(link);
      link.click();
      link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1_000);
    } catch (failure) {
      setDownloadError(failure instanceof Error ? failure.message : 'The download could not be prepared.');
    } finally {
      setDownloading(false);
    }
  }

  return (
    <section className="results-workspace" aria-labelledby="results-title">
      <div className="section-heading">
        <div>
          <span className="eyebrow">YOUR WORKSPACE</span>
          <h2 id="results-title" tabIndex={-1}>
            Your results
          </h2>
          <p className="muted">
            {batch.totalItems.toLocaleString()} websites ·{' '}
            {batch.status === 'COMPLETED'
              ? 'Batch finished. Review each website’s outcome below.'
              : 'Processing continues in the background. Open any available result.'}
          </p>
        </div>
        <div className="result-header-actions">
          <span className={`status-pill ${batch.status === 'COMPLETED' ? 'ready' : 'processing'}`}>
            <span className="status-dot" />
            {batch.status === 'COMPLETED' ? 'Finished' : 'Updating automatically'}
          </span>
          <PageNavigation batch={batch} loading={loading} onPage={onPage} position="top" />
        </div>
      </div>
      <div className="card results-card">
        <div className="results-toolbar">
          <div>
            <p className="toolbar-label">On this page</p>
            <div className="filter-tabs" aria-label="Filter results on this page">
              {filters.map((option) => (
                <button
                  type="button"
                  key={option.value}
                  aria-pressed={filter === option.value}
                  className={`filter-tab ${filter === option.value ? 'selected' : ''}`}
                  onClick={() => setFilter(option.value)}
                >
                  {option.label} <span>{counts[option.value]}</span>
                </button>
              ))}
            </div>
          </div>
          <label className="search-field">
            <Icon name="search" />
            <span className="sr-only">Search this page</span>
            <input
              type="search"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Find a website…"
            />
          </label>
        </div>
        {error && (
          <p className="notice error" role="alert">
            {error} Your last loaded results remain visible. We’ll retry automatically.
          </p>
        )}
        {loading ? (
          <div className="empty-state" role="status">
            <span className="spinner" />
            Loading results…
          </div>
        ) : rows.length === 0 ? (
          <div className="empty-state">
            <Icon name="search" />
            <h3>No matching websites</h3>
            <p>Try another search or choose a different status on this page.</p>
            <button
              className="button secondary"
              type="button"
              onClick={() => {
                setFilter('all');
                setSearch('');
              }}
            >
              Show all websites
            </button>
          </div>
        ) : (
          <ul className="result-list">
            {rows.map((row) => {
              const group = resultGroup(row);
              return (
                <li key={row.index} id={`result-row-${row.index}`} data-testid="result-row" className="result-row">
                  <span className={`website-icon ${group}`}>
                    <Icon name={group === 'ready' ? 'check' : group === 'attention' ? 'warning' : 'globe'} />
                  </span>
                  <div className="result-summary">
                    <div className="result-name">
                      <strong>{websiteLabel(row.inputUrl)}</strong>
                      <span className={`status-pill ${group}`}>
                        <span className="status-dot" />
                        {statusLabel(row.status)}
                      </span>
                    </div>
                    <p className="input-url" title={row.inputUrl}>
                      {row.inputUrl}
                    </p>
                    {rowDetails(row) && (
                      <p className={`result-explanation ${row.error ? 'error' : ''}`}>{rowDetails(row)}</p>
                    )}
                  </div>
                  <div className="result-actions row-actions">
                    {row.artifactUrl && (
                      <>
                        <button
                          type="button"
                          className="button ghost"
                          aria-label={`Preview ${row.inputUrl}`}
                          onClick={() => onPreview({ url: row.artifactUrl!, website: row.inputUrl })}
                        >
                          <Icon name="eye" />
                          Preview
                        </button>
                        <a
                          className="button icon-button ghost"
                          href={row.artifactUrl}
                          download
                          aria-label={`Download llms.txt for ${row.inputUrl}`}
                        >
                          <Icon name="download" />
                        </a>
                      </>
                    )}
                    {row.identityId && (
                      <button
                        type="button"
                        className="button secondary"
                        id={`result-details-${row.index}`}
                        aria-label={`View details for ${row.inputUrl}`}
                        onClick={() => onDetails(row)}
                      >
                        View details
                        <Icon name="arrow" />
                      </button>
                    )}
                  </div>
                </li>
              );
            })}
          </ul>
        )}
        <div className="results-pagination">
          <p>
            Showing {batch.offset + 1}–{Math.min(batch.offset + batch.items.length, batch.totalItems)} of{' '}
            {batch.totalItems}
          </p>
          <PageNavigation batch={batch} loading={loading} onPage={onPage} position="bottom" />
        </div>
      </div>
      <div className="download-strip">
        <span className="file-icon">
          <Icon name="download" />
        </span>
        <div className="download-description">
          <strong>Take your files with you</strong>
          <p>
            {downloadReady
              ? 'This page is ready to download. The ZIP includes current files and a summary of every row.'
              : counts.processing > 0
                ? 'The ZIP unlocks when every website on this page finishes. Individual files are available as they become ready.'
                : 'There are no generated files to download on this page.'}
          </p>
        </div>
        <button
          className="button primary"
          type="button"
          disabled={downloading || !downloadReady}
          onClick={() => void downloadPage()}
        >
          {downloading ? <span className="spinner" /> : <Icon name="download" />}
          {downloading ? 'Preparing ZIP…' : 'Download page ZIP'}
        </button>
      </div>
      {downloadError && (
        <p className="notice error" role="alert">
          {downloadError}
        </p>
      )}
    </section>
  );
}
