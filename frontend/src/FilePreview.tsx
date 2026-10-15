import { useEffect, useId, useRef, useState } from 'react';
import { CopyButton } from './CopyButton';
import { Icon } from './Icon';
import { responseError } from './api';

export type PreviewFile = { url: string; website: string };

export function FilePreview({ file, onClose }: { file: PreviewFile; onClose: () => void }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const returnFocus = useRef(document.activeElement instanceof HTMLElement ? document.activeElement : null);
  const titleId = useId();
  const descriptionId = useId();
  const [text, setText] = useState<string | null>(null);
  const [error, setError] = useState('');
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    const element = dialog.current;
    const previousOverflow = document.body.style.overflow;
    element?.showModal();
    document.body.style.overflow = 'hidden';
    return () => {
      element?.close();
      document.body.style.overflow = previousOverflow;
      returnFocus.current?.focus({ preventScroll: true });
    };
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15_000);
    let stopped = false;
    setText(null);
    setError('');

    async function load() {
      try {
        const response = await fetch(file.url, { signal: controller.signal, cache: 'no-store' });
        if (!response.ok) throw await responseError(response, 'The file could not be loaded');
        const content = await response.text();
        if (!stopped) setText(content);
      } catch (failure) {
        if (!stopped) {
          setError(
            controller.signal.aborted
              ? 'The file took too long to load. Please try again.'
              : failure instanceof Error
                ? failure.message
                : 'The file could not be loaded.',
          );
        }
      } finally {
        clearTimeout(timeout);
      }
    }

    void load();
    return () => {
      stopped = true;
      controller.abort();
      clearTimeout(timeout);
    };
  }, [file.url, attempt]);

  return (
    <dialog
      ref={dialog}
      className="preview-dialog"
      aria-labelledby={titleId}
      aria-describedby={descriptionId}
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
      onClick={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div className="dialog-content">
        <div className="dialog-heading">
          <div className="file-heading">
            <span className="file-icon">
              <Icon name="file" />
            </span>
            <div>
              <h2 id={titleId}>llms.txt preview</h2>
              <p className="source-url">{file.website}</p>
            </div>
          </div>
          <button
            type="button"
            className="button icon-button ghost"
            aria-label="Close preview"
            onClick={onClose}
            autoFocus
          >
            <Icon name="close" />
          </button>
        </div>
        <p id={descriptionId} className="muted">
          The current generated file. You can inspect it while the rest of your batch continues.
        </p>
        {error ? (
          <div className="empty-state compact">
            <Icon name="warning" />
            <p className="error" role="alert">
              {error}
            </p>
            <button className="button secondary" type="button" onClick={() => setAttempt((value) => value + 1)}>
              Try again
            </button>
          </div>
        ) : text === null ? (
          <div className="empty-state" role="status">
            <span className="spinner" />
            Loading your file…
          </div>
        ) : (
          <pre className="file-content" tabIndex={0}>
            {text}
          </pre>
        )}
        <div className="dialog-footer">
          <span className="muted">
            Publish this file as <code>/llms.txt</code> on your own website.
          </span>
          <div className="result-actions">
            <CopyButton text={text ?? ''} disabled={text === null} />
            {text !== null && (
              <a className="button primary" href={file.url} download>
                <Icon name="download" />
                Download file
              </a>
            )}
          </div>
        </div>
      </div>
    </dialog>
  );
}
