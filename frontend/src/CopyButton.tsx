import { useEffect, useRef, useState } from 'react';
import { Icon } from './Icon';

export function CopyButton({ text, disabled = false }: { text: string; disabled?: boolean }) {
  const [state, setState] = useState<'idle' | 'copied' | 'failed'>('idle');
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    setState('idle');
    return () => {
      mounted.current = false;
    };
  }, [text]);

  async function copy() {
    try {
      await navigator.clipboard.writeText(text);
      if (mounted.current) setState('copied');
    } catch {
      if (mounted.current) setState('failed');
    }
  }

  return (
    <span className="copy-control">
      <button className="button secondary" type="button" disabled={disabled} onClick={() => void copy()}>
        <Icon name={state === 'copied' ? 'check' : 'copy'} />
        {state === 'copied' ? 'Copied' : 'Copy text'}
      </button>
      <span className={state === 'failed' ? 'error inline-note' : 'sr-only'} role="status">
        {state === 'failed'
          ? 'Copy failed. Select the text or download the file.'
          : state === 'copied'
            ? 'Text copied.'
            : ''}
      </span>
    </span>
  );
}
