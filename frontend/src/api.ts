export async function responseError(response: Response, fallback: string): Promise<Error> {
  try {
    const body = await response.json();
    return new Error(body.message ?? body.error?.message ?? `${fallback} (${response.status}).`);
  } catch {
    return new Error(`${fallback} (${response.status}).`);
  }
}

export async function readJson<T>(url: string, signal: AbortSignal, init?: RequestInit): Promise<T> {
  const response = await fetch(url, { ...init, signal, cache: 'no-store' });
  if (!response.ok) throw await responseError(response, 'Request failed');
  return (await response.json()) as T;
}
