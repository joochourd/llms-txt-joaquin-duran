import { resultOffset } from './batchPage.ts';

export type Route = { batchId: string | null; identityId: string | null; offset: number };

export function readRoute(search: string): Route {
  const query = new URLSearchParams(search);
  return { batchId: query.get('batch'), identityId: query.get('crawl'), offset: resultOffset(search) };
}

export function routeUrl(route: Route): string {
  const query = new URLSearchParams();
  if (route.batchId) query.set('batch', route.batchId);
  if (route.identityId) query.set('crawl', route.identityId);
  if (route.offset > 0) query.set('offset', String(route.offset));
  return query.size ? `/?${query}` : '/';
}
