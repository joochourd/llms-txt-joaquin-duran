export type Resource = {
  resourceKey: string;
  title: string;
  description: string;
  sourceUrl: string;
  preferredLinkUrl: string;
  category: string;
  reasonCodes: string[];
};
export type Attempt = {
  status: string;
  publicationDecision?: string;
  queuedAt: string;
  startedAt?: string;
  finishedAt?: string;
  error?: { message: string };
};
export type Monitoring = {
  state: string;
  enabled: boolean;
  nextCheckAt?: string;
  expiresAt?: string;
  canEnable: boolean;
  failureCount: number;
  latestAutomaticAttempt?: Attempt;
};
export type Crawl = {
  identityId: string;
  normalizedSeedUrl: string;
  finalSeedUrl?: string;
  currentVersionSequence?: number;
  acceptedObservationSequence: number;
  outputHash?: string;
  sourceChanged?: boolean;
  artifactUrl?: string;
  lastSuccessfulValidationAt?: string;
  lastOutputUpdatedAt?: string;
  lastAttemptSummary?: string;
  monitoring: Monitoring;
  selectedResources?: Resource[];
  warnings?: string[];
  coverage?: {
    pagesAttempted: number;
    pagesUseful: number;
    limitReasons?: string[];
  };
};

// Empty collections are omitted by the API serializer, including before first acceptance.
export function crawlResult(crawl: Crawl) {
  return {
    ...crawl,
    selectedResources: crawl.selectedResources ?? [],
    warnings: crawl.warnings ?? [],
    coverage: crawl.coverage ? { ...crawl.coverage, limitReasons: crawl.coverage.limitReasons ?? [] } : undefined,
  };
}

export function sourceChangeMessage(crawl: Crawl): string | null {
  if (crawl.sourceChanged === false) return 'The selected website pages were unchanged at the latest check.';
  if (crawl.sourceChanged !== true) return null;
  if (
    crawl.lastSuccessfulValidationAt &&
    crawl.lastOutputUpdatedAt &&
    Date.parse(crawl.lastSuccessfulValidationAt) > Date.parse(crawl.lastOutputUpdatedAt)
  ) {
    return 'The selected website pages changed. The generated llms.txt text stayed the same.';
  }
  return 'The latest check detected changes in the selected website pages.';
}

export function preservedResultMessage(crawl: Crawl): string | null {
  return crawl.artifactUrl && crawl.lastAttemptSummary === 'PRESERVED'
    ? 'An update could not be published. Your previously accepted file is still available to preview and download.'
    : null;
}
