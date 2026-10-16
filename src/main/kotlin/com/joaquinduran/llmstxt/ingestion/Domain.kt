package com.joaquinduran.llmstxt.ingestion

enum class RunStatus {
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED,
    TIMED_OUT,
    ;

    fun terminal() = this in setOf(COMPLETED, FAILED, TIMED_OUT)
}

enum class RunPhase { PREPARING, DISCOVERING, ANALYZING, PUBLISHING }

enum class Admission { QUEUED, COALESCED, CACHED, REJECTED }

enum class MonitoringState { NONE, RESERVED, ACTIVE, EXPIRED }

enum class PublicationDecision { UPDATED, UNCHANGED, SUPERSEDED, PRESERVED }

data class DomainError(
    val code: String,
    val message: String,
)

data class CrawlIdentity(
    val identityId: String,
    val normalizedSeedUrl: String,
    val intentProfile: String,
    val profileHash: String,
    val origin: String,
)

data class CrawlMeta(
    val identityId: String,
    val normalizedSeedUrl: String,
    val intentProfile: String,
    val requiredPipelineVersion: String,
    val metadataRevision: Long = 0,
    val nextObservationSequence: Long = 0,
    val acceptedObservationSequence: Long = 0,
    val outstandingRunCount: Long = 0,
    val monitoringState: MonitoringState = MonitoringState.NONE,
    val monitoringSlotReserved: Boolean = false,
    val acceptedPipelineVersion: String? = null,
    val currentVersionSequence: Long? = null,
    val currentArtifactHash: String? = null,
    val acceptedObservationStartedAt: Long? = null,
    val finalSeedUrl: String? = null,
    val effectiveOrigin: String? = null,
    val lastAttemptAt: Long? = null,
    val lastAttemptRunId: String? = null,
    val lastAttemptSummary: String? = null,
    val lastSuccessfulValidationAt: Long? = null,
    val lastOutputUpdatedAt: Long? = null,
    val activeRunIdHint: String? = null,
    val activeHintExpiresAt: Long? = null,
    val monitoringReservationExpiresAt: Long? = null,
    val monitoringExpiresAt: Long? = null,
    val nextCheckAt: Long? = null,
    val failureCount: Long = 0,
    val dueShard: Int? = null,
    val duePk: String? = null,
    val dueSk: String? = null,
    val lastMonitoringRunId: String? = null,
) {
    fun hasCanonical() = currentVersionSequence != null && currentArtifactHash != null && acceptedObservationSequence > 0
}

data class RunBudgets(
    val maxDepth: Int,
    val maxPages: Int,
    val maxCandidates: Int,
    val maxUrlEvaluations: Int,
    val maxLinksInspected: Int,
    val maxRequests: Int,
    val maxSelectedResources: Int,
    val maxCrawlDurationSeconds: Long,
)

data class RunCounters(
    val pages: Long = 0,
    val candidates: Long = 0,
    val requests: Long = 0,
)

data class RunCoverage(
    val pagesAttempted: Long,
    val pagesUseful: Long,
    val candidatesRetained: Long,
    val candidatesOmitted: Long,
    val limitReasons: List<String>,
    val discoverySources: List<String>,
)

data class CrawlRun(
    val runId: String,
    val identityId: String,
    val normalizedSeedUrl: String,
    val pipelineVersion: String,
    val originHint: String,
    val queuedAt: Long,
    val queueDeadlineAt: Long? = null,
    val availableAt: Long,
    val workShard: Int,
    val workPk: String?,
    val workSk: String?,
    val budgets: RunBudgets,
    val status: RunStatus = RunStatus.QUEUED,
    val phase: RunPhase = RunPhase.PREPARING,
    val capacityReserved: Boolean = true,
    val trigger: String = "INTERACTIVE",
    val attemptId: String? = null,
    val observationSequence: Long? = null,
    val startedAt: Long? = null,
    val finishedAt: Long? = null,
    val deadlineAt: Long? = null,
    val recoveryAt: Long? = null,
    val counters: RunCounters = RunCounters(),
    val warnings: List<String> = emptyList(),
    val coverage: RunCoverage? = null,
    val publicationDecision: PublicationDecision? = null,
    val error: DomainError? = null,
    val versionSequence: Long? = null,
    val outputHash: String? = null,
    val expiresAtEpochSeconds: Long? = null,
)

data class Batch(
    val batchId: String,
    val inputs: List<String>,
    val force: Boolean,
    val createdAt: Long,
    val admissionDeadlineAt: Long?,
    val expiresAtEpochSeconds: Long,
    val admissionComplete: Boolean = false,
    val bulk: Boolean = false,
    val totalInputs: Int = inputs.size,
    val received: Boolean = true,
    val nextAdmissionIndex: Int = 0,
    val nextCompletionIndex: Int = 0,
    // Derived from persisted WorkIndex keys, including pending retention work.
    val workPending: Boolean = false,
    val receiptDeadlineAt: Long? = null,
    val resolvedInputs: Int = 0,
    val nextRetentionIndex: Int = 0,
)

data class BatchItem(
    val batchId: String,
    val index: Int,
    val seedUrl: String,
    val admission: Admission,
    val identityId: String? = null,
    val runId: String? = null,
    val versionSequence: Long? = null,
    val acceptedObservationSequence: Long? = null,
    val outputHash: String? = null,
    val error: DomainError? = null,
    val expiresAtEpochSeconds: Long?,
)

data class Capacity(
    val outstandingRuns: Long,
    val monitoredIdentities: Long,
)
