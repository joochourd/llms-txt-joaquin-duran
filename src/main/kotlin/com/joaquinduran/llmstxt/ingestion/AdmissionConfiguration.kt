package com.joaquinduran.llmstxt.ingestion

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Context

@Context
@ConfigurationProperties("admission")
class AdmissionConfiguration {
    var pipelineVersion: String = "deterministic-v4"
    var maxBatchUrls: Int = 1000
    var maxOpenBulkBatches: Int = 5
    var maxUrlBytes: Int = 2048
    var batchAdmissionTimeoutSeconds: Long = 30
    var maxOutstandingRuns: Long = 40
    var maxMonitoredIdentities: Long = 50
    var crawlFreshnessTtlSeconds: Long = 900
    var maxCrawlDurationSeconds: Long = 180
    var runRecoveryGraceSeconds: Long = 30
    var forceIdentityCooldownSeconds: Long = 60
    var batchRetentionDays: Long = 7
    var maxDepth: Int = 3
    var maxPages: Int = 10
    var maxCandidates: Int = 300
    var maxUrlEvaluations: Int = 2000
    var maxLinksInspected: Int = 5000
    var maxRequests: Int = 60
    var maxSelectedResources: Int = 10

    @jakarta.annotation.PostConstruct
    fun validate() {
        require(pipelineVersion.matches(Regex("[A-Za-z0-9_.-]{1,100}"))) { "Invalid pipeline version." }
        require(maxBatchUrls in 1..1000) { "MAX_BATCH_URLS must be between 1 and 1000." }
        require(maxUrlBytes in 1..2048) { "MAX_URL_BYTES must be between 1 and 2048." }
        require(maxOpenBulkBatches in 1..20) { "MAX_OPEN_BULK_BATCHES must be between 1 and 20." }
        require(
            listOf(
                maxUrlBytes,
                maxDepth,
                maxPages,
                maxCandidates,
                maxUrlEvaluations,
                maxLinksInspected,
                maxRequests,
                maxSelectedResources,
            ).all { it > 0 },
        ) { "Admission budgets must be positive." }
        require(
            listOf(
                batchAdmissionTimeoutSeconds,
                maxOutstandingRuns,
                maxMonitoredIdentities,
                crawlFreshnessTtlSeconds,
                maxCrawlDurationSeconds,
                runRecoveryGraceSeconds,
                forceIdentityCooldownSeconds,
                batchRetentionDays,
            ).all { it in 1..31_536_000 },
        ) { "Invalid admission limits." }
        require(maxSelectedResources <= maxPages && maxPages <= maxCandidates) { "Inconsistent page budgets." }
    }

    fun budgets() = RunBudgets(
        maxDepth,
        maxPages,
        maxCandidates,
        maxUrlEvaluations,
        maxLinksInspected,
        maxRequests,
        maxSelectedResources,
        maxCrawlDurationSeconds,
    )
}
