package com.joaquinduran.llmstxt.api.dto

import com.joaquinduran.llmstxt.crawl.model.Resource
import io.micronaut.http.*
import io.micronaut.http.annotation.*
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.serde.annotation.Serdeable
import java.net.URI

@Serdeable
data class CrawlResponse(
    val identityId: String,
    val normalizedSeedUrl: String,
    val finalSeedUrl: String?,
    val effectiveOrigin: String?,
    val currentVersionSequence: Long?,
    val acceptedObservationSequence: Long,
    val outputHash: String?,
    val artifactUrl: String?,
    val acceptedObservationStartedAt: String?,
    val lastSuccessfulValidationAt: String?,
    val lastOutputUpdatedAt: String?,
    val lastAttemptAt: String?,
    val lastAttemptRunId: String?,
    val lastAttemptSummary: String?,
    val monitoringState: String,
    val automaticChecksEnabled: Boolean,
    val context: ContextResponse?,
    val coverage: CoverageResponse?,
    val warnings: List<String>,
    val monitoring: MonitoringResponse,
    val selectedResources: List<ResourceResponse>,
    val sourceChanged: Boolean? = null,
)
