package com.joaquinduran.llmstxt.api.mapping

import com.joaquinduran.llmstxt.api.dto.ContextResponse
import com.joaquinduran.llmstxt.api.dto.CoverageResponse
import com.joaquinduran.llmstxt.api.dto.CrawlResponse
import com.joaquinduran.llmstxt.api.dto.MonitoringResponse
import com.joaquinduran.llmstxt.ingestion.RunCoverage
import com.joaquinduran.llmstxt.results.*
import com.joaquinduran.llmstxt.results.AcceptedCrawl

internal fun RunCoverage.dto() = CoverageResponse(
    pagesAttempted = pagesAttempted,
    pagesUseful = pagesUseful,
    candidatesRetained = candidatesRetained,
    candidatesOmitted = candidatesOmitted,
    limitReasons = limitReasons,
    discoverySources = discoverySources,
)

internal fun AcceptedCrawl.dto(monitoring: MonitoringResponse) = CrawlResponse(
    identityId = metadata.identityId,
    normalizedSeedUrl = metadata.normalizedSeedUrl,
    finalSeedUrl = metadata.finalSeedUrl,
    effectiveOrigin = metadata.effectiveOrigin,
    currentVersionSequence = metadata.currentVersionSequence,
    acceptedObservationSequence = metadata.acceptedObservationSequence,
    outputHash = metadata.currentArtifactHash,
    artifactUrl = if (metadata.hasCanonical()) "/artifacts/${metadata.identityId}/llms.txt" else null,
    acceptedObservationStartedAt = metadata.acceptedObservationStartedAt?.let(::timestamp),
    lastSuccessfulValidationAt = metadata.lastSuccessfulValidationAt?.let(::timestamp),
    lastOutputUpdatedAt = metadata.lastOutputUpdatedAt?.let(::timestamp),
    lastAttemptAt = metadata.lastAttemptAt?.let(::timestamp),
    lastAttemptRunId = metadata.lastAttemptRunId,
    lastAttemptSummary = metadata.lastAttemptSummary,
    monitoringState = monitoring.state,
    automaticChecksEnabled = monitoring.enabled,
    context = context?.let { ContextResponse(it.type, it.confidence, it.evidence) },
    coverage = coverage?.dto(),
    warnings = warnings,
    monitoring = monitoring,
    selectedResources = resources.map { it.dto() },
    sourceChanged = sourceChanged,
)
