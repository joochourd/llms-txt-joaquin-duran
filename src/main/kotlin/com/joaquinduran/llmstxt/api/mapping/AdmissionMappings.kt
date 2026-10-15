package com.joaquinduran.llmstxt.api.mapping

import com.joaquinduran.llmstxt.api.dto.ApiError
import com.joaquinduran.llmstxt.api.dto.CountersResponse
import com.joaquinduran.llmstxt.api.dto.CoverageResponse
import com.joaquinduran.llmstxt.api.dto.RunResponse
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.DomainError
import io.micronaut.json.tree.JsonNode
import io.micronaut.serde.annotation.Serdeable
import java.time.Instant
import java.time.format.DateTimeFormatterBuilder

internal fun DomainError.dto() = ApiError(code, message)

private val utcMillis = DateTimeFormatterBuilder().appendInstant(3).toFormatter()

internal fun timestamp(epoch: Long): String = utcMillis.format(Instant.ofEpochMilli(epoch))

internal fun CrawlRun.dto() = RunResponse(
    runId,
    identityId,
    status.name,
    phase.name,
    pipelineVersion,
    observationSequence,
    timestamp(queuedAt),
    queueDeadlineAt?.let(::timestamp),
    startedAt?.let(::timestamp),
    finishedAt?.let(::timestamp),
    deadlineAt?.let(::timestamp),
    CountersResponse(counters.pages, counters.candidates, counters.requests),
    warnings,
    publicationDecision?.name,
    error?.dto(),
    versionSequence,
    outputHash,
    coverage?.let {
        CoverageResponse(
            it.pagesAttempted,
            it.pagesUseful,
            it.candidatesRetained,
            it.candidatesOmitted,
            it.limitReasons,
            it.discoverySources,
        )
    },
)
