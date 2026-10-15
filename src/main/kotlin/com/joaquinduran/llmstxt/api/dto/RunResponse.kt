package com.joaquinduran.llmstxt.api.dto

import com.joaquinduran.llmstxt.api.dto.ApiError
import io.micronaut.json.tree.JsonNode
import io.micronaut.serde.annotation.Serdeable
import java.time.Instant
import java.time.format.DateTimeFormatterBuilder

@Serdeable
data class RunResponse(
    val runId: String,
    val identityId: String,
    val status: String,
    val phase: String,
    val pipelineVersion: String,
    val observationSequence: Long?,
    val queuedAt: String,
    val queueDeadlineAt: String?,
    val startedAt: String?,
    val finishedAt: String?,
    val deadlineAt: String?,
    val counters: CountersResponse,
    val warnings: List<String>,
    val publicationDecision: String?,
    val error: ApiError?,
    val versionSequence: Long?,
    val outputHash: String?,
    val coverage: CoverageResponse?,
)
