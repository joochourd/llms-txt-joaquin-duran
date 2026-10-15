package com.joaquinduran.llmstxt.api.dto

import com.joaquinduran.llmstxt.api.dto.ApiError
import io.micronaut.json.tree.JsonNode
import io.micronaut.serde.annotation.Serdeable
import java.time.Instant
import java.time.format.DateTimeFormatterBuilder

@Serdeable
data class BatchRowResponse(
    val index: Int,
    val inputUrl: String,
    val admission: String?,
    val status: String,
    val identityId: String? = null,
    val runId: String? = null,
    val phase: String? = null,
    val versionSequence: Long? = null,
    val acceptedObservationSequence: Long? = null,
    val outputHash: String? = null,
    val artifactUrl: String? = null,
    val error: ApiError? = null,
)
