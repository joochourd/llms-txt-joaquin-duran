package com.joaquinduran.llmstxt.api.dto

import io.micronaut.json.tree.JsonNode
import io.micronaut.serde.annotation.Serdeable
import java.time.Instant
import java.time.format.DateTimeFormatterBuilder

@Serdeable
data class CoverageResponse(
    val pagesAttempted: Long,
    val pagesUseful: Long,
    val candidatesRetained: Long,
    val candidatesOmitted: Long,
    val limitReasons: List<String>,
    val discoverySources: List<String>,
)
