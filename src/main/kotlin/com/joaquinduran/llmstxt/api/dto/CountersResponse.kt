package com.joaquinduran.llmstxt.api.dto

import io.micronaut.json.tree.JsonNode
import io.micronaut.serde.annotation.Serdeable
import java.time.Instant
import java.time.format.DateTimeFormatterBuilder

@Serdeable
data class CountersResponse(
    val pages: Long,
    val candidates: Long,
    val requests: Long,
)
