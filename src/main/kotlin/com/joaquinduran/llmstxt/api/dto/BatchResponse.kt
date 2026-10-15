package com.joaquinduran.llmstxt.api.dto

import io.micronaut.json.tree.JsonNode
import io.micronaut.serde.annotation.Serdeable
import java.time.Instant
import java.time.format.DateTimeFormatterBuilder

@Serdeable
data class BatchResponse(
    val batchId: String,
    val status: String,
    val admissionComplete: Boolean,
    val createdAt: String,
    val admissionDeadlineAt: String?,
    val items: List<BatchRowResponse>,
    val totalItems: Int = items.size,
    val offset: Int = 0,
    val limit: Int = 100,
    val nextOffset: Int? = null,
    val admittedItems: Int = items.size,
)
