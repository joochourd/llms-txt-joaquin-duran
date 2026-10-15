package com.joaquinduran.llmstxt.api.dto

import io.micronaut.serde.annotation.Serdeable

@Serdeable
data class BatchDownloadSummary(
    val batchId: String,
    val items: List<BatchDownloadItem>,
    val resultPolicy: String = "CURRENT",
    val totalItems: Int = items.size,
    val offset: Int = 0,
    val nextOffset: Int? = null,
)
