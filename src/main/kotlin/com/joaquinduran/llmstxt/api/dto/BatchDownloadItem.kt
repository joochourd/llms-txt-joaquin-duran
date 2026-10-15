package com.joaquinduran.llmstxt.api.dto

import io.micronaut.serde.annotation.Serdeable

@Serdeable
data class BatchDownloadItem(
    val index: Int,
    val inputUrl: String,
    val status: String,
    val fileName: String?,
    val fileRevision: Long?,
    val outputHash: String?,
    val error: ApiError?,
)
