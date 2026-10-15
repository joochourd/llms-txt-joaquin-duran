package com.joaquinduran.llmstxt.api.dto

import io.micronaut.http.annotation.*
import io.micronaut.serde.annotation.Serdeable

@Serdeable
data class MonitoringResponse(
    val state: String,
    val enabled: Boolean,
    val nextCheckAt: String?,
    val expiresAt: String?,
    val canEnable: Boolean,
    val failureCount: Long,
    val latestAutomaticAttempt: RunResponse?,
)
