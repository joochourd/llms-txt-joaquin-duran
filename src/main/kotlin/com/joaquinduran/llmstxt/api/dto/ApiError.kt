package com.joaquinduran.llmstxt.api.dto

import io.micronaut.serde.annotation.Serdeable

@Serdeable
data class ApiError(
    val code: String,
    val message: String,
)
