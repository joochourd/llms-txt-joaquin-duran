package com.joaquinduran.llmstxt.api.dto

import com.joaquinduran.llmstxt.crawl.model.Resource
import io.micronaut.http.*
import io.micronaut.http.annotation.*
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.serde.annotation.Serdeable
import java.net.URI

@Serdeable
data class ContextResponse(
    val type: String,
    val confidence: String,
    val evidence: List<String>,
)
