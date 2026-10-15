package com.joaquinduran.llmstxt.api.dto

import com.joaquinduran.llmstxt.crawl.model.Resource
import io.micronaut.http.*
import io.micronaut.http.annotation.*
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.serde.annotation.Serdeable
import java.net.URI

@Serdeable
data class ResourceResponse(
    val resourceKey: String,
    val sourceUrl: String,
    val primaryHtmlUrl: String,
    val preferredLinkUrl: String,
    val linkFormat: String,
    val title: String,
    val description: String,
    val category: String,
    val section: String,
    val reasonCodes: List<String>,
)
