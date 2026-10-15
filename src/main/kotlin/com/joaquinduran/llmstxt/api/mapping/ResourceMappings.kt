package com.joaquinduran.llmstxt.api.mapping

import com.joaquinduran.llmstxt.api.dto.ResourceResponse
import com.joaquinduran.llmstxt.crawl.model.Resource
import io.micronaut.http.*
import io.micronaut.http.annotation.*
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.serde.annotation.Serdeable
import java.net.URI

internal fun Resource.dto() = ResourceResponse(
    resourceKey,
    sourceUrl,
    primaryHtmlUrl,
    preferredLinkUrl,
    linkFormat,
    title,
    description,
    category,
    section,
    reasonCodes,
)
