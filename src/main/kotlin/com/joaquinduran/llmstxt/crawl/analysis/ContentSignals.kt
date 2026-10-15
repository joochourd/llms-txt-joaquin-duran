package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import com.joaquinduran.llmstxt.crawl.model.ObservedLink
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.crawl.model.SeedContext
import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

private val stopwords =
    setOf("the", "and", "for", "with", "your", "from", "this", "that", "www", "com", "org", "html", "index", "home", "about", "docs")

internal fun contentTokens(value: String) = Regex("[\\p{L}\\p{N}]+")
    .findAll(value.lowercase())
    .map { it.value }
    .filter {
        it.length > 2 &&
            it !in stopwords
    }.toSet()

internal fun structuredTypes(page: ExtractedPage): Set<String> = Regex("\"@type\"\\s*:\\s*(\"[^\"]+\"|\\[[^]]*])")
    .findAll(page.structured)
    .flatMap { match ->
        Regex("\"([^\"]+)\"").findAll(match.groupValues[1]).map { it.groupValues[1].substringAfterLast('/').lowercase() }
    }.toSet()
