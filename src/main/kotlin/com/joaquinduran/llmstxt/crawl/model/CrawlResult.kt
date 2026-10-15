package com.joaquinduran.llmstxt.crawl.model

import com.joaquinduran.llmstxt.ingestion.RunCoverage
import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

data class CrawlResult(
    val finalSeedUrl: String,
    val effectiveOrigin: String,
    val context: SeedContext,
    val resources: List<Resource>,
    val checks: Map<String, String>,
    val bytes: ByteArray,
    val coverage: com.joaquinduran.llmstxt.ingestion.RunCoverage,
    val warnings: List<String>,
    val observedFrom: Long,
    val observedUntil: Long,
)
