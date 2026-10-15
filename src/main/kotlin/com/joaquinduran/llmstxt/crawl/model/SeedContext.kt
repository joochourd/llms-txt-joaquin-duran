package com.joaquinduran.llmstxt.crawl.model

import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

data class SeedContext(
    val type: String,
    val confidence: String,
    val evidence: List<String>,
)
