package com.joaquinduran.llmstxt.crawl.model

import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

data class Resource(
    val resourceKey: String,
    val sourceUrl: String,
    val primaryHtmlUrl: String,
    val preferredLinkUrl: String,
    val linkFormat: String,
    val title: String,
    val description: String,
    val contentHash: String,
    val metadataHash: String,
    val relevantLinksHash: String,
    val category: String,
    val section: String,
    val reasonCodes: List<String>,
    val aliases: List<String>,
    val depth: Int?,
    val score: Double,
)
