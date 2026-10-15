package com.joaquinduran.llmstxt.crawl.model

import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

data class ExtractedPage(
    val candidate: Candidate,
    val url: HttpUrl,
    val title: String,
    val description: String,
    val text: String,
    val headings: List<String>,
    val links: List<ObservedLink>,
    val structured: String,
    val noindex: Boolean,
    val challenge: Boolean,
    val contentHash: String = sha256(text),
    val textLength: Int = text.length,
    val javascriptShell: Boolean = false,
    val sourceFormat: String = "HTML",
    val titleObserved: Boolean = true,
    val warnings: List<String> = emptyList(),
)
