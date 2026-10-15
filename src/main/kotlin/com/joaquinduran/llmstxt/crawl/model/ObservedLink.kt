package com.joaquinduran.llmstxt.crawl.model

import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

data class ObservedLink(
    val url: String,
    val anchor: String,
    val source: String,
    val group: String? = null,
)
