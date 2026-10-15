package com.joaquinduran.llmstxt.crawl.model

import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

data class Candidate(
    val url: String,
    val source: String,
    val parent: String?,
    val anchor: String,
    val depth: Int?,
    val score: Double,
    val baseline: Boolean = false,
    val seed: Boolean = false,
    val group: String? = null,
    val sectionOverview: Boolean = false,
    val structuralHub: Boolean = false,
)
