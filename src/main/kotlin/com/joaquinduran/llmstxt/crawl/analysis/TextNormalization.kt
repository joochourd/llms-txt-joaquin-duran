package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

internal fun clean(value: String) = Normalizer.normalize(value, Normalizer.Form.NFC).replace(Regex("[\\p{Z}\\s\\p{Cc}]+"), " ").trim()
