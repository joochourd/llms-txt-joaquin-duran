package com.joaquinduran.llmstxt.api.downloads

import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl.Companion.toHttpUrl
import java.text.Normalizer
import java.util.Locale

object DownloadNames {
    fun text(
        seedUrl: String,
        version: Long? = null,
    ): String {
        val url = seedUrl.toHttpUrl()
        val source = (listOf(url.host) + url.pathSegments.filter { it.isNotEmpty() }).joinToString("-")
        val normalized = Normalizer
            .normalize(source, Normalizer.Form.NFKD)
            .replace(Regex("\\p{M}+"), "")
            .replace(Regex("[^A-Za-z0-9._-]+"), "-")
            .replace(Regex("-+"), "-")
            .trim('.', '-')
            .lowercase(Locale.ROOT)
        val needsHash = normalized.length > 120 || url.query != null
        val base = normalized.take(120).trimEnd('.', '-') + if (needsHash) "-" + sha256(seedUrl).take(8) else ""
        return base + (version?.let { "-v$it" } ?: "") + ".txt"
    }

    fun unique(
        fileName: String,
        used: MutableSet<String>,
    ): String {
        var candidate = fileName
        var suffix = 2
        while (!used.add(candidate)) {
            candidate = fileName.removeSuffix(".txt") + "-${suffix++}.txt"
        }
        return candidate
    }
}
