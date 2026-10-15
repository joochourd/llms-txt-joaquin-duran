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

class ContextInference {
    fun context(seed: ExtractedPage): SeedContext {
        val openApiDescription = seed.sourceFormat == "JSON" &&
            (
                Regex("\"openapi\"\\s*:\\s*\"3\\.").containsMatchIn(seed.structured) ||
                    Regex("\"swagger\"\\s*:\\s*\"2\\.0\"").containsMatchIn(seed.structured)
            )
        if (openApiDescription) {
            return SeedContext("DOCUMENTATION", "HIGH", listOf("OpenAPI description"))
        }
        val content = (seed.url.encodedPath + " " + seed.title + " " + seed.headings.joinToString(" ")).lowercase()
        val structured = structuredTypes(seed)
        val scores = linkedMapOf<String, Int>()
        val evidence = linkedMapOf<String, MutableList<String>>()

        fun signal(
            type: String,
            condition: Boolean,
            points: Int,
            reason: String,
        ) {
            if (condition) {
                scores[type] = (scores[type] ?: 0) + points
                evidence.getOrPut(type) { mutableListOf() }.add(reason)
            }
        }
        signal(
            "DOCUMENTATION",
            seed.links.count { Regex("/(docs|guide|reference|api)(/|$)").containsMatchIn(it.url) } >= ContextWeights.MIN_RELATED_LINKS,
            ContextWeights.STRUCTURAL_SIGNAL,
            "Documentation navigation",
        )
        signal(
            "DOCUMENTATION",
            Regex("docs|documentation|reference|guide|api").containsMatchIn(content),
            ContextWeights.EXPLICIT_SIGNAL,
            "Documentation headings or path",
        )
        signal(
            "DOCUMENTATION",
            seed.links.any { it.anchor.lowercase().contains("guide") },
            ContextWeights.SUPPORTING_SIGNAL,
            "Guide links",
        )
        signal(
            "DOCUMENTATION",
            Regex("\\b(documentation|docs|reference)\\b").containsMatchIn(seed.title.lowercase()) &&
                seed.links.count {
                    Regex(
                        "\\b(get(?:ting)? started|developer|integration|quickstart|api|sdk|guide)\\b",
                    ).containsMatchIn(it.anchor.lowercase())
                } >=
                ContextWeights.MIN_RELATED_LINKS,
            ContextWeights.STRUCTURAL_SIGNAL,
            "Documentation entry points",
        )
        signal(
            "ARTICLE",
            structured.any { it in setOf("article", "newsarticle", "blogposting", "techarticle") } || seed.url.host.endsWith("wikipedia.org"),
            ContextWeights.STRUCTURAL_SIGNAL,
            "Article structure",
        )
        signal(
            "ARTICLE",
            Regex("/wiki/|/article/|/topic/").containsMatchIn(content),
            ContextWeights.EXPLICIT_SIGNAL,
            "Topic path",
        )
        signal(
            "ARTICLE",
            seed.textLength >= ContentThresholds.LONG_TEXT_LENGTH,
            ContextWeights.SUPPORTING_SIGNAL,
            "Substantial topic content",
        )
        signal(
            "BLOG_NEWS",
            seed.links.count { Regex("/blog/|/news/").containsMatchIn(it.url) } >= ContextWeights.MIN_RELATED_LINKS,
            ContextWeights.STRUCTURAL_SIGNAL,
            "Article collection",
        )
        signal(
            "BLOG_NEWS",
            Regex("blog|news").containsMatchIn(content),
            ContextWeights.EXPLICIT_SIGNAL,
            "News headings or path",
        )
        signal(
            "BLOG_NEWS",
            seed.links.any { it.anchor.lowercase().contains("latest") },
            ContextWeights.SUPPORTING_SIGNAL,
            "Recent article links",
        )
        signal(
            "COMMERCE",
            "product" in structured || seed.links.count { it.url.contains("/product") } >= ContextWeights.MIN_RELATED_LINKS,
            ContextWeights.STRUCTURAL_SIGNAL,
            "Product catalog",
        )
        signal(
            "COMMERCE",
            Regex("shop|store|catalog|products").containsMatchIn(content),
            ContextWeights.EXPLICIT_SIGNAL,
            "Catalog headings or path",
        )
        signal(
            "COMMERCE",
            seed.links.any { it.url.contains("/shipping") || it.url.contains("/returns") },
            ContextWeights.SUPPORTING_SIGNAL,
            "Commerce policies",
        )
        signal(
            "MARKETING",
            "organization" in structured || "softwareapplication" in structured,
            ContextWeights.STRUCTURAL_SIGNAL,
            "Company or software structure",
        )
        signal(
            "MARKETING",
            Regex("platform|software|solutions|services").containsMatchIn(content),
            ContextWeights.EXPLICIT_SIGNAL,
            "Product headings",
        )
        signal(
            "MARKETING",
            seed.links.any { it.url.contains("/pricing") || it.url.contains("/features") },
            ContextWeights.SUPPORTING_SIGNAL,
            "Product navigation",
        )
        val ordered = scores.entries.sortedWith(compareByDescending<Map.Entry<String, Int>> { it.value }.thenBy { it.key })
        val winner = ordered.firstOrNull()
        if (winner == null || winner.value < ContextWeights.MIN_CONTEXT_SCORE ||
            winner.value - (ordered.getOrNull(1)?.value ?: 0) < ContextWeights.MIN_CONTEXT_MARGIN
        ) {
            return SeedContext("GENERIC", "LOW", listOf("Insufficient or ambiguous context signals"))
        }
        return SeedContext(winner.key, if (winner.value >= ContextWeights.HIGH_CONFIDENCE_SCORE) "HIGH" else "MEDIUM", evidence.getValue(winner.key))
    }
}
