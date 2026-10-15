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

class ResourceRanking {
    fun preComponents(
        url: HttpUrl,
        anchor: String,
        source: String,
        seed: ExtractedPage,
        context: SeedContext,
    ): Map<String, Double> {
        val scores = linkedMapOf(
            "source" to when (source) {
                "NAVIGATION" -> if (context.type == "ARTICLE") RankingWeights.ARTICLE_NAVIGATION else RankingWeights.NAVIGATION
                "SECONDARY_NAVIGATION" -> RankingWeights.SECONDARY_NAVIGATION
                "MAIN_CONTENT" -> if (context.type == "ARTICLE") RankingWeights.ARTICLE_MAIN_CONTENT else RankingWeights.MAIN_CONTENT
                "FOOTER" -> RankingWeights.FOOTER
                "SITEMAP" -> RankingWeights.SITEMAP
                else -> 0.0
            },
        )
        val seedTokens = contentTokens(
            seed.title + " " + seed.url.pathSegments
                .lastOrNull()
                .orEmpty(),
        )
        if (seedTokens.isNotEmpty()) {
            scores["seedAffinity"] = RankingWeights.SEED_AFFINITY *
                contentTokens(anchor + " " + url.encodedPath).intersect(seedTokens).size / seedTokens.size
        }
        val parent = seed.url.encodedPath
            .substringBeforeLast('/')
            .takeIf { it.length > 1 }
        scores["pathAffinity"] = if (parent != null && url.encodedPath.startsWith("$parent/")) {
            RankingWeights.PARENT_PATH_AFFINITY
        } else if (url.pathSegments.intersect(seed.url.pathSegments).any { it.isNotBlank() }) {
            RankingWeights.SHARED_PATH_AFFINITY
        } else {
            0.0
        }
        val path = url.encodedPath.lowercase()
        if (Regex("/(docs|guide|reference|api)(/|$)").containsMatchIn(path)) {
            scores["documentation"] =
                RankingWeights.DOCUMENTATION + if (context.type == "DOCUMENTATION") RankingWeights.CONTEXT_BONUS else 0
        }
        if (Regex("/(product|products|service|services)(/|$)").containsMatchIn(path)) {
            scores["product"] =
                RankingWeights.PRODUCT + if (context.type in setOf("MARKETING", "COMMERCE")) RankingWeights.CONTEXT_BONUS else 0
        }
        if (context.type == "BLOG_NEWS" && Regex("/(blog|news|article|topic)/").containsMatchIn(path)) scores["topic"] = RankingWeights.TOPIC
        if (context.type == "COMMERCE" && Regex("/(shipping|returns)(/|$)").containsMatchIn(path) &&
            source == "FOOTER"
        ) {
            scores["commercePolicy"] = RankingWeights.COMMERCE_POLICY
        }
        if (url.querySize > RankingWeights.MAX_SIMPLE_QUERY_PARAMETERS) scores["complexQuery"] = RankingWeights.COMPLEX_QUERY
        return scores
    }

    fun preScore(
        url: HttpUrl,
        anchor: String,
        source: String,
        seed: ExtractedPage,
        context: SeedContext,
    ) = preComponents(url, anchor, source, seed, context).values.sum()

    fun scoreComponents(
        page: ExtractedPage,
        seed: ExtractedPage,
        context: SeedContext,
    ): Map<String, Double> =
        preComponents(page.url, page.title + " " + page.headings.joinToString(" "), page.candidate.source, seed, context) + mapOf(
            "content" to if (page.textLength >= ContentThresholds.LONG_TEXT_LENGTH) {
                RankingWeights.LONG_CONTENT
            } else if (page.textLength >= ContentThresholds.SUBSTANTIAL_TEXT_LENGTH) {
                RankingWeights.SUBSTANTIAL_CONTENT
            } else {
                0.0
            },
            "description" to
                if (page.description.length >= ContentThresholds.MIN_USEFUL_DESCRIPTION_LENGTH) RankingWeights.USEFUL_DESCRIPTION else 0.0,
            "title" to if (page.title.length >= ContentThresholds.MIN_USEFUL_TITLE_LENGTH) RankingWeights.USEFUL_TITLE else 0.0,
            "noindex" to if (page.noindex) RankingWeights.NOINDEX else 0.0,
            "sectionOverview" to if (page.candidate.sectionOverview) RankingWeights.SECTION_OVERVIEW.toDouble() else 0.0,
        )

    fun useful(page: ExtractedPage) = !page.challenge && !page.javascriptShell &&
        (
            page.textLength >= ContentThresholds.MIN_USEFUL_TEXT_LENGTH ||
                (
                    page.titleObserved && page.title.length >= ContentThresholds.MIN_USEFUL_TITLE_LENGTH &&
                        (
                            page.textLength >= ContentThresholds.MIN_SHORT_TEXT_LENGTH ||
                                page.description.length >= ContentThresholds.MIN_USEFUL_DESCRIPTION_LENGTH
                        )
                )
        )
}
