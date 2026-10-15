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

class ResourceClassifier(
    private val ranking: ResourceRanking,
    private val selection: ResourceSelection,
) {
    fun resource(
        page: ExtractedPage,
        seed: ExtractedPage,
        context: SeedContext,
    ): Resource {
        val path = page.url.encodedPath.lowercase()
        val headings = page.headings.joinToString(" ").lowercase()
        val structured = structuredTypes(page)
        val category = when {
            "product" in structured || "softwareapplication" in structured -> "PRODUCT"

            Regex("/(docs|guide|reference|api)(/|$)").containsMatchIn(path) -> "DOCUMENTATION"

            Regex("/(privacy|terms|shipping|returns)(/|$)").containsMatchIn(path) -> "POLICY"

            structured.any { it in setOf("article", "newsarticle", "blogposting", "techarticle") } || page.url.host.endsWith("wikipedia.org") ||
                Regex("/(blog|news|article)/").containsMatchIn(path) -> "ARTICLE"

            Regex("/(category|categories|catalog)(/|$)").containsMatchIn(path) -> "CATEGORY"

            Regex("/(product|products|service|services)(/|$)").containsMatchIn(path) -> "PRODUCT"

            Regex("/(about|team|company|careers)(/|$)").containsMatchIn(path) -> "COMPANY"

            context.type == "DOCUMENTATION" -> "DOCUMENTATION"

            else -> "OTHER"
        }
        val section = if (page.candidate.seed) {
            "Overview"
        } else {
            when (context.type) {
                "DOCUMENTATION" -> if (category == "DOCUMENTATION") {
                    if (contentTokens(path + " " + headings).any { it in setOf("api", "reference") }) "API Reference" else "Guides"
                } else {
                    "Resources"
                }

                "MARKETING" -> when (category) {
                    "PRODUCT" -> "Products and Services"
                    "DOCUMENTATION" -> "Documentation"
                    "COMPANY" -> "Company"
                    else -> "Resources"
                }

                "ARTICLE" -> "Related Topics"

                "BLOG_NEWS" -> if (category == "ARTICLE") "Articles" else "Topics"

                "COMMERCE" -> when (category) {
                    "PRODUCT" -> "Products"
                    "CATEGORY" -> "Categories"
                    "POLICY" -> "Policies"
                    else -> "Resources"
                }

                else -> "Resources"
            }
        }
        val reasons = mutableListOf<String>()
        if (page.candidate.source == "NAVIGATION") reasons.add("PRIMARY_NAVIGATION")
        if (page.candidate.source == "SITEMAP") reasons.add("SITEMAP_DISCOVERY")
        if (page.candidate.baseline) reasons.add("PREVIOUSLY_SELECTED_REVALIDATED")
        if (page.candidate.sectionOverview) reasons.add("SECTION_OVERVIEW")
        if (page.candidate.structuralHub || selection.isHub(page.url.toString(), page.links)) reasons.add("STRUCTURAL_HUB")
        if (page.textLength >= ContentThresholds.SUBSTANTIAL_TEXT_LENGTH) reasons.add("SUBSTANTIAL_MAIN_CONTENT")
        if (category != "OTHER") reasons.add("${category}_PAGE")
        val score = ranking.scoreComponents(page, seed, context).values.sum()
        val final = page.url.toString()
        return Resource(
            sha256(final),
            page.candidate.url,
            final,
            final,
            page.sourceFormat,
            page.title,
            page.description,
            page.contentHash,
            sha256(page.title + "\n" + page.description + "\n" + category),
            sha256(
                page.links
                    .map { it.url }
                    .distinct()
                    .sorted()
                    .joinToString("\n"),
            ),
            category,
            section,
            reasons.distinct().sorted(),
            listOf(page.candidate.url).distinct(),
            page.candidate.depth,
            score,
        )
    }
}
