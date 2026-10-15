package com.joaquinduran.llmstxt.crawl.generation

import com.joaquinduran.llmstxt.crawl.analysis.clean
import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.crawl.model.SeedContext
import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

class LlmsRenderer(
    private val configuration: CrawlConfiguration,
) {
    private fun escape(value: String) = clean(value).replace("\\", "\\\\").replace(Regex("([\\[\\]`*_<>])"), "\\\\$1")

    private fun destination(value: String): String {
        // CommonMark decodes entities in destinations; escape ampersands to preserve the actual URL.
        val url = value
            .replace("\\", "\\\\")
            .replace("&", "&amp;")
            .replace("<", "%3C")
            .replace(">", "%3E")
        return if ('(' in url || ')' in url) "<$url>" else url
    }

    fun render(
        seed: ExtractedPage,
        context: SeedContext,
        selected: List<Resource>,
    ): Pair<ByteArray, List<Resource>> {
        val sections = when (context.type) {
            "DOCUMENTATION" -> listOf("Overview", "Guides", "API Reference", "Resources")
            "MARKETING" -> listOf("Overview", "Products and Services", "Documentation", "Company", "Resources")
            "ARTICLE" -> listOf("Overview", "Related Topics", "Resources")
            "BLOG_NEWS" -> listOf("Overview", "Articles", "Topics")
            "COMMERCE" -> listOf("Overview", "Products", "Categories", "Policies", "Resources")
            else -> listOf("Overview", "Resources")
        }
        var resources = selected
        var notes = true
        while (resources.isNotEmpty()) {
            val text = buildString {
                append("# ").append(escape(seed.title)).append("\n\n")
                if (notes && seed.description.isNotEmpty()) append("> ").append(escape(seed.description)).append("\n\n")
                sections.forEach { section ->
                    val rows = resources.filter { it.section == section }
                    if (rows.isNotEmpty()) {
                        append("## ").append(section).append("\n\n")
                        rows.forEach { row ->
                            append("- [")
                                .append(escape(row.title))
                                .append("](")
                                .append(destination(row.preferredLinkUrl))
                                .append(")")
                            if (notes && row.description.isNotEmpty()) append(": ").append(escape(row.description))
                            append('\n')
                        }
                        append('\n')
                    }
                }
            }.trimEnd() + "\n"
            val bytes = text.toByteArray(Charsets.UTF_8)
            if (bytes.size <= configuration.content.maxLlmsBytes) return bytes to resources
            if (notes) notes = false else resources = resources.dropLast(1)
        }
        throw CrawlFailure("OUTPUT_LIMIT", "No useful resource fits the output byte limit.")
    }
}
