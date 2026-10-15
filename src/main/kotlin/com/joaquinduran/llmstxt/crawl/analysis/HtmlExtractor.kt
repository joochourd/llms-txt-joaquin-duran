package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import com.joaquinduran.llmstxt.crawl.model.ObservedLink
import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

class HtmlExtractor(
    private val config: CrawlConfiguration,
) {
    fun extract(
        response: FetchResponse,
        candidate: Candidate,
        budget: CrawlBudget,
        linksRemaining: Int,
    ): ExtractedPage {
        budget.remainingMillis()
        val mime = response
            .header("Content-Type")
            ?.substringBefore(';')
            ?.trim()
            ?.lowercase()
        if (mime !in setOf("text/html", "application/xhtml+xml")) throw CrawlFailure("UNSUPPORTED_CONTENT", "The source is not an HTML page.")
        val doc = Jsoup.parse(ByteArrayInputStream(response.bytes), null, response.url.toString())
        budget.remainingMillis()
        val title = clean(doc.selectFirst("h1")?.text().orEmpty())
            .ifEmpty { clean(doc.title()) }
            .ifEmpty {
                response.url.pathSegments
                    .lastOrNull { it.isNotEmpty() }
                    ?.replace('-', ' ') ?: response.url.host
            }
        val description = clean(doc.selectFirst("meta[name=description]")?.attr("content").orEmpty())
            .ifEmpty { clean(doc.selectFirst("meta[property=og:description]")?.attr("content").orEmpty()) }
            .ifEmpty {
                doc
                    .select("main p, article p, p")
                    .map { clean(it.text()) }
                    .firstOrNull {
                        it.length >=
                            ContentThresholds.MIN_USEFUL_DESCRIPTION_LENGTH
                    }.orEmpty()
            }
        val links = doc.select("a[href]").take(linksRemaining).map { link ->
            budget.remainingMillis()
            ObservedLink(
                link.absUrl("href"),
                clean(link.text()).take(ContentThresholds.MAX_ANCHOR_LENGTH),
                when {
                    link.closest("footer") != null -> "FOOTER"
                    link.closest("aside") != null -> "SECONDARY_NAVIGATION"
                    link.closest("nav, header, [role=navigation]") != null -> "NAVIGATION"
                    else -> "MAIN_CONTENT"
                },
                link
                    .closest(
                        "section",
                    )?.selectFirst("h2, h3")
                    ?.text()
                    ?.let(::clean)
                    ?.take(ContentThresholds.MAX_GROUP_LENGTH)
                    ?.takeIf { it.isNotBlank() },
            )
        }
        val structured = doc
            .select(
                "script[type=application/ld+json]",
            ).joinToString(" ") { it.data().take(ContentThresholds.MAX_STRUCTURED_DATA_LENGTH) }
        val main = (doc.selectFirst("main") ?: doc.selectFirst("article") ?: doc.body()).clone()
        main.select("script, style, nav, footer, header, noscript, form, [aria-hidden=true]").remove()
        val text = clean(main.text())
        val redirectTitle = title.lowercase().trimEnd('.', '…')
        if (text.length < ContentThresholds.SUBSTANTIAL_TEXT_LENGTH && redirectTitle in setOf("redirecting", "redirecting you")) {
            throw CrawlFailure(
                "SOURCE_REDIRECT_PLACEHOLDER",
                "The source is a client-side redirect placeholder; submit the destination document URL.",
            )
        }
        val challenge = listOf(
            "verify you are human",
            "checking your browser",
            "access denied",
            "enable javascript and cookies",
            "just a moment",
            "captcha",
        ).any { phrase ->
            clean(doc.title()).lowercase().contains(phrase) ||
                (text.length < ContentThresholds.CHALLENGE_TEXT_LIMIT && text.lowercase().contains(phrase))
        } || (
            text.length < ContentThresholds.CHALLENGE_TEXT_LIMIT && title.lowercase() in setOf(
                "error",
                "forbidden",
                "not found",
                "page not found",
                "404 not found",
                "internal server error",
                "service unavailable",
            )
        )
        return ExtractedPage(
            candidate,
            response.url,
            title.take(config.content.titleMaxChars),
            description.take(config.content.descriptionMaxChars),
            text,
            main.select("h1, h2, h3").take(ContentThresholds.MAX_HEADINGS).map {
                clean(it.text()).take(ContentThresholds.MAX_HEADING_LENGTH)
            },
            links,
            structured,
            doc.select("meta[name=robots]").any { it.attr("content").lowercase().contains("noindex") },
            challenge,
            javascriptShell = text.isEmpty() && links.isEmpty() && doc.select("script[src]").isNotEmpty(),
            titleObserved = doc.selectFirst("h1")?.text()?.isNotBlank() == true || doc.title().isNotBlank(),
        )
    }
}
