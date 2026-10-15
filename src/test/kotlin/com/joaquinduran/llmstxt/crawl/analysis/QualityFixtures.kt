package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.crawl.model.SeedContext
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.jsoup.Jsoup

internal object QualityFixtures {
    data class Analysis(
        val seed: ExtractedPage,
        val context: SeedContext,
        val resources: List<Resource>,
        val families: Map<String, String>,
    )

    fun html(name: String) = requireNotNull(javaClass.getResource("/fixtures/quality/$name.html")).readText()

    fun pages(name: String): Map<String, String> {
        val seed = html(name)
        val pages = linkedMapOf("/" to seed)
        Jsoup.parse(seed).select("a[href]").forEach { link ->
            val title = link.text()
            val type = link.attr("data-type").ifBlank { "WebPage" }
            pages[link.attr("href")] = html("detail")
                .replace("{{TITLE}}", title)
                .replace("{{TYPE}}", type)
                .replace("{{CONTENT}}", "$title. " + "Practical explanations for $title and its distinct workflows. ".repeat(24))
        }
        return pages
    }

    fun extract(
        path: String,
        html: String,
        candidate: Candidate,
    ) = HtmlExtractor(CrawlConfiguration())
        .extract(
            FetchResponse("https://quality.example$path".toHttpUrl(), 200, mapOf("Content-Type" to listOf("text/html")), html.toByteArray()),
            candidate,
            CrawlBudget(60, 5000),
            5000,
        ).copy(text = "")

    fun analysis(name: String): Analysis {
        val pages = pages(name)
        val seed = extract("/", pages.getValue("/"), Candidate("https://quality.example/", "SEED", null, "", 0, 0.0, seed = true))
        val analyzer = DeterministicAnalyzer()
        val context = analyzer.context(seed)
        val observations = listOf(seed) + seed.links.map { link ->
            val url = link.url.toHttpUrl()
            val candidate = Candidate(
                link.url,
                link.source,
                seed.url.toString(),
                link.anchor,
                1,
                analyzer.preScore(url, link.anchor, link.source, seed, context),
                group = link.group,
                sectionOverview = link.group?.equals(link.anchor, true) == true,
                structuralHub = analyzer.isHub(link.url, seed.links),
            )
            extract(url.encodedPath, pages.getValue(url.encodedPath), candidate)
        }
        val resources = observations.filter(analyzer::useful).map { analyzer.resource(it, seed, context) }
        val families = resources.associate { resource ->
            resource.resourceKey to analyzer.family(
                observations.first { it.url.toString() == resource.primaryHtmlUrl }.candidate.copy(
                    structuralHub =
                        "STRUCTURAL_HUB" in resource.reasonCodes,
                ),
                resource.category,
            )
        }
        return Analysis(seed, context, resources, families)
    }

    fun analyzed(name: String): Triple<ExtractedPage, SeedContext, List<Resource>> {
        val analysis = analysis(name)
        return Triple(analysis.seed, analysis.context, DeterministicAnalyzer().select(analysis.resources, 8, analysis.families))
    }
}
