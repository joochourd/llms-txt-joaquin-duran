package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.generation.LlmsRenderer
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.ObservedLink
import com.joaquinduran.llmstxt.crawl.model.SeedContext
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.net.URI

class QualityCorpusTest {
    @Test
    fun `documentation keeps foundational navigation ahead of optional modules`() {
        val (_, context, selected) = QualityFixtures.analyzed("documentation")
        assertEquals("DOCUMENTATION", context.type)
        val paths = selected.map { URI(it.primaryHtmlUrl).path }
        assertTrue(paths.containsAll(listOf("/", "/manual/start", "/manual/runtime", "/manual/reference")))
        assertTrue(selected.count { "/plugins/" in it.primaryHtmlUrl } <= 4)
        assertTrue(selected.all { it.category == "DOCUMENTATION" })
    }

    @Test
    fun `editorial coverage includes structural areas instead of mostly one article family`() {
        val (_, _, selected) = QualityFixtures.analyzed("news")
        assertEquals(8, selected.size)
        assertTrue(selected.count { "/dispatches/" in it.primaryHtmlUrl } <= 4)
        assertTrue(selected.map { URI(it.primaryHtmlUrl).path }.containsAll(listOf("/sections", "/topics", "/events")))
        assertTrue(selected.any { it.category == "ARTICLE" })
    }

    @Test
    fun `marketing retains product areas and useful examples without story saturation`() {
        val (_, _, selected) = QualityFixtures.analyzed("marketing")
        assertEquals(8, selected.size)
        assertTrue(selected.count { "/proof/" in it.primaryHtmlUrl } <= 3)
        assertTrue(selected.map { URI(it.primaryHtmlUrl).path }.containsAll(listOf("/solutions", "/plans", "/support", "/company")))
    }

    @Test
    fun `developer documentation retains multiple functional areas and reference`() {
        val (_, context, selected) = QualityFixtures.analyzed("developer")
        assertEquals("DOCUMENTATION", context.type)
        val paths = selected.map { URI(it.primaryHtmlUrl).path }
        assertTrue(paths.containsAll(listOf("/manual/start", "/manual/storage", "/manual/identity", "/manual/transactions")))
        assertTrue(selected.any { it.section == "API Reference" })
    }

    @Test
    fun `JavaScript shells remain insufficient without executing or guessing their data source`() {
        val seed = QualityFixtures.extract(
            "/",
            QualityFixtures.html("insufficient"),
            Candidate("https://quality.example/", "SEED", null, "", 0, 0.0, seed = true),
        )
        assertEquals(0, seed.textLength)
        assertTrue(seed.links.isEmpty())
        assertTrue(seed.javascriptShell)
        assertFalse(DeterministicAnalyzer().useful(seed))
    }

    @Test
    fun `all successful corpus shapes retain deterministic rendering under shuffled observations`() {
        val analyzer = DeterministicAnalyzer()
        val renderer = LlmsRenderer(CrawlConfiguration())
        for (name in listOf("documentation", "news", "marketing", "developer")) {
            val (seed, context, resources, families) = QualityFixtures.analysis(name)
            val expected = renderer.render(seed, context, analyzer.select(resources, 8, families)).first
            for (order in listOf(resources.reversed(), resources.shuffled(kotlin.random.Random(7)))) {
                assertArrayEquals(expected, renderer.render(seed, context, analyzer.select(order, 8, families)).first, name)
            }
        }
    }

    @Test
    fun `diversity does not impose a hard cap on homogeneous useful documentation`() {
        val (_, _, original) = QualityFixtures.analyzed("documentation")
        val resources = (1..24).map { index ->
            original.first { it.section != "Overview" }.copy(
                resourceKey = "$index",
                primaryHtmlUrl = "https://quality.example/manual/plugins/$index",
                contentHash = "$index",
                reasonCodes = emptyList(),
            )
        }
        assertEquals(20, DeterministicAnalyzer().select(resources, 20).size)
    }

    @Test
    fun `diagnostic components explain score and JSON-LD prose cannot become a product type`() {
        val seed = QualityFixtures.extract(
            "/",
            QualityFixtures.html("documentation"),
            Candidate("https://quality.example/", "SEED", null, "", 0, 0.0, seed = true),
        )
        val page = QualityFixtures.extract(
            "/module",
            QualityFixtures.pages("documentation").getValue("/manual/runtime"),
            Candidate("https://quality.example/module", "MAIN_CONTENT", seed.url.toString(), "Runtime", 1, 15.0),
        )
        val analyzer = DeterministicAnalyzer()
        val generic = SeedContext("GENERIC", "LOW", emptyList())
        val resource = analyzer.resource(page, seed, generic)
        assertEquals("OTHER", resource.category)
        assertEquals(analyzer.scoreComponents(page, seed, generic).values.sum(), resource.score)
    }

    @Test
    fun `navigation prominence cannot exempt a repeated leaf family from diversity`() {
        val analyzer = DeterministicAnalyzer()
        val (_, _, sample) = QualityFixtures.analyzed("news")
        val structural = sample.filter { it.category != "ARTICLE" }
        val article = sample.first { it.category == "ARTICLE" }
        val leaves = (1..20).map { index ->
            article.copy(
                resourceKey = "$index",
                primaryHtmlUrl = "https://quality.example/dispatches/$index",
                contentHash = "article-$index",
                reasonCodes = listOf("PRIMARY_NAVIGATION"),
            )
        }
        val selected = analyzer.select(structural + leaves, 8)
        assertTrue(selected.count { it.category == "ARTICLE" } <= 4)
    }

    @Test
    fun `document fragments and query variants cannot manufacture structural hub authority`() {
        val analyzer = DeterministicAnalyzer()
        val base = "https://quality.example/manual/module/"

        fun link(url: String) = ObservedLink(url, "Observed link", "MAIN_CONTENT")
        assertFalse(analyzer.isHub(base, listOf(link("$base#one"), link("$base#two"), link("$base?tab=a"))))
        assertFalse(analyzer.isHub(base, listOf(link("${base}child?tab=a"), link("${base}child?tab=b"))))
        assertTrue(analyzer.isHub(base, listOf(link("${base}first#one"), link("${base}second#two"))))
    }

    @Test
    fun `reference section requires a token rather than a substring in ordinary words`() {
        val seed = QualityFixtures.analysis("developer").seed
        val candidate = Candidate("https://quality.example/capital", "NAVIGATION", seed.url.toString(), "Capital", 1, 20.0)
        val page = QualityFixtures.extract("/capital", "<h1>Capital</h1><main>${"Useful capital information. ".repeat(20)}</main>", candidate)
        assertEquals("Guides", DeterministicAnalyzer().resource(page, seed, SeedContext("DOCUMENTATION", "MEDIUM", emptyList())).section)
    }
}
