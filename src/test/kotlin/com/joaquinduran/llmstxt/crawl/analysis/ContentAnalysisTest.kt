package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.discovery.SitemapDiscovery
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.generation.LlmsRenderer
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.SeedContext
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

class ContentAnalysisTest {
    private val config = CrawlConfiguration()

    private fun page(body: String) = HtmlExtractor(config).extract(
        FetchResponse(
            "https://example.com/docs/".toHttpUrl(),
            200,
            mapOf("Content-Type" to listOf("text/html")),
            body.toByteArray(),
        ),
        Candidate("https://example.com/docs/", "SEED", null, "", 0, 0.0, seed = true),
        CrawlBudget(60, 5000),
        100,
    )

    @Test
    fun `extractor hashes the complete cleaned main content and retains navigation evidence`() {
        val analyzer = DeterministicAnalyzer()
        val context = SeedContext("DOCUMENTATION", "MEDIUM", emptyList())
        val navigation = "<h1>Documentation</h1><nav><a href='/docs/authentication'>Authentication</a></nav>"
        val mainText = "Useful content ".repeat(100)
        val originalPage = page("$navigation<main><p>$mainText</p><p>Version 2026</p></main><footer>noise</footer>")
        val updatedPage = page("$navigation<main><p>$mainText</p><p>Version 2027</p></main><footer>noise</footer>")
        val releasedPage = originalPage.copy(text = "")

        val originalResource = analyzer.resource(originalPage, originalPage, context)
        val updatedResource = analyzer.resource(updatedPage, updatedPage, context)
        val releasedResource = analyzer.resource(releasedPage, releasedPage, context)

        assertFalse(originalPage.text.contains("noise"))
        assertTrue(originalPage.text.contains("2026"))
        assertEquals("NAVIGATION", originalPage.links.single().source)
        assertNotEquals(originalResource.contentHash, updatedResource.contentHash)
        assertEquals(originalResource, releasedResource)
        assertTrue(analyzer.useful(releasedPage))
    }

    @Test
    fun `challenges are insufficient and weak context falls back to generic`() {
        val analyzer = DeterministicAnalyzer()
        val challengePage = page("<title>Just a moment</title><main>Verify you are human</main>")
        val genericPage = page("<h1>Example</h1><p>A small page.</p>")

        assertFalse(analyzer.useful(challengePage))
        assertEquals("GENERIC", analyzer.context(genericPage).type)
    }

    @Test
    fun `selection and rendering are deterministic under reversed observations and dedupe content with stable aliases`() {
        val analyzer = DeterministicAnalyzer()
        val seedHtml = "<h1>API [Guide]</h1><meta name='description' content='Read practical documentation.'>" +
            "<main>${"Detailed instructions. ".repeat(20)}</main>"
        val seed = page(seedHtml)
        val context = SeedContext("DOCUMENTATION", "MEDIUM", listOf("Documentation headings"))
        val seedResource = analyzer.resource(seed, seed, context)
        val authenticationResource = seedResource.copy(
            resourceKey = "b",
            primaryHtmlUrl = "https://example.com/docs/authentication",
            sourceUrl = "https://example.com/docs/authentication",
            preferredLinkUrl = "https://example.com/docs/authentication",
            section = "Guides",
            category = "DOCUMENTATION",
            contentHash = "other",
            aliases = listOf("https://example.com/docs/authentication"),
        )
        val duplicateAuthenticationResource = authenticationResource.copy(
            resourceKey = "c",
            primaryHtmlUrl = "https://example.com/docs/alias",
            aliases = listOf("https://example.com/docs/alias"),
        )
        val resources = listOf(seedResource, authenticationResource, duplicateAuthenticationResource)
        val renderer = LlmsRenderer(config)

        val (renderedBytes, selectedResources) = renderer.render(seed, context, analyzer.select(resources, 20))
        val (reversedBytes, _) = renderer.render(seed, context, analyzer.select(resources.reversed(), 20))

        assertArrayEquals(renderedBytes, reversedBytes)
        assertEquals(2, selectedResources.size)
        assertEquals(2, selectedResources.first { it.section == "Guides" }.aliases.size)
        assertTrue(renderedBytes.toString(Charsets.UTF_8).contains("API \\[Guide\\]"))
        assertEquals('\n', renderedBytes.last().toInt().toChar())
    }

    @Test
    fun `sitemap parser rejects DTD and gzip expansion and bounds entries`() {
        val parser = SitemapDiscovery(config)

        fun response(xml: String) = FetchResponse("https://example.com/sitemap.xml".toHttpUrl(), 200, emptyMap(), xml.toByteArray())

        assertThrows(Exception::class.java) {
            parser.parse(
                response("<!DOCTYPE x [<!ENTITY foo SYSTEM 'file:///etc/passwd'>]><urlset><url><loc>&foo;</loc></url></urlset>"),
                CrawlBudget(60, 5000),
                1000,
            )
        }
        val failure = assertThrows(CrawlFailure::class.java) {
            parser.parse(
                response("<urlset><url><loc>https://example.com/a</loc></url><url><loc>https://example.com/b</loc></url></urlset>"),
                CrawlBudget(60, 5000),
                1,
            )
        }
        assertEquals("SITEMAP_ENTRY_LIMIT", failure.code)
        val valid = parser.parse(
            response("<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'><url><loc>https://example.com/a</loc></url></urlset>"),
            CrawlBudget(60, 5000),
            1000,
        )
        assertEquals(listOf("https://example.com/a"), valid.urls)
        val compressed = ByteArrayOutputStream()
            .apply {
                GZIPOutputStream(this)
                    .use { it.write(ByteArray(2048) { 'a'.code.toByte() }) }
            }.toByteArray()
        val limited = SitemapDiscovery(CrawlConfiguration().apply { discovery.maxSitemapBytes = 1024 })
        val gzip = FetchResponse("https://example.com/sitemap.xml.gz".toHttpUrl(), 200, emptyMap(), compressed)
        assertEquals("SITEMAP_BODY_LIMIT", assertThrows(CrawlFailure::class.java) { limited.parse(gzip, CrawlBudget(60, 5000), 1000) }.code)
    }

    @Test
    fun `renderer uses ordinary destinations and protects parentheses and entity-shaped queries`() {
        val seed = page("<h1>Reference</h1><main>${"Useful information. ".repeat(20)}</main>")
        val context = SeedContext("GENERIC", "LOW", emptyList())
        val resource = DeterministicAnalyzer().resource(seed, seed, context)

        fun render(url: String) =
            LlmsRenderer(config).render(seed, context, listOf(resource.copy(preferredLinkUrl = url))).first.toString(Charsets.UTF_8)

        assertTrue(render("https://example.com/path").contains("](https://example.com/path)"))
        assertTrue(render("https://example.com/topic_(detail)").contains("](<https://example.com/topic_(detail)>)"))
        assertTrue(render("https://example.com/?value=&copy;").contains("value=&amp;copy;"))
        assertTrue(render("https://example.com/?value=\\(literal)").contains("](<https://example.com/?value=\\\\(literal)>)"))
    }
}
