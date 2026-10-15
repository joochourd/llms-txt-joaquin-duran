package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.CrawlResult
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import software.amazon.awssdk.core.exception.SdkClientException

@Tag("integration")
class OptionalCrawlFailureTest : MonitoringFixture() {
    @Test
    fun `unexpected programming failures escape optional sitemap work`() {
        val failure = IllegalStateException("Unexpected optional processing failure")

        val observed = assertThrows(IllegalStateException::class.java) {
            crawl { throw failure }
        }

        assertSame(failure, observed)
    }

    @Test
    fun `unexpected infrastructure failures escape optional sitemap work`() {
        val failure = SdkClientException.builder().message("Unexpected infrastructure failure").build()

        val observed = assertThrows(SdkClientException::class.java) {
            crawl { throw failure }
        }

        assertSame(failure, observed)
    }

    @Test
    fun `optional sitemap work propagates cancellation`() {
        val failure = assertThrows(CrawlFailure::class.java) {
            crawl { budget ->
                budget.cancelled.set(true)
                budget.remainingMillis()
                error("A cancelled budget must throw.")
            }
        }

        assertEquals("CANCELLED", failure.code)
    }

    @Test
    fun `optional sitemap work propagates an expired execution deadline`() {
        var reachedSitemap = false

        val failure = assertThrows(CrawlFailure::class.java) {
            crawl(durationMillis = 1000) { budget ->
                reachedSitemap = true
                Thread.sleep(budget.remainingMillis() + 5)
                budget.remainingMillis()
                error("An expired budget must throw.")
            }
        }

        assertTrue(reachedSitemap, "The deadline must expire during the optional phase.")
        assertEquals("TIME_BUDGET", failure.code)
    }

    @Test
    fun `expected remote sitemap failures preserve HTML discovery`() {
        val result = crawl { throw CrawlFailure("SOURCE_UNAVAILABLE", "The optional source is unavailable.") }

        assertTrue("SOURCE_UNAVAILABLE" in result.warnings)
        assertEquals("HTML", result.resources.single().linkFormat)
        assertEquals("VALID", result.checks["https://fixture.example/"])
    }

    @ParameterizedTest
    @ValueSource(strings = ["xml", "gzip", "unknown"])
    fun `malformed sitemap content preserves HTML discovery`(format: String) {
        val bytes = when (format) {
            "xml" -> "<urlset><url><loc>".toByteArray()
            "gzip" -> byteArrayOf(0x1f, 0x8b.toByte(), 0x08)
            else -> "<not-a-sitemap/>".toByteArray()
        }

        val result = crawl { bytes }

        assertTrue("SITEMAP_INVALID" in result.warnings)
        assertEquals("HTML", result.resources.single().linkFormat)
    }

    @ParameterizedTest
    @ValueSource(strings = ["element", "header"])
    fun `advertised Markdown alternatives are not fetched or substituted`(advertisement: String) {
        val requestedPaths = mutableListOf<String>()
        val alternate = if (advertisement == "element") {
            "<link rel='alternate' type='text/markdown' href='/seed.md'>"
        } else {
            ""
        }
        val headers = buildMap {
            put("Content-Type", listOf("text/html"))
            if (advertisement == "header") put("Link", listOf("</seed.md>; rel=alternate; type=text/markdown"))
        }
        val transport = PageTransport { url, _, _ ->
            requestedPaths.add(url.encodedPath)
            when (url.encodedPath) {
                "/" -> FetchResponse(url, 200, headers, html(alternate))
                "/seed.md" -> error("Markdown alternatives must not be fetched.")
                else -> FetchResponse(url, 404, emptyMap(), byteArrayOf())
            }
        }

        val result = run(transport)

        assertFalse("/seed.md" in requestedPaths)
        assertTrue(result.warnings.none { it.startsWith("MARKDOWN_") })
        val resource = result.resources.single()
        assertEquals("HTML", resource.linkFormat)
        assertEquals(resource.primaryHtmlUrl, resource.preferredLinkUrl)
        assertTrue(result.bytes.toString(Charsets.UTF_8).contains("https://fixture.example/"))
    }

    private fun crawl(
        durationMillis: Long = 5000,
        sitemapResponse: (CrawlBudget) -> ByteArray,
    ): CrawlResult {
        val transport = PageTransport { url, budget, _ ->
            when (url.encodedPath) {
                "/" -> FetchResponse(url, 200, mapOf("Content-Type" to listOf("text/html")), html())

                "/sitemap.xml" -> FetchResponse(
                    url,
                    200,
                    mapOf("Content-Type" to listOf("application/xml")),
                    sitemapResponse(budget),
                )

                else -> FetchResponse(url, 404, emptyMap(), byteArrayOf())
            }
        }
        return run(transport, durationMillis)
    }

    private fun run(
        transport: PageTransport,
        durationMillis: Long = 5000,
    ): CrawlResult {
        val run = queued()
        val fetcher = SafeFetcher(transport, origins, config, clock, UrlIdentityResolver(settings))
        return CrawlRunner(fetcher, config, clock).run(run, emptyList(), CrawlBudget(60, durationMillis)) { _, _ -> }
    }

    private fun html(head: String = ""): ByteArray = (
        "<html><head>$head</head><body><main><h1>Useful fixture seed</h1><p>" +
            "Practical fixture content with useful explanations. ".repeat(20) + "</p></main></body></html>"
    ).toByteArray()
}
