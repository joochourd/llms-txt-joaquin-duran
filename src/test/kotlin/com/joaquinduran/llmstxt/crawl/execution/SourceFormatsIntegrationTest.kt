package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.ingestion.DomainError
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

@Tag("integration")
class SourceFormatsIntegrationTest : MonitoringFixture() {
    private val text = "This concise guide explains the supported installation steps."

    private fun transport(
        mime: String,
        body: String,
        seen: MutableList<String>,
        robotsStatus: Int = 404,
        robotsBody: String = "",
        seedStatus: Int = 200,
    ) = PageTransport { url, _, _ ->
        seen.add(url.encodedPath)
        when (url.encodedPath) {
            "/robots.txt" -> FetchResponse(url, robotsStatus, mapOf("Content-Type" to listOf("text/plain")), robotsBody.toByteArray())
            "/" -> FetchResponse(url, seedStatus, mapOf("Content-Type" to listOf(mime)), body.toByteArray())
            else -> FetchResponse(url, 404, emptyMap(), byteArrayOf())
        }
    }

    private fun runner(transport: PageTransport) = CrawlRunner(
        SafeFetcher(transport, origins, config, clock, UrlIdentityResolver(settings)),
        config,
        clock,
    )

    @ParameterizedTest
    @ValueSource(ints = [401, 403, 404, 410])
    fun `unavailable robots policy permits a normal source request without implying source authorization`(robotsStatus: Int) {
        val run = requireNotNull(execution.claim(queued()))
        val seen = mutableListOf<String>()
        val value = runner(transport("text/html", "<h1>Installation guide</h1><main>$text</main>", seen, robotsStatus))
            .run(run, emptyList(), CrawlBudget(60, 5000)) { _, _ -> }

        assertEquals(listOf("/robots.txt", "/", "/sitemap.xml"), seen)
        assertEquals(RunStatus.COMPLETED, publish(run, value).status)
        assertTrue("LIMITED_SEED_CONTENT" in value.warnings)
    }

    @ParameterizedTest
    @ValueSource(ints = [429, 500, 503])
    fun `robots rate limits and server errors still prevent source fetches`(robotsStatus: Int) {
        val run = requireNotNull(execution.claim(queued()))
        val seen = mutableListOf<String>()

        assertThrows(CrawlFailure::class.java) {
            runner(transport("text/html", "<h1>Installation guide</h1><main>$text</main>", seen, robotsStatus))
                .run(run, emptyList(), CrawlBudget(60, 5000)) { _, _ -> }
        }

        assertEquals(listOf("/robots.txt"), seen)
    }

    @Test
    fun `explicit robots disallow and source forbidden remain terminal failures`() {
        val run = requireNotNull(execution.claim(queued()))
        val seen = mutableListOf<String>()
        val blocked = assertThrows(CrawlFailure::class.java) {
            runner(transport("text/html", text, seen, 200, "User-agent: *\nDisallow: /\n"))
                .run(run, emptyList(), CrawlBudget(60, 5000)) { _, _ -> }
        }
        val forbidden = assertThrows(CrawlFailure::class.java) {
            runner(transport("text/html", text, seen, seedStatus = 403))
                .run(run, emptyList(), CrawlBudget(60, 5000)) { _, _ -> }
        }

        assertEquals("ROBOTS_DISALLOWED", blocked.code)
        assertEquals("SEED_UNAVAILABLE", forbidden.code)
        assertTrue(forbidden.message.orEmpty().contains("403"))
    }

    @ParameterizedTest
    @ValueSource(strings = ["text/html", "text/markdown", "application/json"])
    fun `short standalone sources publish without child pages in each supported format`(mime: String) {
        val run = requireNotNull(execution.claim(queued()))
        val seen = mutableListOf<String>()
        val body = when (mime) {
            "text/html" -> "<h1>Installation guide</h1><main>$text</main>"
            "text/markdown" -> "# Installation guide\n\n$text"
            else -> """{"title":"Installation guide","description":"$text"}"""
        }

        val value = runner(transport(mime, body, seen)).run(run, emptyList(), CrawlBudget(60, 5000)) { _, _ -> }
        val published = publish(run, value)

        assertEquals(RunStatus.COMPLETED, published.status)
        assertEquals(1, value.resources.size)
        assertEquals(1, value.coverage.pagesAttempted)
        assertTrue("LIMITED_SEED_CONTENT" in value.warnings)
        assertEquals("https://fixture.example/", value.resources.single().preferredLinkUrl)
    }

    @Test
    fun `mixed format discovery never calls described operations references or unsafe links`() {
        val run = requireNotNull(execution.claim(queued()))
        val seen = mutableListOf<String>()
        val source = PageTransport { url, _, _ ->
            seen.add(url.toString())
            val mime: String
            val body: String
            when (url.encodedPath) {
                "/robots.txt", "/sitemap.xml" -> {
                    return@PageTransport FetchResponse(url, 404, emptyMap(), byteArrayOf())
                }

                "/" -> {
                    mime = "text/html"
                    body = "<h1>Installation guide</h1><main>$text<a href='/guide.md'>Guide</a><a href='/api.json'>API reference</a></main>"
                }

                "/guide.md" -> {
                    mime = "text/markdown"
                    body = "# Deployment guide\n\n$text\n\n[Private](http://127.0.0.1/admin)\n\n![Tracking](https://outside.example/pixel.png)"
                }

                "/api.json" -> {
                    mime = "application/json"
                    body =
                        """
                        {"openapi":"3.1.0","info":{"title":"Installation API reference","description":"$text"},
                        "paths":{"/danger":{"delete":{"summary":"Delete an installation"}}},
                        "components":{"schemas":{"Hidden":{"${'$'}ref":"http://127.0.0.1/private"}}}}
                        """.trimIndent()
                }

                else -> {
                    error("Unexpected source request: $url")
                }
            }
            FetchResponse(url, 200, mapOf("Content-Type" to listOf(mime)), body.toByteArray())
        }

        val value = runner(source).run(run, emptyList(), CrawlBudget(60, 5000)) { _, _ -> }
        publish(run, value)
        val accepted = execution.baseline(requireNotNull(store.meta(run.identityId)))

        assertEquals(setOf("HTML", "MARKDOWN", "JSON"), accepted.map { it.linkFormat }.toSet())
        assertEquals(setOf("/", "/guide.md", "/api.json", "/robots.txt", "/sitemap.xml"), seen.map { java.net.URI(it).path }.toSet())
        assertTrue(seen.all { it.startsWith("https://fixture.example/") })
    }

    @Test
    fun `unreachable robots transport never grants implicit access to the source`() {
        val run = requireNotNull(execution.claim(queued()))
        val seen = mutableListOf<String>()
        val unavailable = PageTransport { url, _, _ ->
            seen.add(url.encodedPath)
            throw CrawlFailure("FETCH_FAILED", "Simulated network deadline")
        }

        val failure = assertThrows(CrawlFailure::class.java) {
            runner(unavailable).run(run, emptyList(), CrawlBudget(60, 5000)) { _, _ -> }
        }

        assertEquals("FETCH_FAILED", failure.code)
        assertEquals(listOf("/robots.txt"), seen)
    }

    @Test
    fun `a short replacement cannot remove an unvalidated previously accepted resource`() {
        val initial = requireNotNull(execution.claim(queued()))
        val source = result(initial).resources.single()
        val guideUrl = "https://fixture.example/guide"
        val guide = source.copy(
            resourceKey = com.joaquinduran.llmstxt.ingestion
                .sha256(guideUrl),
            sourceUrl = guideUrl,
            primaryHtmlUrl = guideUrl,
            preferredLinkUrl = guideUrl,
            aliases = listOf(guideUrl),
        )
        val previous = result(initial).copy(
            resources = listOf(guide),
            checks = mapOf(guideUrl to "VALID"),
            bytes = "# Installation guide\n\n## Resources\n\n- [Guide](<$guideUrl>): Previously verified guide.\n".toByteArray(),
        )
        publish(initial, previous)
        val before = requireNotNull(store.meta(initial.identityId))
        clock.now += 61_000
        val receipt = com.joaquinduran.llmstxt.ingestion
            .submitAndProcess(service, store, clock, listOf(initial.normalizedSeedUrl), true)
        val refresh = requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(store.rows(receipt.batchId, true).single().runId)))))
        val seen = mutableListOf<String>()
        val failedSource = PageTransport { url, _, _ ->
            seen.add(url.encodedPath)
            when (url.encodedPath) {
                "/robots.txt", "/sitemap.xml" -> FetchResponse(url, 404, emptyMap(), byteArrayOf())
                "/" -> FetchResponse(url, 200, mapOf("Content-Type" to listOf("text/markdown")), "# Installation guide\n\n$text".toByteArray())
                else -> FetchResponse(url, 500, emptyMap(), byteArrayOf())
            }
        }

        val failure = assertThrows(CrawlFailure::class.java) {
            runner(failedSource).run(refresh, execution.baseline(before), CrawlBudget(60, 5000)) { _, _ -> }
        }
        execution.finish(refresh.runId, RunStatus.FAILED, PublicationDecision.PRESERVED, DomainError(failure.code, requireNotNull(failure.message)))

        assertEquals("INCOMPLETE_BASELINE", failure.code)
        assertEquals(before.currentArtifactHash, store.meta(initial.identityId)?.currentArtifactHash)
        assertEquals(before.acceptedObservationSequence, store.meta(initial.identityId)?.acceptedObservationSequence)
    }
}
