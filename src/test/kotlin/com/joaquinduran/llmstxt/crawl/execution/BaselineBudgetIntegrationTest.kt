package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.CurrentResultCodec
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class BaselineBudgetIntegrationTest : MonitoringFixture() {
    private fun priorResult(): CrawlMeta {
        settings.maxPages = 30
        settings.maxSelectedResources = 20
        val initial = requireNotNull(execution.claim(queued()))
        val template = result(initial).resources.single()
        val resources = (1..11).map { index ->
            val url = "https://fixture.example/prior-$index"
            template.copy(resourceKey = sha256(url), sourceUrl = url, primaryHtmlUrl = url, preferredLinkUrl = url, aliases = listOf(url))
        }
        publish(initial, result(initial).copy(resources = resources))
        settings.maxPages = 10
        settings.maxSelectedResources = 10
        clock.now += 61_000
        return requireNotNull(store.meta(initial.identityId))
    }

    private fun refresh(meta: CrawlMeta): CrawlRun {
        val batch = submitAndProcess(service, store, clock, listOf(meta.normalizedSeedUrl), true)
        return requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(store.rows(batch.batchId, true).single().runId)))))
    }

    private fun runner(
        seen: MutableList<String>,
        forbidden: String? = null,
    ): CrawlRunner {
        val links = (1..20).joinToString("") { "<a href='/new-$it'>New guide $it</a>" }
        val html = "<h1>Integration documentation</h1><main>${"Useful integration source guidance. ".repeat(20)}$links</main>"
        val source = PageTransport { url, _, _ ->
            seen.add(url.encodedPath)
            val status = when (url.encodedPath) {
                "/robots.txt", "/sitemap.xml" -> 404
                forbidden -> 403
                else -> 200
            }
            FetchResponse(url, status, mapOf("Content-Type" to listOf("text/html")), html.toByteArray())
        }
        return CrawlRunner(SafeFetcher(source, origins, config, clock, UrlIdentityResolver(settings)), config, clock)
    }

    @Test
    fun `reduced exploration budget still revalidates the complete previous selection without fetching new pages`() {
        val previous = priorResult()
        val run = refresh(previous)
        val seen = mutableListOf<String>()

        val generated = runner(seen).run(run, execution.baseline(previous), CrawlBudget(60, 5000)) { _, _ -> }
        val accepted = publish(run, generated)

        assertEquals(RunStatus.COMPLETED, accepted.status)
        assertEquals(12, generated.coverage.pagesAttempted)
        assertEquals(11, seen.count { it.startsWith("/prior-") })
        assertFalse(seen.any { it.startsWith("/new-") })
        assertTrue(generated.resources.size <= 10)
        assertTrue("BASELINE_REVALIDATION" in generated.warnings)
    }

    @Test
    fun `a failed prior source still preserves canonical bytes and identifies the failed resource`() {
        val previous = priorResult()
        val before = CurrentResultCodec.bytes(store.get(CurrentResultCodec.key(previous.identityId)))
        val run = refresh(previous)
        val seen = mutableListOf<String>()

        val failure = assertThrows(CrawlFailure::class.java) {
            runner(seen, "/prior-9").run(run, execution.baseline(previous), CrawlBudget(60, 5000)) { _, _ -> }
        }
        execution.finish(run.runId, RunStatus.FAILED, PublicationDecision.PRESERVED, DomainError(failure.code, requireNotNull(failure.message)))

        assertEquals("INCOMPLETE_BASELINE", failure.code)
        assertTrue(failure.message.orEmpty().contains("/prior-9"))
        assertTrue(failure.message.orEmpty().contains("HTTP_403"))
        assertEquals(previous.acceptedObservationSequence, store.meta(previous.identityId)?.acceptedObservationSequence)
        assertArrayEquals(before, CurrentResultCodec.bytes(store.get(CurrentResultCodec.key(previous.identityId))))
        assertEquals(0, store.capacity().outstandingRuns)
    }

    @Test
    fun `baseline revalidation never extends the HTTP request budget`() {
        val previous = priorResult()
        val run = refresh(previous)
        val seen = mutableListOf<String>()
        val budget = CrawlBudget(5, 5000)

        val failure = assertThrows(CrawlFailure::class.java) {
            runner(seen).run(run, execution.baseline(previous), budget) { _, _ -> }
        }

        assertEquals("INCOMPLETE_BASELINE", failure.code)
        assertTrue(budget.requests <= 5)
        assertTrue(seen.size <= 5)
        assertEquals(previous.currentArtifactHash, store.meta(previous.identityId)?.currentArtifactHash)
    }
}
