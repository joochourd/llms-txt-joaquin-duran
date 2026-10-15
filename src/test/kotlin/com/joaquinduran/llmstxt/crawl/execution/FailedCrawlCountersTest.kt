package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunCounters
import com.joaquinduran.llmstxt.ingestion.RunPhase
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.ExecutionStore
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import software.amazon.awssdk.core.exception.SdkClientException
import java.util.concurrent.atomic.AtomicInteger

@Tag("integration")
class FailedCrawlCountersTest : MonitoringFixture() {
    @ParameterizedTest
    @ValueSource(strings = ["robots", "seed", "redirect", "sitemap", "cancelled", "deadline"])
    fun `terminal failures retain every attempted HTTP request`(failurePoint: String) {
        if (failurePoint == "deadline") settings.maxCrawlDurationSeconds = 1
        val queuedRun = queued()
        val observedRequests = AtomicInteger()
        val html = "<html>" +
            "<body><main><h1>Useful fixture</h1><p>${"Useful fixture content. ".repeat(30)}</p></main></body></html>"
        val transport = PageTransport { url, budget, _ ->
            observedRequests.incrementAndGet()
            when {
                url.encodedPath == "/robots.txt" -> {
                    FetchResponse(
                        url,
                        if (failurePoint == "robots") 500 else 404,
                        emptyMap(),
                        byteArrayOf(),
                    )
                }

                failurePoint == "cancelled" -> {
                    budget.cancelled.set(true)
                    budget.remainingMillis()
                    error("A cancelled budget must throw.")
                }

                failurePoint == "deadline" -> {
                    Thread.sleep(budget.remainingMillis() + 5)
                    clock.now += settings.maxCrawlDurationSeconds * 1000
                    budget.remainingMillis()
                    error("An expired budget must throw.")
                }

                url.encodedPath == "/" && failurePoint == "redirect" -> {
                    FetchResponse(
                        url,
                        302,
                        mapOf("Location" to listOf("/unavailable")),
                        byteArrayOf(),
                    )
                }

                url.encodedPath == "/" && failurePoint == "sitemap" -> {
                    FetchResponse(
                        url,
                        200,
                        mapOf("Content-Type" to listOf("text/html")),
                        html.toByteArray(),
                    )
                }

                url.encodedPath == "/sitemap.xml" -> {
                    error("Unexpected optional processing failure")
                }

                else -> {
                    FetchResponse(url, 403, emptyMap(), byteArrayOf())
                }
            }
        }
        val runner = CrawlRunner(SafeFetcher(transport, origins, config, clock, UrlIdentityResolver(settings)), config, clock)
        val dispatcher = WorkDispatcher(execution, runner, origins, config, settings, clock)

        try {
            await({ store.run(queuedRun.runId)?.status?.terminal() == true }) { dispatcher.tick() }
            val terminal = requireNotNull(store.run(queuedRun.runId))
            val expectedRequests = when (failurePoint) {
                "robots" -> 1
                "redirect" -> 3
                "sitemap" -> 3
                else -> 2
            }

            assertEquals(if (failurePoint == "deadline") RunStatus.TIMED_OUT else RunStatus.FAILED, terminal.status)
            assertEquals(expectedRequests, observedRequests.get())
            assertEquals(expectedRequests.toLong(), terminal.counters.requests)
            assertEquals(1L, terminal.counters.pages)
            assertEquals(0L, store.capacity().outstandingRuns)
            assertEquals(0L, store.capacity().monitoredIdentities)

            val repeated = execution.finish(terminal.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)

            assertEquals(terminal.counters, repeated.counters)
            assertEquals(0L, store.capacity().outstandingRuns)
        } finally {
            dispatcher.close()
        }
    }

    @Test
    fun `final counters never regress persisted progress or alter an existing terminal outcome`() {
        val run = requireNotNull(execution.claim(queued()))
        val recorded = RunCounters(pages = 3, candidates = 5, requests = 7)
        execution.progress(run, RunPhase.DISCOVERING, recorded)

        val terminal = execution.finish(run.runId, RunStatus.FAILED, PublicationDecision.PRESERVED, counters = RunCounters(1, 2, 8))
        val repeated = execution.finish(run.runId, RunStatus.FAILED, PublicationDecision.PRESERVED, counters = RunCounters(9, 9, 9))

        assertEquals(RunCounters(pages = 3, candidates = 5, requests = 8), terminal.counters)
        assertEquals(terminal, repeated)
        assertEquals(0L, store.capacity().outstandingRuns)
        assertEquals(
            "CANCELLED",
            assertThrows(CrawlFailure::class.java) {
                execution.progress(run, RunPhase.DISCOVERING, RunCounters(9, 9, 9))
            }.code,
        )
    }

    @Test
    fun `ambiguous finalization commits counters and quota release together exactly once`() {
        val run = requireNotNull(execution.claim(queued()))
        val transactions = AtomicInteger()
        val adapter = AdmissionStore(
            proxy { name, _, call ->
                val response = call()
                if (name == "transactWriteItems" && transactions.incrementAndGet() == 1) {
                    throw SdkClientException.create("The committed response was lost.")
                }
                response
            },
            store.configuration,
        )
        val finalizer = ExecutionStore(adapter, settings, config, clock)
        val counters = RunCounters(pages = 1, requests = 2)

        val terminal = finalizer.finish(run.runId, RunStatus.FAILED, PublicationDecision.PRESERVED, counters = counters)

        assertEquals(counters, terminal.counters)
        assertEquals(1, transactions.get())
        assertEquals(0L, store.capacity().outstandingRuns)
        assertEquals(0L, store.capacity().monitoredIdentities)
        assertEquals(0L, requireNotNull(store.meta(run.identityId)).outstandingRunCount)
    }
}
