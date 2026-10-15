package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicInteger

@Tag("integration")
class RejectedDispatchTest : MonitoringFixture() {
    @Test
    fun `rejected submission releases local ownership and leaves durable work recoverable`() {
        val candidate = queued()
        val requests = AtomicInteger()
        val transport = PageTransport { _, _, _ ->
            requests.incrementAndGet()
            error("A rejected worker must never fetch a source.")
        }
        val runner = CrawlRunner(SafeFetcher(transport, origins, config, clock, UrlIdentityResolver(settings)), config, clock)
        val dispatcher = WorkDispatcher(execution, runner, origins, config, settings, clock)
        val workers = field<ThreadPoolExecutor>(dispatcher, "workers")
        val active = field<ConcurrentHashMap<String, CrawlBudget>>(dispatcher, "active")
        val slots = field<Semaphore>(dispatcher, "slots")
        try {
            // The dispatcher still admits/claims; only actual executor submission is rejected.
            workers.shutdown()
            await({ store.run(candidate.runId)?.status == RunStatus.RUNNING }) { dispatcher.tick() }
            val claimed = requireNotNull(store.run(candidate.runId))
            val registrationsAfterRejection = active.keys.toSet()
            val permitsAfterRejection = slots.availablePermits()

            assertNotNull(claimed.attemptId)
            assertNotNull(claimed.observationSequence)
            assertTrue(claimed.capacityReserved)
            assertNotNull(claimed.workPk)
            assertEquals(1L, store.capacity().outstandingRuns)

            // One available origin permit, never two: rejection must neither leak nor over-release.
            requireNotNull(origins.tryCrawl(claimed.originHint)).use {
                val extra = origins.tryCrawl(claimed.originHint)
                try {
                    assertNull(extra)
                } finally {
                    extra?.close()
                }
            }
            clock.now = requireNotNull(claimed.recoveryAt) + 1
            await({ store.run(claimed.runId)?.status == RunStatus.TIMED_OUT }) { dispatcher.tick() }
            dispatcher.tick()

            assertAll(
                { assertTrue(registrationsAfterRejection.isEmpty(), "Rejected work must leave no local active registration.") },
                { assertEquals(config.execution.maxConcurrentCrawls, permitsAfterRejection) },
                { assertTrue(active.isEmpty(), "Watchdog recovery must not retain a stale local registration.") },
                { assertEquals(config.execution.maxConcurrentCrawls, slots.availablePermits(), "Local permits must be released exactly once.") },
                { assertEquals(0, requests.get()) },
                { assertFalse(requireNotNull(store.run(claimed.runId)).capacityReserved) },
                { assertEquals(0L, store.capacity().outstandingRuns) },
                { assertEquals(0L, store.capacity().monitoredIdentities) },
                { assertEquals(0L, requireNotNull(store.meta(claimed.identityId)).outstandingRunCount) },
            )
        } finally {
            dispatcher.close()
        }
    }

    // Inspect private ownership without adding a production injection/diagnostic API.
    @Suppress("UNCHECKED_CAST")
    private fun <T> field(
        dispatcher: WorkDispatcher,
        name: String,
    ): T = WorkDispatcher::class.java
        .getDeclaredField(name)
        .apply { isAccessible = true }
        .get(dispatcher) as T
}
