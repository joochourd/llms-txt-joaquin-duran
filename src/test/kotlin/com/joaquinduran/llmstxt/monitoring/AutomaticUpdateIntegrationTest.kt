package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.crawl.execution.CrawlRunner
import com.joaquinduran.llmstxt.crawl.execution.WorkDispatcher
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.ingestion.submitAndProcess
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class AutomaticUpdateIntegrationTest : MonitoringFixture() {
    @Test
    fun `scheduler detects file and source changes without browser requests and preserves identical file bytes`() {
        config.monitoring.monitorIntervalSeconds = 15
        config.monitoring.monitorWindowSeconds = 600
        val startedAt = clock.millis()
        val batch = submitAndProcess(service, store, clock, listOf("https://monitoring-fixture.example/"), false)
        val identityId = requireNotNull(store.rows(batch.batchId, true).single().identityId)
        val transport = RegisteredFixtureTransport(clock, startedAt)
        val runner = CrawlRunner(SafeFetcher(transport, origins, config, clock, UrlIdentityResolver(settings)), config, clock)
        val dispatcher = WorkDispatcher(execution, runner, origins, config, settings, clock)
        val scheduler = scheduler()

        fun accepted(sequence: Long): CrawlMeta {
            await({ requireNotNull(store.meta(identityId)).acceptedObservationSequence >= sequence }) {
                scheduler.tick()
                dispatcher.tick()
            }
            return requireNotNull(store.meta(identityId))
        }

        try {
            val initial = accepted(1)
            assertTrue(execution.baseline(initial).any { it.sourceUrl.endsWith("/help") })
            clock.now = startedAt + 46_000
            val fileChanged = accepted(2)
            assertNotEquals(initial.currentArtifactHash, fileChanged.currentArtifactHash)
            assertTrue(execution.baseline(fileChanged).any { it.sourceUrl.endsWith("/guide") })
            val previousContentHash = execution.baseline(fileChanged).single { it.sourceUrl.endsWith("/product") }.contentHash

            clock.now = startedAt + 91_000
            val contentChanged = accepted(3)
            assertNotEquals(previousContentHash, execution.baseline(contentChanged).single { it.sourceUrl.endsWith("/product") }.contentHash)
            assertEquals(fileChanged.currentArtifactHash, contentChanged.currentArtifactHash)
            assertEquals(fileChanged.currentVersionSequence, contentChanged.currentVersionSequence)
            assertEquals(fileChanged.lastOutputUpdatedAt, contentChanged.lastOutputUpdatedAt)
            assertTrue(requireNotNull(contentChanged.lastSuccessfulValidationAt) > requireNotNull(fileChanged.lastSuccessfulValidationAt))

            clock.now = requireNotNull(contentChanged.nextCheckAt)
            val unchanged = accepted(4)
            assertEquals(fileChanged.currentArtifactHash, unchanged.currentArtifactHash)
            assertEquals(Capacity(0, 1), store.capacity())

            clock.now = requireNotNull(initial.monitoringExpiresAt)
            await({ store.meta(identityId)?.monitoringState == MonitoringState.EXPIRED }) { scheduler.tick() }
            assertEquals(Capacity(0, 0), store.capacity())
            assertEquals("MONITORING_EXPIRED", assertThrows(MonitoringRejected::class.java) { monitoring.change(identityId, true) }.code)
            assertEquals(fileChanged.currentArtifactHash, store.meta(identityId)?.currentArtifactHash)
        } finally {
            scheduler.close()
            dispatcher.close()
        }
    }
}
