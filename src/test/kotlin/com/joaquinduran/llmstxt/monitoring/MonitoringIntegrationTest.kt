package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.api.errors.ApiProblem
import com.joaquinduran.llmstxt.crawl.execution.CrawlRunner
import com.joaquinduran.llmstxt.crawl.execution.WorkDispatcher
import com.joaquinduran.llmstxt.crawl.fetch.OriginBackoff
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.monitoring.MonitoringAdmissionKind
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.CrawlMetaCodec
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoSchema
import com.joaquinduran.llmstxt.persistence.dynamo.MonitoringStore
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.model.*
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Tag("integration")
class MonitoringIntegrationTest : MonitoringFixture() {
    @Test
    fun `disable and enable preserve the original monitoring window and count one slot`() {
        val original = canonical()
        val disabled = monitoring.change(original.identityId, false)
        assertEquals(MonitoringState.NONE, disabled.monitoringState)
        assertEquals(original.currentArtifactHash, disabled.currentArtifactHash)
        assertEquals(Capacity(0, 0), store.capacity())
        assertEquals(disabled, monitoring.change(original.identityId, false))
        val enabled = monitoring.change(original.identityId, true)
        val repeated = monitoring.change(original.identityId, true)
        assertEquals(enabled.monitoringExpiresAt, repeated.monitoringExpiresAt)
        assertEquals(enabled.nextCheckAt, repeated.nextCheckAt)
        assertEquals(Capacity(0, 1), store.capacity())
        clock.now += 10_000
        val enabledAgain = monitoring.change(original.identityId, true)
        assertEquals(original.monitoringExpiresAt, disabled.monitoringExpiresAt)
        assertEquals(enabled.monitoringExpiresAt, enabledAgain.monitoringExpiresAt)
        assertEquals(enabled.nextCheckAt, enabledAgain.nextCheckAt)
        assertEquals(original.acceptedObservationSequence, enabledAgain.acceptedObservationSequence)
    }

    @Test
    fun `controls require a canonical and quota rejection preserves existing bytes`() {
        val pending = queued()
        assertEquals("NO_RESULT", assertThrows(MonitoringRejected::class.java) { monitoring.change(pending.identityId, false) }.code)
        execution.finish(pending.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)
        val first = canonical("https://first.example/")
        val second = canonical("https://second.example/")
        monitoring.change(second.identityId, false)
        settings.maxMonitoredIdentities = 1
        assertEquals("MONITORING_CAPACITY_EXCEEDED", assertThrows(MonitoringRejected::class.java) { monitoring.change(second.identityId, true) }.code)
        assertEquals(second.currentArtifactHash, store.meta(second.identityId)?.currentArtifactHash)
        assertEquals(first.currentVersionSequence, store.meta(first.identityId)?.currentVersionSequence)
        assertEquals(Capacity(0, 1), store.capacity())
    }

    @Test
    fun `racing disable and enable reserve and release at most one durable slot`() {
        val meta = canonical()
        val pool = Executors.newFixedThreadPool(8)
        try {
            for (enabled in listOf(false, true, false)) {
                pool.invokeAll((1..8).map { Callable { monitoring.change(meta.identityId, enabled) } }).forEach { it.get() }
                assertEquals(Capacity(0, if (enabled) 1 else 0), store.capacity())
            }
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `simultaneous disable and enable reevaluate the same baseline and preserve balanced capacity`() {
        val original = canonical()
        clock.now += 1_000
        val baselineReads = CountDownLatch(2)
        val readCount = AtomicInteger()
        val racingStore = AdmissionStore(
            proxy { method, arguments, invoke ->
                val response = invoke()
                if (method == "getItem" &&
                    (arguments[0] as GetItemRequest).key() == DomainKeys.crawl(original.identityId) &&
                    readCount.getAndIncrement() < 2
                ) {
                    baselineReads.countDown()
                    check(baselineReads.await(10, TimeUnit.SECONDS)) { "Both actors must read the same accepted baseline." }
                }
                response
            },
            store.configuration,
        )
        val racingMonitoring = monitoringService(racingStore)
        val actors = Executors.newFixedThreadPool(2)

        try {
            actors
                .invokeAll(
                    listOf(
                        Callable { racingMonitoring.change(original.identityId, false) },
                        Callable { racingMonitoring.change(original.identityId, true) },
                    ),
                ).forEach { it.get() }
        } finally {
            actors.shutdownNow()
        }

        val winner = requireNotNull(store.meta(original.identityId))
        assertTrue(winner.metadataRevision > original.metadataRevision)
        assertTrue(winner.monitoringState in setOf(MonitoringState.NONE, MonitoringState.ACTIVE))
        assertEquals(winner.monitoringState == MonitoringState.ACTIVE, winner.monitoringSlotReserved)
        assertEquals(Capacity(0, if (winner.monitoringSlotReserved) 1 else 0), store.capacity())
        assertEquals(original.acceptedObservationSequence, winner.acceptedObservationSequence)
        assertEquals(original.currentVersionSequence, winner.currentVersionSequence)
        assertEquals(original.currentArtifactHash, winner.currentArtifactHash)
    }

    @Test
    fun `expired monitoring cannot be renewed and releases its slot exactly once`() {
        val original = canonical()
        clock.now = requireNotNull(original.monitoringExpiresAt)

        assertEquals("MONITORING_EXPIRED", assertThrows(MonitoringRejected::class.java) { monitoring.change(original.identityId, true) }.code)
        val expired = requireNotNull(store.meta(original.identityId))
        assertTrue(monitoring.expire(expired))
        monitoring.expire(expired)

        assertEquals(Capacity(0, 0), store.capacity())
        assertEquals(MonitoringState.EXPIRED, store.meta(original.identityId)?.monitoringState)
        assertEquals(original.currentArtifactHash, store.meta(original.identityId)?.currentArtifactHash)
    }

    @Test
    fun `disabled monitoring cannot start a new window after its original expiry`() {
        val original = canonical()
        monitoring.change(original.identityId, false)
        clock.now = requireNotNull(original.monitoringExpiresAt)

        assertEquals("MONITORING_EXPIRED", assertThrows(MonitoringRejected::class.java) { monitoring.change(original.identityId, true) }.code)
        assertEquals(Capacity(0, 0), store.capacity())
        assertEquals(original.currentArtifactHash, store.meta(original.identityId)?.currentArtifactHash)
    }

    @Test
    fun `reserved expiry waits for the running watchdog and cannot release pending generation`() {
        val run = requireNotNull(execution.claim(queued()))
        clock.now = requireNotNull(store.meta(run.identityId)?.monitoringReservationExpiresAt)
        assertTrue(monitoring.expire(requireNotNull(store.meta(run.identityId))))
        assertEquals(Capacity(1, 1), store.capacity())
        assertTrue(requireNotNull(store.meta(run.identityId)?.monitoringReservationExpiresAt) > clock.now)
        val runner = CrawlRunner(
            SafeFetcher(
                PageTransport {
                    _,
                    _,
                    _,
                    ->
                    fail("Expired work cannot fetch")
                },
                origins,
                config,
                clock,
                UrlIdentityResolver(settings),
            ),
            config,
            clock,
        )
        val dispatcher = WorkDispatcher(execution, runner, origins, config, settings, clock)
        try {
            await({ store.run(run.runId)?.status?.terminal() == true }) { dispatcher.tick() }
        } finally {
            dispatcher.close()
        }
        assertEquals(Capacity(0, 0), store.capacity())
        assertEquals(MonitoringState.NONE, store.meta(run.identityId)?.monitoringState)
    }

    @Test
    fun `internal checks bypass freshness and force cooldown while duplicate due attempts coalesce`() {
        config.monitoring.monitorIntervalSeconds = 60
        val meta = canonical()
        val before = requireNotNull(store.meta(meta.identityId))
        val run = due(meta.identityId)
        assertTrue(clock.now - requireNotNull(before.acceptedObservationStartedAt) < settings.crawlFreshnessTtlSeconds * 1000)
        assertEquals("MONITORING", run.trigger)
        assertNull(store.meta(meta.identityId)?.lastForceCreatedAt)
        assertEquals(MonitoringAdmissionKind.SKIPPED, monitoring.admit(before).kind)
        clock.now = requireNotNull(store.meta(meta.identityId)?.nextCheckAt)
        assertEquals(MonitoringAdmissionKind.COALESCED, monitoring.admit(requireNotNull(store.meta(meta.identityId))).kind)
        assertEquals(Capacity(1, 1), store.capacity())
    }

    @Test
    fun `racing scheduled admission produces one run with a short pending recheck`() {
        val meta = canonical()
        clock.now = requireNotNull(meta.nextCheckAt)
        val due = requireNotNull(store.meta(meta.identityId))
        val pool = Executors.newFixedThreadPool(8)
        try {
            val admitted = pool.invokeAll((1..8).map { Callable { monitoring.admit(due) } }).map { it.get() }
            assertEquals(1, admitted.count { it.kind == MonitoringAdmissionKind.ENQUEUED })
            assertEquals(Capacity(1, 1), store.capacity())
            assertTrue(requireNotNull(store.meta(meta.identityId)?.nextCheckAt) < clock.now + config.monitoring.monitorIntervalSeconds * 1000)
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `capacity local origin and durable backoff defer without a force charge`() {
        val meta = canonical()
        clock.now = requireNotNull(meta.nextCheckAt)
        settings.maxOutstandingRuns = 1
        val unrelated = queued("https://other.example/")
        assertEquals(MonitoringAdmissionKind.DEFERRED, monitoring.admit(requireNotNull(store.meta(meta.identityId))).kind)
        execution.finish(unrelated.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)
        clock.now = requireNotNull(store.meta(meta.identityId)?.nextCheckAt)
        requireNotNull(origins.tryCrawl("https://fixture.example")).use {
            assertEquals(MonitoringAdmissionKind.DEFERRED, monitoring.admit(requireNotNull(store.meta(meta.identityId))).kind)
        }
        clock.now = requireNotNull(store.meta(meta.identityId)?.nextCheckAt)
        val blocked = clock.now + 3_600_000
        OriginBackoff(store, config).record("https://fixture.example", blocked)
        assertEquals(MonitoringAdmissionKind.DEFERRED, monitoring.admit(requireNotNull(store.meta(meta.identityId))).kind)
        assertTrue(requireNotNull(store.meta(meta.identityId)?.nextCheckAt) >= blocked)
        assertEquals(Capacity(0, 1), store.capacity())
        assertNull(store.meta(meta.identityId)?.lastForceCreatedAt)
    }

    @Test
    fun `lost committed monitoring admission disable and expiry acknowledgements do not double counters`() {
        val meta = canonical()
        val wrapped = proxy { name, _, delegate ->
            val response = delegate()
            if (name == "transactWriteItems") throw SdkClientException.create("Injected lost committed acknowledgement")
            response
        }
        val adapter = monitoringService(AdmissionStore(wrapped, store.configuration))
        clock.now = requireNotNull(meta.nextCheckAt)
        val admitted = adapter.admit(requireNotNull(store.meta(meta.identityId)))
        assertEquals(MonitoringAdmissionKind.ENQUEUED, admitted.kind)
        assertEquals(Capacity(1, 1), store.capacity())
        adapter.change(meta.identityId, false)
        adapter.change(meta.identityId, false)
        assertEquals(Capacity(1, 0), store.capacity())
        val resumed = adapter.change(meta.identityId, true)
        clock.now = requireNotNull(resumed.monitoringExpiresAt)
        adapter.expire(requireNotNull(store.meta(meta.identityId)))
        assertEquals(Capacity(1, 0), store.capacity())
    }

    @Test
    fun `different identities racing for the last monitoring slot cannot exceed global capacity`() {
        val first = canonical("https://first.example/")
        val second = canonical("https://second.example/")
        monitoring.change(first.identityId, false)
        monitoring.change(second.identityId, false)
        settings.maxMonitoredIdentities = 1
        val pool = Executors.newFixedThreadPool(2)
        try {
            val outcomes = pool
                .invokeAll(
                    listOf(first, second).map { meta ->
                        Callable {
                            try {
                                monitoring.change(meta.identityId, true)
                                "ACTIVE"
                            } catch (exception: MonitoringRejected) {
                                exception.code
                            }
                        }
                    },
                ).map { it.get() }
            assertEquals(setOf("ACTIVE", "MONITORING_CAPACITY_EXCEEDED"), outcomes.toSet())
            assertEquals(Capacity(0, 1), store.capacity())
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `expired empty initial reservation is released once without relying on TTL`() {
        val identity = UrlIdentityResolver(settings).normalize("https://reservation.example/")
        val meta = CrawlMeta(
            identity.identityId,
            identity.normalizedSeedUrl,
            identity.intentProfile,
            settings.pipelineVersion,
            monitoringState = MonitoringState.RESERVED,
            monitoringSlotReserved = true,
            monitoringReservationExpiresAt = clock.now - 1,
        )
        store.putAbsent(CrawlMetaCodec.encode(meta))
        update(DynamoSchema.capacityKey, "SET monitoredIdentities = :one", mapOf(":one" to number(1)))
        assertTrue(monitoring.expire(meta))
        monitoring.expire(meta)
        assertEquals(Capacity(0, 0), store.capacity())
        assertFalse(requireNotNull(store.meta(identity.identityId)).monitoringSlotReserved)
    }
}
