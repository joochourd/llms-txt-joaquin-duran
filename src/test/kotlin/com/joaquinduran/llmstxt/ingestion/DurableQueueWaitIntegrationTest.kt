package com.joaquinduran.llmstxt.ingestion

import com.joaquinduran.llmstxt.api.query.AdmissionStatus
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest

@Tag("integration")
class DurableQueueWaitIntegrationTest : MonitoringFixture() {
    @Test
    fun `queued work still coalesces after ten hours without reserving another slot`() {
        val original = queued()
        clock.now += 10 * 60 * 60 * 1000

        val repeated = submitAndProcess(service, store, clock, listOf(original.normalizedSeedUrl), false)
        val decision = store.rows(repeated.batchId, true).single()

        assertEquals(Admission.COALESCED, decision.admission)
        assertEquals(original.runId, decision.runId)
        assertEquals(Capacity(1, 1), store.capacity())
    }

    @Test
    fun `execution budgets and publication start when a worker claims long waiting work`() {
        val pending = queued()
        clock.now += 10 * 60 * 60 * 1000
        val startedAt = clock.now
        assertTrue(monitoring.expire(requireNotNull(store.meta(pending.identityId))))
        assertEquals(Capacity(1, 1), store.capacity())

        val running = requireNotNull(execution.claim(pending))
        val terminal = publish(running)

        assertEquals(startedAt, running.startedAt)
        assertEquals(startedAt + settings.maxCrawlDurationSeconds * 1000, running.deadlineAt)
        assertEquals(RunStatus.COMPLETED, terminal.status)
        assertEquals(Capacity(0, 1), store.capacity())
    }

    @Test
    fun `a capacity blocked input does not delay later fresh cached decisions`() {
        settings.maxOutstandingRuns = 1
        val cached = canonical("https://cached.example/")
        queued("https://occupied.example/")
        val batch = service.submit(listOf("https://pending.example/") + List(10) { cached.normalizedSeedUrl }, false)

        processor().use { it.process(batch.batchId) }
        val rows = store.rows(batch.batchId, true)
        val page = AdmissionStatus(store, service, clock).batch(batch.batchId)

        assertNull(store.row(batch.batchId, 0))
        assertEquals((1..10).toList(), rows.map { it.index })
        assertTrue(rows.all { it.admission == Admission.CACHED })
        assertEquals(10, page.admittedItems)
        assertEquals(Capacity(1, 2), store.capacity())
    }

    @Test
    fun `bulk admission remains pending beyond twenty four hours and resumes when capacity is freed`() {
        settings.maxOutstandingRuns = 1
        val occupied = queued("https://occupied.example/")
        val batch = service.submit((0 until 11).map { "https://site-$it.example/" }, false)
        clock.now += 48 * 60 * 60 * 1000
        execution.finish(occupied.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)

        processor().use { it.process(batch.batchId) }
        val rows = store.rows(batch.batchId, true)

        assertEquals(1, rows.size)
        assertEquals(Admission.QUEUED, rows.single().admission)
        assertFalse(requireNotNull(store.batch(batch.batchId)).admissionComplete)
    }

    @Test
    fun `active bulk inputs have no TTL and result retention begins only after completion`() {
        val batch = service.submit(List(11) { "ftp://invalid.example/" }, false)
        val inputKey = DomainKeys.key("BATCH#${batch.batchId}", "INPUT#${DomainKeys.ordered(0)}")
        assertFalse(store.get(inputKey).containsKey("expiresAtEpochSeconds"))
        clock.now += 10L * 24 * 60 * 60 * 1000

        processor().use { processor ->
            repeat(4) { processor.process(batch.batchId) }
        }
        val finished = requireNotNull(store.batch(batch.batchId))
        val page = AdmissionStatus(store, service, clock).batch(batch.batchId)

        assertEquals("COMPLETED", page.status)
        assertEquals(11, page.items.size)
        assertFalse(finished.workPending)
        assertTrue(finished.expiresAtEpochSeconds >= clock.now / 1000 + settings.batchRetentionDays * 86400)
        assertTrue(store.get(inputKey).containsKey("expiresAtEpochSeconds"))
        assertTrue(store.get(DomainKeys.batchItem(batch.batchId, 0)).containsKey("expiresAtEpochSeconds"))
    }

    @Test
    fun `expired terminal run details do not hide pending work or prevent bulk completion`() {
        val batch = service.submit((0 until 11).map { "https://site-$it.example/" }, false)
        processor().use { it.process(batch.batchId) }
        val first = requireNotNull(store.row(batch.batchId, 0))
        val finished = publish(requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(first.runId))))))
        clock.now += 10L * 24 * 60 * 60 * 1000
        client.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(store.table)
                .key(DomainKeys.run(finished.runId))
                .build(),
        )

        processor().use { it.process(batch.batchId) }
        val waiting = AdmissionStatus(store, service, clock).batch(batch.batchId)

        assertEquals("EXPIRED", waiting.items.first().status)
        assertEquals(
            "RUN_DETAILS_EXPIRED",
            waiting.items
                .first()
                .error
                ?.code,
        )
        assertNotNull(waiting.items.first().artifactUrl)
        assertEquals("QUEUED", waiting.items[1].status)
        assertEquals(1, requireNotNull(store.batch(batch.batchId)).nextCompletionIndex)

        for (row in store.rows(batch.batchId, true).drop(1)) {
            publish(requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(row.runId))))))
        }
        processor().use { processor ->
            repeat(3) { processor.process(batch.batchId) }
        }

        assertEquals("COMPLETED", AdmissionStatus(store, service, clock).batch(batch.batchId).status)
        assertFalse(requireNotNull(store.batch(batch.batchId)).workPending)
        assertEquals(0L, store.capacity().outstandingRuns)
    }

    @Test
    fun `missing run details cannot disguise a still reserved execution as expired`() {
        val batch = service.submit(List(11) { "https://pending.example/" }, false)
        processor().use { it.process(batch.batchId) }
        val runId = requireNotNull(store.row(batch.batchId, 0)?.runId)
        client.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(store.table)
                .key(DomainKeys.run(runId))
                .build(),
        )

        assertThrows(StorageUnavailable::class.java) { AdmissionStatus(store, service, clock).batch(batch.batchId) }
        processor().use { it.process(batch.batchId) }

        assertEquals(0, requireNotNull(store.batch(batch.batchId)).nextCompletionIndex)
        assertTrue(requireNotNull(store.batch(batch.batchId)).workPending)
        assertEquals(1L, store.capacity().outstandingRuns)
    }

    @Test
    fun `generation succeeds without a monitoring slot and monitoring can be enabled later`() {
        settings.maxMonitoredIdentities = 1
        val monitored = canonical("https://monitored.example/")

        val running = requireNotNull(execution.claim(queued("https://unmonitored.example/")))
        assertEquals(RunStatus.COMPLETED, publish(running).status)
        val generated = requireNotNull(store.meta(running.identityId))

        assertTrue(generated.hasCanonical())
        assertEquals(MonitoringState.NONE, generated.monitoringState)
        assertFalse(generated.monitoringSlotReserved)
        assertNull(generated.nextCheckAt)
        assertNotNull(generated.monitoringExpiresAt)
        assertEquals(Capacity(0, 1), store.capacity())

        monitoring.change(monitored.identityId, false)
        val enabled = monitoring.change(generated.identityId, true)

        assertEquals(MonitoringState.ACTIVE, enabled.monitoringState)
        assertEquals(generated.monitoringExpiresAt, enabled.monitoringExpiresAt)
        assertEquals(Capacity(0, 1), store.capacity())
    }

    private fun processor() = BulkBatchProcessor(store, BulkBatchStore(store, clock), service, clock)
}
