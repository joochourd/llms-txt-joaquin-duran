package com.joaquinduran.llmstxt.ingestion

import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.base
import com.joaquinduran.llmstxt.persistence.dynamo.bool
import com.joaquinduran.llmstxt.persistence.dynamo.number
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class UnrestrictedAdmissionIntegrationTest : MonitoringFixture() {
    @Test
    fun `receipts are accepted beyond the previous maximum open batch count`() {
        val receipts = (1..25).map { position ->
            service.submit(listOf("https://waiting-$position.example/"), false)
        }

        assertEquals(25, receipts.map { it.batchId }.toSet().size)
        assertTrue(receipts.all { store.batch(it.batchId)?.received == true })
        assertEquals(Capacity(0, 0), store.capacity())
    }

    @Test
    fun `legacy backlog counters no longer prevent receipts or get rewritten`() {
        val legacyCounterKey = DomainKeys.key("CAPACITY#BULK", "META")
        store.putAbsent(base("CAPACITY#BULK", "META", "BulkCapacity") + mapOf("activeBatches" to number(100)))
        val legacyCounter = store.get(legacyCounterKey)

        val receipt = service.submit(listOf("https://accepted.example/"), false)

        assertTrue(store.batch(receipt.batchId)?.received == true)
        assertEquals(legacyCounter, store.get(legacyCounterKey))
    }

    @Test
    fun `legacy receipts finish without depending on or modifying the former backlog counter`() {
        val receipt = service.submit(List(12) { "ftp://invalid.example/" }, false)
        val receiptKey = DomainKeys.batch(receipt.batchId)
        update(receiptKey, "SET bulkSlotReserved = :reserved", mapOf(":reserved" to bool(true)))
        val legacyCounterKey = DomainKeys.key("CAPACITY#BULK", "META")
        store.putAbsent(base("CAPACITY#BULK", "META", "BulkCapacity") + mapOf("activeBatches" to number(100)))
        val legacyCounter = store.get(legacyCounterKey)

        BulkBatchProcessor(store, BulkBatchStore(store, clock), service, clock).use { processor ->
            repeat(2) { processor.process(receipt.batchId) }
        }
        val finished = store.get(receiptKey)

        assertFalse(requireNotNull(store.batch(receipt.batchId)).workPending)
        assertFalse(finished.containsKey("workPk"))
        assertFalse(finished.containsKey("bulkSlotReserved"))
        assertTrue(finished.containsKey("expiresAtEpochSeconds"))
        assertEquals(12, store.rows(receipt.batchId, true).size)
        assertEquals(legacyCounter, store.get(legacyCounterKey))
    }

    @Test
    fun `a completed forced run can be immediately forced again while active work still coalesces`() {
        val metadata = canonical()
        val firstBatch = submitAndProcess(service, store, clock, listOf(metadata.normalizedSeedUrl), true)
        val firstDecision = requireNotNull(store.row(firstBatch.batchId, 0))
        val firstRun = requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(firstDecision.runId)))))
        publish(firstRun)
        update(DomainKeys.crawl(metadata.identityId), "SET lastForceCreatedAt = :now", mapOf(":now" to number(clock.millis())))

        val secondBatch = submitAndProcess(service, store, clock, listOf(metadata.normalizedSeedUrl), true)
        val secondDecision = requireNotNull(store.row(secondBatch.batchId, 0))
        val duplicateBatch = submitAndProcess(service, store, clock, listOf(metadata.normalizedSeedUrl), true)
        val duplicateDecision = requireNotNull(store.row(duplicateBatch.batchId, 0))

        assertEquals(Admission.QUEUED, secondDecision.admission)
        assertNotEquals(firstDecision.runId, secondDecision.runId)
        assertEquals(Admission.COALESCED, duplicateDecision.admission)
        assertEquals(secondDecision.runId, duplicateDecision.runId)
        assertEquals(1, store.capacity().outstandingRuns)
    }
}
