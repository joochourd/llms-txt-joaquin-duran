package com.joaquinduran.llmstxt.ingestion

import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest

@Tag("integration")
class BulkBatchIntegrationTest : MonitoringFixture() {
    @Test
    fun `one thousand inputs are durably received before creating crawl runs`() {
        val inputs = (0 until 1_000).map { "https://site-$it.example/docs" }

        val received = service.submit(inputs, false)
        val metadata = store.get(DomainKeys.batch(received.batchId))
        val chunks = client
            .query(
                QueryRequest
                    .builder()
                    .tableName(store.table)
                    .keyConditionExpression("pk = :pk AND begins_with(sk, :input)")
                    .expressionAttributeValues(
                        mapOf(
                            ":pk" to DomainKeys.batch(received.batchId).getValue("pk"),
                            ":input" to software.amazon.awssdk.services.dynamodb.model.AttributeValue
                                .builder()
                                .s("INPUT#")
                                .build(),
                        ),
                    ).build(),
            ).items()
        val storedInputs = chunks.sortedBy { it.getValue("sk").s() }.flatMap { it.getValue("inputs").l().map { value -> value.s() } }

        assertFalse(received.admissionComplete)
        assertEquals("1000", metadata.getValue("totalInputs").n())
        assertTrue(metadata.getValue("received").bool())
        assertTrue(metadata["inputs"]?.l().isNullOrEmpty())
        assertEquals(inputs, storedInputs)
        assertTrue(store.rows(received.batchId, true).isEmpty())
        assertEquals(Capacity(0, 0), store.capacity())
    }

    @Test
    fun `oversized bulk input is rejected before durable receipt or reservations`() {
        val inputs = List(11) { "https://example.com/${"x".repeat(3_000)}" }

        val failure = assertThrows(com.joaquinduran.llmstxt.api.errors.ApiProblem::class.java) {
            service.submit(inputs, false)
        }

        assertEquals(400, failure.status)
        assertEquals(Capacity(0, 0), store.capacity())
    }

    private fun processor() = BulkBatchProcessor(store, BulkBatchStore(store, clock), service, clock)

    @Test
    fun `capacity exhaustion keeps remaining inputs pending and a new processor resumes the durable cursor`() {
        settings.maxOutstandingRuns = 2
        val received = service.submit((0 until 12).map { "https://site-$it.example/" }, false)
        processor().use { it.process(received.batchId) }
        val first = requireNotNull(store.batch(received.batchId))

        assertEquals(2, first.nextAdmissionIndex)
        assertEquals(2, store.rows(received.batchId, true).size)
        assertEquals(Capacity(2, 2), store.capacity())
        assertTrue(store.rows(received.batchId, true).all { it.admission == Admission.QUEUED })

        val row = store.rows(received.batchId, true).first()
        val run = requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(row.runId)))))
        publish(run)
        processor().use { it.process(received.batchId) }

        assertEquals(3, store.batch(received.batchId)?.nextAdmissionIndex)
        assertEquals(3, store.rows(received.batchId, true).size)
        assertEquals(2, store.capacity().outstandingRuns)
    }

    @Test
    fun `shared and cached bulk decisions release a backlog slot only after all positions are terminal`() {
        val inputs = List(12) { "https://shared.example/" }
        val batch = service.submit(inputs, false)
        processor().use { it.process(batch.batchId) }
        val rows = store.rows(batch.batchId, true)

        assertEquals(12, rows.size)
        assertEquals(1, rows.mapNotNull { it.runId }.distinct().size)
        assertTrue(requireNotNull(store.batch(batch.batchId)).bulkSlotReserved)
        val run = requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(rows.first().runId)))))
        publish(run)
        processor().use { it.process(batch.batchId) }
        processor().use { it.process(batch.batchId) }

        assertFalse(requireNotNull(store.batch(batch.batchId)).bulkSlotReserved)
        assertEquals("0", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
        assertEquals(Capacity(0, 1), store.capacity())

        val cached = service.submit(inputs, false)
        processor().use { it.process(cached.batchId) }
        assertTrue(store.rows(cached.batchId, true).all { it.admission == Admission.CACHED })
        assertFalse(requireNotNull(store.batch(cached.batchId)).bulkSlotReserved)
        assertEquals("0", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
    }

    @Test
    fun `a full backlog rejects another receipt without leaking its slot`() {
        settings.maxOpenBulkBatches = 1
        val accepted = service.submit(List(11) { "https://first.example/" }, false)

        val failure = assertThrows(com.joaquinduran.llmstxt.api.errors.ApiProblem::class.java) {
            service.submit(List(11) { "https://second.example/" }, false)
        }

        assertEquals(429, failure.status)
        assertEquals("BACKLOG_FULL", failure.code)
        assertTrue(requireNotNull(store.batch(accepted.batchId)).received)
        assertEquals("1", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
    }

    @Test
    fun `lost receipt and cursor acknowledgements resolve from durable witnesses without duplicate reservations`() {
        val lost = mutableSetOf<String>()
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                val result = delegate()
                if (method == "transactWriteItems") {
                    val request = arguments[0] as TransactWriteItemsRequest
                    val operation = request.transactItems().first().let { item ->
                        item
                            .put()
                            ?.item()
                            ?.get("sk")
                            ?.s() ?: item.update()?.updateExpression().orEmpty()
                    }
                    if (lost.add(operation)) throw SdkClientException.builder().message("Lost acknowledgement").build()
                }
                result
            },
            store.configuration,
        )
        val receiver = AdmissionService(adapter, settings, UrlIdentityResolver(settings), clock)

        val accepted = receiver.submit(List(120) { "ftp://invalid.example/" }, false)

        assertTrue(requireNotNull(store.batch(accepted.batchId)).received)
        assertEquals("1", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
        assertEquals(120, BulkBatchStore(store, clock).inputs(accepted, 0, 120).size)
        assertTrue(lost.size >= 3)

        BulkBatchProcessor(adapter, BulkBatchStore(adapter, clock), receiver, clock).use { processor ->
            repeat(6) { processor.process(accepted.batchId) }
            processor.process(accepted.batchId)
        }

        val completed = requireNotNull(store.batch(accepted.batchId))
        assertEquals(120, completed.nextAdmissionIndex)
        assertEquals(120, completed.nextCompletionIndex)
        assertEquals(120, completed.resolvedInputs)
        assertFalse(completed.bulkSlotReserved)
        assertEquals("0", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
        assertTrue(lost.any { it.startsWith("SET nextAdmissionIndex") })
        assertTrue(lost.any { it.startsWith("SET nextCompletionIndex") })
        assertTrue(lost.any { it.startsWith("SET bulkSlotReserved") })
    }

    @Test
    fun `an interrupted unacknowledged upload releases its slot after the receipt deadline`() {
        var interruptedBatchId: String? = null
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                if (method == "transactWriteItems") {
                    val request = arguments[0] as TransactWriteItemsRequest
                    val chunk = request
                        .transactItems()
                        .firstOrNull()
                        ?.put()
                        ?.item()
                    if (chunk?.get("sk")?.s()?.startsWith("INPUT#") == true) {
                        interruptedBatchId = chunk.getValue("pk").s().removePrefix("BATCH#")
                        throw SdkClientException.builder().message("Upload interrupted").build()
                    }
                }
                delegate()
            },
            store.configuration,
        )
        val receiver = AdmissionService(adapter, settings, UrlIdentityResolver(settings), clock)

        assertThrows(StorageUnavailable::class.java) { receiver.submit(List(11) { "https://interrupted.example/" }, false) }
        val id = requireNotNull(interruptedBatchId)
        val before = requireNotNull(store.batch(id))
        assertFalse(before.received)
        assertTrue(before.bulkSlotReserved)
        clock.now = requireNotNull(before.receiptDeadlineAt)
        processor().use { it.process(id) }
        processor().use { it.process(id) }

        assertFalse(requireNotNull(store.batch(id)).bulkSlotReserved)
        assertEquals("0", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
        assertEquals(Capacity(0, 0), store.capacity())
    }

    @Test
    fun `one thousand distinct identities drain through four admission slots without losing inputs`() {
        settings.maxOutstandingRuns = 4
        settings.maxMonitoredIdentities = 50
        val batch = service.submit((0 until 1000).map { "https://site-$it.example/" }, false)
        var admittedThrough = 0
        var passes = 0
        processor().use { processor ->
            while (requireNotNull(store.batch(batch.batchId)).bulkSlotReserved) {
                assertTrue(++passes <= 280, "Admission and bounded retention checkpoints must finish without retry sleeps.")
                processor.process(batch.batchId)
                val current = requireNotNull(store.batch(batch.batchId))
                val count = current.nextAdmissionIndex - admittedThrough
                assertTrue(store.capacity().outstandingRuns <= 4)
                if (count > 0) {
                    for (row in store.rowsPage(batch.batchId, admittedThrough, count)) {
                        val queued = requireNotNull(store.run(requireNotNull(row.runId)))
                        publish(requireNotNull(execution.claim(queued)))
                    }
                    admittedThrough = current.nextAdmissionIndex
                }
            }
        }

        assertEquals(1000, admittedThrough)
        assertEquals((0 until 1000).toList(), store.rows(batch.batchId, true).map { it.index })
        assertEquals(Capacity(0, 50), store.capacity())
        assertEquals("0", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
    }

    @Test
    fun `missing accepted input fails closed without releasing the recoverable receipt`() {
        val batch = service.submit(List(11) { "https://pending.example/" }, false)
        val initial = store.get(DomainKeys.batch(batch.batchId))
        assertFalse(initial.containsKey("expiresAtEpochSeconds"))

        clock.now = batch.expiresAtEpochSeconds * 1000
        client.deleteItem(
            software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
                .builder()
                .tableName(store.table)
                .key(DomainKeys.key("BATCH#${batch.batchId}", "INPUT#${DomainKeys.ordered(0)}"))
                .build(),
        )
        processor().use { processor ->
            assertThrows(StorageUnavailable::class.java) { processor.process(batch.batchId) }
        }

        val retained = store.get(DomainKeys.batch(batch.batchId))
        assertFalse(retained.containsKey("expiresAtEpochSeconds"))
        assertTrue(requireNotNull(store.batch(batch.batchId)).bulkSlotReserved)
        assertEquals("1", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
    }

    @Test
    fun `a new processor resumes partial retention before releasing the bulk slot once`() {
        val batch = service.submit(List(250) { "ftp://invalid.example/" }, false)
        processor().use { processor ->
            repeat(3) { processor.process(batch.batchId) }
        }
        val checkpoint = requireNotNull(store.batch(batch.batchId))

        assertEquals(250, checkpoint.nextCompletionIndex)
        assertEquals(99, checkpoint.nextRetentionIndex)
        assertTrue(checkpoint.bulkSlotReserved)
        assertEquals("1", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())

        processor().use { processor ->
            repeat(4) { processor.process(batch.batchId) }
        }
        val completed = requireNotNull(store.batch(batch.batchId))

        assertFalse(completed.bulkSlotReserved)
        assertEquals(255, completed.nextRetentionIndex)
        assertEquals(250, completed.resolvedInputs)
        assertTrue(store.rows(batch.batchId, true).all { it.expiresAtEpochSeconds == completed.expiresAtEpochSeconds })
        assertEquals("0", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
    }

    @Test
    fun `incompatible backlog counters fail closed without resetting stored data`() {
        update(
            DomainKeys.key("CAPACITY#BULK", "META"),
            "SET activeBatches = :bad",
            mapOf(
                ":bad" to software.amazon.awssdk.services.dynamodb.model.AttributeValue
                    .builder()
                    .n("-1")
                    .build(),
            ),
        )

        assertThrows(StorageUnavailable::class.java) { service.submit(List(11) { "https://example.com/" }, false) }

        assertEquals("-1", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
    }
}
