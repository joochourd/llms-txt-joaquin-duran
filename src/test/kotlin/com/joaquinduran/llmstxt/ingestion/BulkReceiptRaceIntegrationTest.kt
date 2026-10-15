package com.joaquinduran.llmstxt.ingestion

import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException

@Tag("integration")
class BulkReceiptRaceIntegrationTest : MonitoringFixture() {
    @Test
    fun `a seal winning against an expired receipt snapshot preserves all accepted inputs`() {
        val (receipt, inFlightSeal) = unsealedReceipt()
        var sealCommitted = false
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                if (startsRetention(method, arguments) && !sealCommitted) {
                    client.transactWriteItems(inFlightSeal)
                    sealCommitted = true
                }
                delegate()
            },
            store.configuration,
        )
        clock.now = requireNotNull(receipt.receiptDeadlineAt)

        val expired = BulkBatchStore(adapter, clock, settings).expireReceipt(receipt)
        val accepted = requireNotNull(store.batch(receipt.batchId))

        assertTrue(sealCommitted)
        assertFalse(expired)
        assertTrue(accepted.received)
        assertTrue(accepted.bulkSlotReserved)
        assertEquals(0, accepted.nextCompletionIndex)
        assertEquals(11, BulkBatchStore(store, clock, settings).inputs(accepted, 0, 11).size)
    }

    @Test
    fun `receipt cleanup winning prevents a previously prepared seal from acknowledging abandoned inputs`() {
        val (receipt, inFlightSeal) = unsealedReceipt()
        var sealRejected = false
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                val result = delegate()
                if (startsRetention(method, arguments)) {
                    try {
                        client.transactWriteItems(inFlightSeal)
                    } catch (_: TransactionCanceledException) {
                        sealRejected = true
                    }
                }
                result
            },
            store.configuration,
        )
        clock.now = requireNotNull(receipt.receiptDeadlineAt)

        assertTrue(BulkBatchStore(adapter, clock, settings).expireReceipt(receipt))
        val abandoned = requireNotNull(store.batch(receipt.batchId))

        assertTrue(sealRejected)
        assertFalse(abandoned.received)
        assertFalse(abandoned.bulkSlotReserved)
        assertEquals(0L, store.capacity().outstandingRuns)
    }

    private fun unsealedReceipt(): Pair<Batch, TransactWriteItemsRequest> {
        var seal: TransactWriteItemsRequest? = null
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                if (method == "transactWriteItems") {
                    val request = arguments[0] as TransactWriteItemsRequest
                    if (request
                            .transactItems()
                            .first()
                            .update()
                            ?.updateExpression()
                            ?.startsWith("SET received =") == true
                    ) {
                        seal = request
                        throw SdkClientException.builder().message("Seal response interrupted before delivery").build()
                    }
                }
                delegate()
            },
            store.configuration,
        )
        val receiver = AdmissionService(adapter, settings, UrlIdentityResolver(settings), clock)
        assertThrows(StorageUnavailable::class.java) { receiver.submit(List(11) { "ftp://invalid.example/" }, false) }
        val request = requireNotNull(seal)
        val batchId = request
            .transactItems()
            .first()
            .update()
            .key()
            .getValue("pk")
            .s()
            .removePrefix("BATCH#")
        return requireNotNull(store.batch(batchId)) to request
    }

    private fun startsRetention(
        method: String,
        arguments: Array<out Any?>,
    ): Boolean = method == "transactWriteItems" &&
        (arguments[0] as TransactWriteItemsRequest)
            .transactItems()
            .first()
            .update()
            ?.updateExpression()
            ?.startsWith("SET nextCompletionIndex = :total") == true
}
