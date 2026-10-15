package com.joaquinduran.llmstxt.ingestion

import com.joaquinduran.llmstxt.api.dto.BatchResponse
import com.joaquinduran.llmstxt.api.errors.ApiProblem
import com.joaquinduran.llmstxt.api.query.AdmissionStatus
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import io.micronaut.context.ApplicationContext
import io.micronaut.json.JsonMapper
import io.micronaut.runtime.server.EmbeddedServer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Tag("integration")
class SmallBatchBacklogIntegrationTest : MonitoringFixture() {
    @Test
    fun `one seven and ten input receipts remain pending alongside a full 250 input batch`() {
        settings.maxOpenBulkBatches = 5
        val large = service.submit(List(250) { "https://large-$it.example/" }, false)
        processor().use { processor ->
            repeat(2) { processor.process(large.batchId) }
        }
        assertEquals(40, store.capacity().outstandingRuns)

        for (size in listOf(1, 7, 10)) {
            val inputs = List(size) { "https://small-$size-$it.example/" }
            val received = service.submit(inputs, false)
            processor().use { it.process(received.batchId) }
            val status = AdmissionStatus(store, service, clock).batch(received.batchId)

            assertTrue(received.bulk)
            assertTrue(received.received)
            assertEquals(inputs, BulkBatchStore(store, clock).inputs(received, 0, size))
            assertEquals("ADMITTING", status.status)
            assertTrue(status.items.all { it.status == "PENDING" && it.error == null })
            assertTrue(store.rows(received.batchId, true).isEmpty())
            assertFalse(store.get(DomainKeys.batch(received.batchId)).containsKey("expiresAtEpochSeconds"))
        }
        assertEquals(40, store.capacity().outstandingRuns)
    }

    @Test
    fun `HTTP accepts seven inputs with a tracking location while 250 input work fills capacity`() {
        val large = service.submit(List(250) { "https://large-$it.example/" }, false)
        processor().use { processor ->
            repeat(2) { processor.process(large.batchId) }
        }
        val properties = mapOf<String, Any>(
            "crawl.enabled" to false,
            "micronaut.server.port" to -1,
            "dynamodb.environment" to "local",
            "dynamodb.endpoint" to requireNotNull(store.configuration.endpoint),
            "dynamodb.table" to store.table,
            "dynamodb.work-index-shards" to store.configuration.workIndexShards,
            "dynamodb.due-index-shards" to store.configuration.dueIndexShards,
        )

        ApplicationContext.run(EmbeddedServer::class.java, properties).use { server ->
            HttpClient.newHttpClient().use { http ->
                val inputs = List(7) { "https://small-$it.example/" }
                val body = "{\"urls\":[${inputs.joinToString(",") { "\"$it\"" }}]}"
                val request = HttpRequest
                    .newBuilder(URI("${server.url}/api/batches"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build()

                val response = http.send(request, HttpResponse.BodyHandlers.ofString())
                val mapper = server.applicationContext.getBean(JsonMapper::class.java)
                val receipt = requireNotNull(mapper.readValue(response.body().toByteArray(), BatchResponse::class.java))
                val tracking = response.headers().firstValue("Location").orElseThrow()
                val status = http.send(
                    HttpRequest.newBuilder(URI("${server.url}$tracking")).GET().build(),
                    HttpResponse.BodyHandlers.ofString(),
                )

                assertEquals(202, response.statusCode())
                assertEquals("/api/batches/${receipt.batchId}", tracking)
                assertEquals(inputs, receipt.items.map { it.inputUrl })
                assertTrue(receipt.items.all { it.status == "PENDING" && it.error == null })
                assertEquals(200, status.statusCode())
                assertEquals(40, store.capacity().outstandingRuns)
            }
        }
    }

    @Test
    fun `a new processor admits a small waiting receipt after capacity is released without resubmission`() {
        settings.maxOutstandingRuns = 1
        val occupied = queued("https://occupied.example/")
        val receipt = service.submit(listOf("https://waiting.example/"), false)
        processor().use { it.process(receipt.batchId) }
        clock.now += 48 * 60 * 60 * 1000
        execution.finish(occupied.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)

        processor().use { it.process(receipt.batchId) }
        val decision = requireNotNull(store.row(receipt.batchId, 0))
        val run = requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(decision.runId)))))
        publish(run)
        processor().use { processor ->
            repeat(2) { processor.process(receipt.batchId) }
        }

        assertEquals(Admission.QUEUED, decision.admission)
        assertEquals("COMPLETED", AdmissionStatus(store, service, clock).batch(receipt.batchId).status)
        assertFalse(requireNotNull(store.batch(receipt.batchId)).bulkSlotReserved)
        assertEquals(0, store.capacity().outstandingRuns)
    }

    @Test
    fun `small receipts resolve cached and shared inputs behind a capacity blocked input`() {
        settings.maxOutstandingRuns = 1
        val cached = canonical("https://cached.example/")
        val occupied = queued("https://occupied.example/")
        val receipt = service.submit(listOf("https://waiting.example/", cached.normalizedSeedUrl, occupied.normalizedSeedUrl), false)

        processor().use { it.process(receipt.batchId) }
        val rows = store.rows(receipt.batchId, true)

        assertNull(store.row(receipt.batchId, 0))
        assertEquals(listOf(Admission.CACHED, Admission.COALESCED), rows.map { it.admission })
        assertEquals(occupied.runId, rows.last().runId)
        assertEquals(1, store.capacity().outstandingRuns)
    }

    @Test
    fun `forced small receipts wait for capacity instead of returning fresh cached output`() {
        settings.maxOutstandingRuns = 1
        val cached = canonical("https://cached.example/")
        val occupied = queued("https://occupied.example/")
        val receipt = service.submit(listOf(cached.normalizedSeedUrl), true)
        processor().use { it.process(receipt.batchId) }
        assertNull(store.row(receipt.batchId, 0))

        execution.finish(occupied.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)
        processor().use { it.process(receipt.batchId) }

        assertEquals(Admission.QUEUED, store.row(receipt.batchId, 0)?.admission)
        assertEquals(1, store.capacity().outstandingRuns)
    }

    @Test
    fun `small receipts respect the durable backlog limit without leaking a reservation`() {
        settings.maxOpenBulkBatches = 1
        val accepted = service.submit(listOf("https://first.example/"), false)

        val rejected = assertThrows(ApiProblem::class.java) {
            service.submit(listOf("https://second.example/"), false)
        }

        assertEquals("BACKLOG_FULL", rejected.code)
        assertEquals(429, rejected.status)
        assertTrue(requireNotNull(store.batch(accepted.batchId)).received)
        assertEquals("1", store.get(DomainKeys.key("CAPACITY#BULK", "META")).getValue("activeBatches").n())
        assertEquals(0, store.capacity().outstandingRuns)
    }

    private fun processor() = BulkBatchProcessor(store, BulkBatchStore(store, clock, settings), service, clock)
}
