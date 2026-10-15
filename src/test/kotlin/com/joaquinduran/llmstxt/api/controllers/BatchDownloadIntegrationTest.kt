package com.joaquinduran.llmstxt.api.controllers

import com.joaquinduran.llmstxt.api.dto.ApiError
import com.joaquinduran.llmstxt.api.dto.BatchDownloadSummary
import com.joaquinduran.llmstxt.api.dto.BatchResponse
import com.joaquinduran.llmstxt.ingestion.BulkBatchProcessor
import com.joaquinduran.llmstxt.ingestion.submitAndProcess
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.ObservationKeys
import io.micronaut.context.ApplicationContext
import io.micronaut.json.JsonMapper
import io.micronaut.runtime.server.EmbeddedServer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import java.io.ByteArrayInputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.zip.ZipInputStream

@Tag("integration")
class BatchDownloadIntegrationTest : MonitoringFixture() {
    private fun withServer(action: (EmbeddedServer, HttpClient, JsonMapper) -> Unit) {
        val properties = mapOf<String, Any>(
            "crawl.enabled" to false,
            "micronaut.server.port" to -1,
            "dynamodb.environment" to "local",
            "dynamodb.endpoint" to store.configuration.endpoint,
            "dynamodb.table" to store.table,
            "api-limits.reads-per-minute-per-client" to 60000,
        )
        ApplicationContext.run(EmbeddedServer::class.java, properties).use { server ->
            HttpClient.newHttpClient().use { http -> action(server, http, server.applicationContext.getBean(JsonMapper::class.java)) }
        }
    }

    private fun get(
        server: EmbeddedServer,
        http: HttpClient,
        path: String,
        etag: String? = null,
    ): HttpResponse<ByteArray> {
        val builder = HttpRequest.newBuilder(URI("${server.url}$path")).GET()
        if (etag != null) builder.header("If-None-Match", etag)
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray())
    }

    private fun entries(bytes: ByteArray): Map<String, ByteArray> = ZipInputStream(ByteArrayInputStream(bytes)).use { zip ->
        buildMap {
            while (true) {
                val entry = zip.nextEntry ?: break
                assertFalse(containsKey(entry.name), "Archive entries must be unique.")
                put(entry.name, zip.readAllBytes())
            }
        }
    }

    @Test
    fun `conditional downloads keep their HTTP contract without loading missing artifact bytes`() {
        val accepted = canonical()
        val hash = requireNotNull(accepted.currentArtifactHash)
        client.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(store.table)
                .key(DomainKeys.key("CRAWL#${accepted.identityId}", "RESULT"))
                .build(),
        )

        withServer { server, http, _ ->
            val paths = listOf(
                "/artifacts/${accepted.identityId}/llms.txt",
            )
            for (path in paths) {
                for (validator in listOf("\"$hash\"", "W/\"$hash\"", "*")) {
                    val cached = get(server, http, path, validator)

                    assertEquals(304, cached.statusCode())
                    assertEquals("\"$hash\"", cached.headers().firstValue("ETag").orElseThrow())
                    assertEquals(0, cached.body().size)
                }
                assertEquals(503, get(server, http, path).statusCode())
            }
        }
    }

    @Test
    fun `archive contains current bytes for each row and reports failures and duplicate filenames`() {
        clock.now = System.currentTimeMillis()
        val first = canonical("https://first.example/docs/")
        canonical("https://second.example/")
        val batch = submitAndProcess(
            service,
            store,
            clock,
            listOf(
                first.normalizedSeedUrl,
                first.normalizedSeedUrl,
                "https://second.example/",
                "ftp://invalid.example/",
            ),
            false,
        )
        val next = requireNotNull(execution.claim(due(first.identityId)))
        publish(next, result(next, "changed"))
        withServer { server, http, mapper ->
            val response = get(server, http, "/api/batches/${batch.batchId}/download")
            assertEquals(200, response.statusCode())
            assertEquals("application/zip", response.headers().firstValue("Content-Type").orElseThrow())
            assertEquals(
                "attachment; filename=\"llms-batch-${batch.batchId.take(8)}.zip\"",
                response.headers().firstValue("Content-Disposition").orElseThrow(),
            )
            val archive = entries(response.body())
            assertEquals(setOf("first.example-docs.txt", "first.example-docs-2.txt", "second.example.txt", "batch-summary.json"), archive.keys)
            val current = get(server, http, "/artifacts/${first.identityId}/llms.txt")
            assertEquals("attachment; filename=\"first.example-docs.txt\"", current.headers().firstValue("Content-Disposition").orElseThrow())
            assertArrayEquals(current.body(), archive["first.example-docs.txt"])
            assertArrayEquals(current.body(), archive["first.example-docs-2.txt"])
            val conditional = get(server, http, "/artifacts/${first.identityId}/llms.txt", current.headers().firstValue("ETag").orElseThrow())
            assertEquals(304, conditional.statusCode())
            assertEquals(0, conditional.body().size)
            val summary = requireNotNull(mapper.readValue(archive.getValue("batch-summary.json"), BatchDownloadSummary::class.java))
            assertEquals(batch.batchId, summary.batchId)
            assertEquals(4, summary.items.size)
            assertEquals("CURRENT", summary.resultPolicy)
            assertEquals(next.observationSequence, summary.items[0].fileRevision)
            assertEquals(store.meta(first.identityId)?.currentArtifactHash, summary.items[0].outputHash)
            assertEquals("FAILED", summary.items[3].status)
            assertNull(summary.items[3].fileName)
            assertNotNull(summary.items[3].error)
        }
    }

    @Test
    fun `unfinished empty and missing batches return explicit errors instead of empty archives`() {
        clock.now = System.currentTimeMillis()
        val pending = submitAndProcess(service, store, clock, listOf("https://pending.example/"), false)
        val failed = submitAndProcess(service, store, clock, listOf("ftp://invalid.example/"), false)
        withServer { server, http, mapper ->
            for ((id, expectedStatus, code) in listOf(
                Triple(pending.batchId, 409, "BATCH_NOT_READY"),
                Triple(failed.batchId, 409, "NO_BATCH_RESULTS"),
                Triple("missing", 404, "NOT_FOUND"),
            )) {
                val response = get(server, http, "/api/batches/$id/download")
                assertEquals(expectedStatus, response.statusCode())
                assertEquals(code, requireNotNull(mapper.readValue(response.body(), ApiError::class.java)).code)
            }
        }
    }

    @Test
    fun `unavailable current bytes fail the whole request rather than silently omitting a result`() {
        clock.now = System.currentTimeMillis()
        val first = canonical()
        val batch = submitAndProcess(service, store, clock, listOf(first.normalizedSeedUrl), false)
        client.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(store.table)
                .key(DomainKeys.key("CRAWL#${first.identityId}", "RESULT"))
                .build(),
        )
        withServer { server, http, mapper ->
            val response = get(server, http, "/api/batches/${batch.batchId}/download")
            assertEquals(503, response.statusCode())
            assertEquals("STORAGE_UNAVAILABLE", requireNotNull(mapper.readValue(response.body(), ApiError::class.java)).code)
        }
    }

    @Test
    fun `result pages preserve original positions and provide a continuation without repeating rows`() {
        clock.now = System.currentTimeMillis()
        val accepted = canonical()
        val batch = submitAndProcess(
            service,
            store,
            clock,
            listOf(accepted.normalizedSeedUrl, accepted.normalizedSeedUrl, accepted.normalizedSeedUrl, "ftp://invalid.example/"),
            false,
        )

        withServer { server, http, mapper ->
            val first = get(server, http, "/api/batches/${batch.batchId}?offset=0&limit=2")
            val second = get(server, http, "/api/batches/${batch.batchId}?offset=2&limit=2")
            val firstPage = requireNotNull(mapper.readValue(first.body(), BatchResponse::class.java))
            val secondPage = requireNotNull(mapper.readValue(second.body(), BatchResponse::class.java))

            assertEquals(200, first.statusCode())
            assertEquals(listOf(0, 1), firstPage.items.map { it.index })
            assertEquals(listOf(2, 3), secondPage.items.map { it.index })
            assertTrue(first.body().toString(Charsets.UTF_8).contains("\"nextOffset\":2"))
            assertTrue(first.body().toString(Charsets.UTF_8).contains("\"totalItems\":4"))
            assertEquals("FAILED", secondPage.items.last().status)
        }
    }

    @Test
    fun `a ready result page downloads while a later page is still processing`() {
        clock.now = System.currentTimeMillis()
        val accepted = canonical()
        val batch = submitAndProcess(service, store, clock, listOf(accepted.normalizedSeedUrl, "https://pending.example/"), false)

        withServer { server, http, mapper ->
            val ready = get(server, http, "/api/batches/${batch.batchId}/download?offset=0&limit=1")
            val pending = get(server, http, "/api/batches/${batch.batchId}/download?offset=1&limit=1")

            assertEquals(200, ready.statusCode())
            val archive = entries(ready.body())
            val summary = requireNotNull(mapper.readValue(archive.getValue("batch-summary.json"), BatchDownloadSummary::class.java))
            assertEquals(listOf(0), summary.items.map { it.index })
            assertTrue(archive.getValue("batch-summary.json").toString(Charsets.UTF_8).contains("\"nextOffset\":1"))
            assertEquals(409, pending.statusCode())
        }
    }

    @Test
    fun `result and archive page bounds reject invalid or excessive requests`() {
        clock.now = System.currentTimeMillis()
        val accepted = canonical()
        val batch = submitAndProcess(service, store, clock, listOf(accepted.normalizedSeedUrl), false)

        withServer { server, http, _ ->
            for (suffix in listOf("", "/download")) {
                for (query in listOf("offset=-1", "limit=0", "limit=101")) {
                    val response = get(server, http, "/api/batches/${batch.batchId}$suffix?$query")

                    assertEquals(400, response.statusCode(), query)
                }
            }
        }
    }

    @Test
    fun `one thousand received inputs return a tracking ID and bounded pending pages before crawl admission`() {
        clock.now = System.currentTimeMillis()
        val inputs = (0 until 1000).map { "https://site-$it.example/documentation/getting-started" }

        withServer { server, http, mapper ->
            val submitted = http.send(
                HttpRequest
                    .newBuilder(URI("${server.url}/api/batches"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(mapOf("urls" to inputs))))
                    .build(),
                HttpResponse.BodyHandlers.ofByteArray(),
            )
            assertEquals(202, submitted.statusCode())
            val receipt = requireNotNull(mapper.readValue(submitted.body(), BatchResponse::class.java))
            val finalPage = get(server, http, "/api/batches/${receipt.batchId}?offset=900&limit=100")
            val results = requireNotNull(mapper.readValue(finalPage.body(), BatchResponse::class.java))

            assertEquals(1000, receipt.totalItems)
            assertEquals(100, receipt.items.size)
            assertEquals(100, receipt.nextOffset)
            assertEquals(0, receipt.admittedItems)
            assertTrue(receipt.items.all { it.status == "PENDING" })
            assertEquals(inputs.takeLast(100), results.items.map { it.inputUrl })
            assertEquals((900 until 1000).toList(), results.items.map { it.index })
            assertNull(results.nextOffset)
            assertEquals("/api/batches/${receipt.batchId}", submitted.headers().firstValue("Location").orElseThrow())
            assertEquals(0L, store.capacity().outstandingRuns)
        }
    }

    @Test
    fun `one thousand cached rows download in ten bounded ZIPs with original positions and unique names`() {
        clock.now = System.currentTimeMillis()
        val accepted = canonical()
        val batch = service.submit(List(1000) { accepted.normalizedSeedUrl }, false)
        BulkBatchProcessor(store, BulkBatchStore(store, clock), service, clock).use { processor ->
            repeat(50) { processor.process(batch.batchId) }
        }
        assertFalse(requireNotNull(store.batch(batch.batchId)).bulkSlotReserved)

        withServer { server, http, mapper ->
            val filenames = mutableSetOf<String>()
            for (offset in 0 until 1000 step 100) {
                val response = get(server, http, "/api/batches/${batch.batchId}/download?offset=$offset&limit=100")
                assertEquals(200, response.statusCode())
                val archive = entries(response.body())
                val summary = requireNotNull(mapper.readValue(archive.getValue("batch-summary.json"), BatchDownloadSummary::class.java))

                assertEquals(101, archive.size)
                assertEquals((offset until offset + 100).toList(), summary.items.map { it.index })
                assertEquals(1000, summary.totalItems)
                assertEquals(offset, summary.offset)
                assertEquals((offset + 100).takeIf { it < 1000 }, summary.nextOffset)
                for (item in summary.items) {
                    assertTrue(filenames.add(requireNotNull(item.fileName)))
                    assertEquals(accepted.currentArtifactHash, item.outputHash)
                }
            }
            assertEquals(1000, filenames.size)
        }
    }
}
