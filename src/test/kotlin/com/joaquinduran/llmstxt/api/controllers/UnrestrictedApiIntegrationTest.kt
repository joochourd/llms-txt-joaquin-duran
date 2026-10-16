package com.joaquinduran.llmstxt.api.controllers

import com.joaquinduran.llmstxt.api.dto.BatchResponse
import com.joaquinduran.llmstxt.ingestion.submitAndProcess
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
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
class UnrestrictedApiIntegrationTest : MonitoringFixture() {
    @Test
    fun `submission bursts persist more than five receipts while execution capacity remains full`() {
        settings.maxOutstandingRuns = 1
        queued("https://occupied.example/")

        withServer { server, http ->
            val mapper = server.applicationContext.getBean(JsonMapper::class.java)
            val batchIds = (1..12).map { position ->
                val request = HttpRequest
                    .newBuilder(URI("${server.url}/api/batches"))
                    .header("Content-Type", "application/json")
                    .header("X-Real-IP", "198.51.100.10")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"urls\":[\"https://waiting-$position.example/\"],\"force\":true}"))
                    .build()

                val response = http.send(request, HttpResponse.BodyHandlers.ofString())

                assertEquals(202, response.statusCode(), response.body())
                val receipt = requireNotNull(mapper.readValue(response.body().toByteArray(), BatchResponse::class.java))
                assertTrue(receipt.items.all { it.status == "PENDING" })
                assertEquals("/api/batches/${receipt.batchId}", response.headers().firstValue("Location").orElseThrow())
                receipt.batchId
            }

            assertEquals(12, batchIds.toSet().size)
            assertTrue(batchIds.all { store.batch(it)?.received == true })
            assertEquals(1, store.capacity().outstandingRuns)
        }
    }

    @Test
    fun `result resources artifacts and batch pages remain readable beyond the former client quota`() {
        val metadata = canonical()
        val batch = submitAndProcess(service, store, clock, listOf(metadata.normalizedSeedUrl), false)
        val paths = listOf(
            "/api/crawls/${metadata.identityId}",
            "/api/crawls/${metadata.identityId}/resources",
            "/artifacts/${metadata.identityId}/llms.txt",
            "/api/batches/${batch.batchId}",
            "/api/runs/${metadata.lastAttemptRunId}",
            "/api/batches/${batch.batchId}/download",
        )

        withServer { server, http ->
            repeat(21) {
                for (path in paths) {
                    val request = HttpRequest.newBuilder(URI("${server.url}$path")).GET().build()

                    val response = http.send(request, HttpResponse.BodyHandlers.ofByteArray())

                    assertEquals(200, response.statusCode(), path)
                }
            }
        }
    }

    private fun withServer(action: (EmbeddedServer, HttpClient) -> Unit) {
        val properties = mapOf<String, Any>(
            "crawl.enabled" to false,
            "micronaut.server.port" to -1,
            "dynamodb.environment" to "local",
            "dynamodb.endpoint" to store.configuration.endpoint,
            "dynamodb.table" to store.table,
        )
        ApplicationContext.run(EmbeddedServer::class.java, properties).use { server ->
            HttpClient.newHttpClient().use { http -> action(server, http) }
        }
    }
}
