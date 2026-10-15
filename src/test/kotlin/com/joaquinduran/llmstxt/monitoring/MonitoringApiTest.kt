package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.api.dto.ApiError
import com.joaquinduran.llmstxt.api.dto.CrawlResponse
import com.joaquinduran.llmstxt.api.dto.MonitoringResponse
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.number
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
class MonitoringApiTest : MonitoringFixture() {
    @Test
    fun `monitoring controls validate input and preserve current downloads without renewal or history routes`() {
        val meta = canonical()
        val pending = queued("https://pending.example/")
        val expiredExample = canonical("https://expired.example/")
        update(
            DomainKeys.crawl(expiredExample.identityId),
            "SET monitoringExpiresAt = :expired",
            mapOf(":expired" to number(System.currentTimeMillis() - 1_000)),
        )
        val properties = mapOf<String, Any>(
            "crawl.enabled" to false,
            "micronaut.server.port" to -1,
            "dynamodb.environment" to "local",
            "dynamodb.endpoint" to store.configuration.endpoint,
            "dynamodb.table" to store.table,
            "api-limits.submissions-per-minute-per-client" to 60000,
            "api-limits.submissions-per-minute-global" to 60000,
        )
        ApplicationContext.run(EmbeddedServer::class.java, properties).use { server ->
            HttpClient.newHttpClient().use { http ->
                val mapper = server.applicationContext.getBean(JsonMapper::class.java)

                fun request(
                    path: String,
                    body: String? = null,
                ): HttpResponse<String> {
                    val builder = HttpRequest.newBuilder(URI("${server.url}$path"))
                    if (body == null) {
                        builder.GET()
                    } else {
                        builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body))
                    }
                    return http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
                }
                val base = "/api/crawls/${meta.identityId}"
                val missing = request("/api/crawls/unknown/monitoring", "{\"enabled\":true}")
                assertEquals(404, missing.statusCode())
                assertEquals("NOT_FOUND", mapper.readValue(missing.body().toByteArray(), ApiError::class.java)?.code)
                val noResult = request("/api/crawls/${pending.identityId}/monitoring", "{\"enabled\":false}")
                assertEquals(409, noResult.statusCode())
                assertEquals("NO_RESULT", mapper.readValue(noResult.body().toByteArray(), ApiError::class.java)?.code)
                val expired = request("/api/crawls/${expiredExample.identityId}/monitoring", "{\"enabled\":true}")
                assertEquals(410, expired.statusCode())
                assertEquals("MONITORING_EXPIRED", mapper.readValue(expired.body().toByteArray(), ApiError::class.java)?.code)
                val crawl = request(base)
                assertEquals(200, crawl.statusCode())
                val product = requireNotNull(mapper.readValue(crawl.body().toByteArray(), CrawlResponse::class.java))
                assertEquals("ACTIVE", product.monitoring.state)
                assertTrue(product.monitoring.canEnable)
                assertEquals(1, product.selectedResources.size)
                assertEquals(404, request("$base/versions").statusCode())
                assertEquals(404, request("$base/validations").statusCode())
                assertEquals(404, request("$base/versions/1").statusCode())
                assertEquals(404, request("$base/validations/1").statusCode())
                assertEquals(404, request("$base/validations/0").statusCode())
                assertEquals(404, request("$base/versions/99").statusCode())
                for (body in listOf("{}", "[]", "{\"enabled\":\"false\"}", "{\"enabled\":null}")) {
                    assertEquals(400, request("$base/monitoring", body).statusCode())
                }
                val disabled = request("$base/monitoring", "{\"enabled\":false}")
                assertEquals(200, disabled.statusCode())
                assertEquals("NONE", requireNotNull(mapper.readValue(disabled.body().toByteArray(), MonitoringResponse::class.java)).state)
                assertEquals(200, request("/artifacts/${meta.identityId}/llms.txt").statusCode())
                assertEquals(200, request("$base/monitoring", "{\"enabled\":true}").statusCode())
                assertEquals(404, request("/api/demo-scenarios/availability").statusCode())
                assertEquals(404, request("/api/demo-scenarios", "").statusCode())
                assertEquals(404, request("$base/monitoring/extend", "").statusCode())
                assertFalse(crawl.body().contains("AttributeValue"))
            }
        }
    }
}
