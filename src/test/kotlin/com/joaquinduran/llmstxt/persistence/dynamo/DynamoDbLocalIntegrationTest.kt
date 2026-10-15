package com.joaquinduran.llmstxt.persistence.dynamo

import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID

@Tag("integration")
class DynamoDbLocalIntegrationTest {
    @Test
    fun `bootstrap is idempotent and HTTP readiness detects damaged infrastructure`() {
        val endpoint = requireNotNull(System.getenv("DYNAMODB_ENDPOINT")) {
            "DYNAMODB_ENDPOINT must be explicit for integration tests. Start DynamoDB Local first."
        }
        require(endpoint.isNotBlank()) { "DYNAMODB_ENDPOINT must be explicit and nonempty for integration tests." }
        val configuration = DynamoConfiguration.fromEnvironment(mapOf("DYNAMODB_ENDPOINT" to endpoint)).apply {
            table = "llms-txt-test-${UUID.randomUUID()}"
        }
        DynamoClients.create(configuration).use { client ->
            try {
                val bootstrap = LocalTableBootstrap(client, configuration)
                bootstrap.initialize()
                assertTrue(InfrastructureReadiness(client, configuration).isReady())

                val probeKey = mapOf("pk" to DynamoSchema.string("TEST#CONNECTIVITY"), "sk" to DynamoSchema.string("META"))
                client.putItem(
                    PutItemRequest
                        .builder()
                        .tableName(configuration.table)
                        .item(
                            probeKey + mapOf(
                                "entityType" to DynamoSchema.string("IntegrationProbe"),
                                "itemSchemaVersion" to DynamoSchema.number(1),
                                "workPk" to DynamoSchema.string("WORK#QUEUED#0"),
                                "workSk" to DynamoSchema.string("00000000000000000001#test"),
                                "duePk" to DynamoSchema.string("DUE#0"),
                                "dueSk" to DynamoSchema.string("00000000000000000001#test"),
                            ),
                        ).build(),
                )
                assertEquals(
                    probeKey["pk"],
                    client
                        .getItem(
                            GetItemRequest
                                .builder()
                                .tableName(configuration.table)
                                .key(probeKey)
                                .consistentRead(true)
                                .build(),
                        ).item()["pk"],
                )
                for ((name, key, value) in listOf(Triple("WorkIndex", "workPk", "WORK#QUEUED#0"), Triple("DueIndex", "duePk", "DUE#0"))) {
                    val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
                    while (true) {
                        val result = client.query(
                            QueryRequest
                                .builder()
                                .tableName(configuration.table)
                                .indexName(name)
                                .keyConditionExpression("#key = :value")
                                .expressionAttributeNames(mapOf("#key" to key))
                                .expressionAttributeValues(mapOf(":value" to DynamoSchema.string(value)))
                                .build(),
                        )
                        if (result.count() == 1) break
                        assertTrue(System.nanoTime() < deadline, "$name did not become queryable")
                        Thread.sleep(50)
                    }
                }
                client.updateItem(
                    UpdateItemRequest
                        .builder()
                        .tableName(configuration.table)
                        .key(DynamoSchema.capacityKey)
                        .updateExpression("SET outstandingRuns = :count")
                        .expressionAttributeValues(mapOf(":count" to DynamoSchema.number(7)))
                        .build(),
                )
                bootstrap.initialize()
                assertEquals(
                    "7",
                    client
                        .getItem(
                            GetItemRequest
                                .builder()
                                .tableName(configuration.table)
                                .key(DynamoSchema.capacityKey)
                                .consistentRead(true)
                                .build(),
                        ).item()["outstandingRuns"]
                        ?.n(),
                )

                val properties = mapOf<String, Any>(
                    "crawl.enabled" to false,
                    "micronaut.server.port" to -1,
                    "dynamodb.environment" to "local",
                    "dynamodb.endpoint" to endpoint,
                    "dynamodb.table" to configuration.table,
                    "dynamodb.region" to "us-east-1",
                    "dynamodb.work-index-shards" to configuration.workIndexShards,
                    "dynamodb.due-index-shards" to configuration.dueIndexShards,
                )
                ApplicationContext.run(EmbeddedServer::class.java, properties).use { server ->
                    HttpClient.newHttpClient().use { http ->
                        fun get(path: String) = http.send(
                            HttpRequest
                                .newBuilder(URI("${server.url}$path"))
                                .timeout(Duration.ofSeconds(15))
                                .GET()
                                .build(),
                            HttpResponse.BodyHandlers.ofString(),
                        )
                        assertEquals(200, get("/health/liveness").statusCode())
                        val ready = get("/health/readiness")
                        assertEquals(200, ready.statusCode())
                        assertEquals("{\"status\":\"UP\"}", ready.body())
                        assertTrue(
                            ready
                                .headers()
                                .firstValue("Content-Type")
                                .orElse("")
                                .startsWith("application/json"),
                        )
                        val frontend = get("/")
                        assertEquals(200, frontend.statusCode())
                        assertTrue(frontend.body().contains("<div id=\"root\"></div>"))
                        val scriptPath = requireNotNull(
                            Regex("src=\"(/assets/[^\"]+\\.js)\"")
                                .find(frontend.body())
                                ?.groupValues
                                ?.get(1),
                        ) { "Built frontend script was not served." }
                        val script = get(scriptPath)
                        assertEquals(200, script.statusCode())
                        assertTrue(script.body().isNotBlank())

                        client.deleteItem(
                            DeleteItemRequest
                                .builder()
                                .tableName(configuration.table)
                                .key(DynamoSchema.capacityKey)
                                .build(),
                        )
                        val unavailableDeadline = System.nanoTime() + Duration.ofSeconds(5).toNanos()
                        var unavailable = get("/health/readiness")
                        while (unavailable.statusCode() == 200 && System.nanoTime() < unavailableDeadline) {
                            Thread.sleep(50)
                            unavailable = get("/health/readiness")
                        }
                        assertEquals(503, unavailable.statusCode())
                        assertEquals(200, get("/health/liveness").statusCode())
                        assertThrows(IllegalArgumentException::class.java) { bootstrap.initialize() }
                        assertTrue(
                            client
                                .getItem(
                                    GetItemRequest
                                        .builder()
                                        .tableName(configuration.table)
                                        .key(DynamoSchema.capacityKey)
                                        .consistentRead(true)
                                        .build(),
                                ).item()
                                .isEmpty(),
                        )
                    }
                }
            } finally {
                client.deleteTable(DeleteTableRequest.builder().tableName(configuration.table).build())
            }
        }
    }
}
