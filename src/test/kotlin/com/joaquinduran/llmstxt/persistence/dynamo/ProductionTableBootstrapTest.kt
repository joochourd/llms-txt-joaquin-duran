package com.joaquinduran.llmstxt.persistence.dynamo

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.util.UUID

@Tag("integration")
class ProductionTableBootstrapTest {
    private fun withTable(block: (DynamoDbClient, DynamoConfiguration) -> Unit) {
        val local = DynamoConfiguration
            .fromEnvironment(
                mapOf("DYNAMODB_ENDPOINT" to requireNotNull(System.getenv("DYNAMODB_ENDPOINT"))),
            ).apply { table = "llms-txt-bootstrap-${UUID.randomUUID()}" }
        DynamoClients.create(local).use { client ->
            client.createTable(DynamoSchema.createTable(local.table))
            try {
                // Only the injected client contacts Local; no AWS client or credentials are created.
                val production = DynamoConfiguration().apply {
                    environment = "aws"
                    table = local.table
                }
                block(client, production)
            } finally {
                client.deleteTable(DeleteTableRequest.builder().tableName(local.table).build())
            }
        }
    }

    private fun read(
        client: DynamoDbClient,
        configuration: DynamoConfiguration,
        key: Item,
    ): Item = client
        .getItem(
            GetItemRequest
                .builder()
                .tableName(configuration.table)
                .key(key)
                .consistentRead(true)
                .build(),
        ).item()

    @Test
    fun `fresh provisioned table receives exactly the two required global metadata records`() = withTable { client, configuration ->
        ProductionTableBootstrap(client, configuration).initialize()

        assertTrue(InfrastructureReadiness(client, configuration).isReady())
        assertEquals("0", read(client, configuration, DynamoSchema.capacityKey)["outstandingRuns"]?.n())
        assertEquals("0", read(client, configuration, DynamoSchema.capacityKey)["monitoredIdentities"]?.n())
        assertTrue(read(client, configuration, mapOf("pk" to DynamoSchema.string("CAPACITY#BULK"), "sk" to DynamoSchema.string("META"))).isEmpty())
    }

    @Test
    fun `repeated initialization preserves nonzero durable counters and all existing metadata`() = withTable { client, configuration ->
        val bootstrap = ProductionTableBootstrap(client, configuration)
        bootstrap.initialize()
        client.updateItem(
            UpdateItemRequest
                .builder()
                .tableName(configuration.table)
                .key(DynamoSchema.capacityKey)
                .updateExpression("SET outstandingRuns = :runs, monitoredIdentities = :monitors")
                .expressionAttributeValues(mapOf(":runs" to DynamoSchema.number(7), ":monitors" to DynamoSchema.number(3)))
                .build(),
        )
        val before = read(client, configuration, DynamoSchema.capacityKey)

        bootstrap.initialize()

        assertEquals(before, read(client, configuration, DynamoSchema.capacityKey))
        assertTrue(InfrastructureReadiness(client, configuration).isReady())
    }

    @ParameterizedTest
    @ValueSource(strings = ["CAPACITY#GLOBAL", "CONFIG#GLOBAL"])
    fun `partial initialization fails without repairing or overwriting either record`(partition: String) = withTable { client, configuration ->
        val bootstrap = ProductionTableBootstrap(client, configuration)
        bootstrap.initialize()
        val removed = mapOf("pk" to DynamoSchema.string(partition), "sk" to DynamoSchema.string("META"))
        val retained = if (partition == "CONFIG#GLOBAL") DynamoSchema.capacityKey else DynamoSchema.configurationKey
        val before = read(client, configuration, retained)
        client.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(configuration.table)
                .key(removed)
                .build(),
        )

        assertThrows(IllegalStateException::class.java) { bootstrap.initialize() }

        assertTrue(read(client, configuration, removed).isEmpty())
        assertEquals(before, read(client, configuration, retained))
    }

    @ParameterizedTest
    @ValueSource(strings = ["itemSchemaVersion", "workIndexShards", "dueIndexShards"])
    fun `incompatible global configuration fails without modifying it`(field: String) = withTable { client, configuration ->
        val bootstrap = ProductionTableBootstrap(client, configuration)
        bootstrap.initialize()
        client.updateItem(
            UpdateItemRequest
                .builder()
                .tableName(configuration.table)
                .key(DynamoSchema.configurationKey)
                .updateExpression("SET #field = :value")
                .expressionAttributeNames(mapOf("#field" to field))
                .expressionAttributeValues(mapOf(":value" to DynamoSchema.number(99)))
                .build(),
        )
        val before = read(client, configuration, DynamoSchema.configurationKey)

        assertThrows(IllegalStateException::class.java) { bootstrap.initialize() }

        assertEquals(before, read(client, configuration, DynamoSchema.configurationKey))
    }

    @Test
    fun `missing global records on a nonempty table never initialize counters to zero`() = withTable { client, configuration ->
        val orphan = mapOf("pk" to DynamoSchema.string("RUN#existing"), "sk" to DynamoSchema.string("META"))
        client.putItem(
            PutItemRequest
                .builder()
                .tableName(configuration.table)
                .item(orphan)
                .build(),
        )

        assertThrows(IllegalStateException::class.java) { ProductionTableBootstrap(client, configuration).initialize() }

        assertTrue(read(client, configuration, DynamoSchema.capacityKey).isEmpty())
        assertTrue(read(client, configuration, DynamoSchema.configurationKey).isEmpty())
        assertEquals(orphan, read(client, configuration, orphan))
    }

    @Test
    fun `lost transaction response is resolved from durable metadata without repeating initialization`() = withTable { client, configuration ->
        var transactions = 0
        val ambiguous = Proxy.newProxyInstance(DynamoDbClient::class.java.classLoader, arrayOf(DynamoDbClient::class.java)) { _, method, args ->
            try {
                val response = method.invoke(client, *(args ?: emptyArray()))
                if (method.name == "transactWriteItems") {
                    transactions++
                    val request = args?.single() as TransactWriteItemsRequest
                    assertTrue(request.transactItems().all { it.put().conditionExpression() == "attribute_not_exists(pk)" })
                    assertFalse(request.clientRequestToken().isNullOrBlank())
                    throw SdkClientException.create("Simulated lost commit response")
                }
                response
            } catch (exception: InvocationTargetException) {
                throw exception.cause ?: exception
            }
        } as DynamoDbClient

        ProductionTableBootstrap(ambiguous, configuration).initialize()

        assertEquals(1, transactions)
        assertTrue(InfrastructureReadiness(client, configuration).isReady())
    }
}
