package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.ingestion.Capacity
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Duration

class LocalTableBootstrap(
    private val client: DynamoDbClient,
    private val configuration: DynamoConfiguration,
) {
    fun initialize(timeout: Duration = Duration.ofSeconds(60)) {
        configuration.validate()
        configuration.requireLocal()
        val deadline = System.nanoTime() + timeout.toNanos()
        val exists = awaitConnection(deadline)
        if (!exists) {
            try {
                client.createTable(DynamoSchema.createTable(configuration.table))
            } catch (_: ResourceInUseException) {
                // Another initializer created the table. Validate it, without initializing counters.
                awaitActive(deadline)
                while (!InfrastructureReadiness(client, configuration).isReady()) {
                    check(System.nanoTime() < deadline) {
                        "Concurrent initialization did not complete. Audit existing bootstrap metadata."
                    }
                    Thread.sleep(250)
                }
                return
            }
            awaitActive(deadline)
            // Initialize both metadata rows atomically and only for the table this process created.
            val capacity = DynamoSchema.capacityKey + mapOf(
                "entityType" to DynamoSchema.string("Capacity"),
                "itemSchemaVersion" to DynamoSchema.number(1),
                "outstandingRuns" to DynamoSchema.number(0),
                "monitoredIdentities" to DynamoSchema.number(0),
            )
            val settings = DynamoSchema.configurationKey + mapOf(
                "entityType" to DynamoSchema.string("Configuration"),
                "itemSchemaVersion" to DynamoSchema.number(1),
                "workIndexShards" to DynamoSchema.number(configuration.workIndexShards),
                "dueIndexShards" to DynamoSchema.number(configuration.dueIndexShards),
            )
            client.transactWriteItems(
                TransactWriteItemsRequest
                    .builder()
                    .transactItems(
                        listOf(capacity, settings).map {
                            TransactWriteItem
                                .builder()
                                .put(
                                    Put
                                        .builder()
                                        .tableName(configuration.table)
                                        .item(it)
                                        .conditionExpression("attribute_not_exists(pk)")
                                        .build(),
                                ).build()
                        },
                    ).build(),
            )
        } else {
            awaitActive(deadline)
        }
        require(InfrastructureReadiness(client, configuration).isReady()) {
            "Table schema or bootstrap metadata is invalid. Audit existing data; do not reset counters or shard counts."
        }
    }

    private fun awaitConnection(deadline: Long): Boolean {
        while (true) {
            try {
                client.describeTable(DescribeTableRequest.builder().tableName(configuration.table).build())
                return true
            } catch (_: ResourceNotFoundException) {
                return false
            } catch (exception: SdkClientException) {
                check(System.nanoTime() < deadline) { "DynamoDB Local did not become available within the startup timeout." }
                Thread.sleep(250)
            }
        }
    }

    private fun awaitActive(deadline: Long) {
        while (true) {
            val table = client.describeTable(DescribeTableRequest.builder().tableName(configuration.table).build()).table()
            if (DynamoSchema.tableIsReady(table)) return
            if (table.tableStatus() == TableStatus.ACTIVE &&
                table.globalSecondaryIndexes().all { it.indexStatus() == IndexStatus.ACTIVE }
            ) {
                error("Existing table schema is incompatible. Audit the table; no schema changes were applied.")
            }
            check(System.nanoTime() < deadline) { "Table and indexes did not become active with the required schema." }
            Thread.sleep(250)
        }
    }
}
