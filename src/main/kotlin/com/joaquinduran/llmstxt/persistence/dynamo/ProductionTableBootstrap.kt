package com.joaquinduran.llmstxt.persistence.dynamo

import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.util.UUID

/** Administrative initialization for an already-provisioned AWS table. */
class ProductionTableBootstrap(
    private val client: DynamoDbClient,
    private val configuration: DynamoConfiguration,
) {
    fun initialize() {
        configuration.validate()
        require(configuration.environment == "aws") { "Production bootstrap requires APP_ENV=aws and no endpoint override." }
        val table = client.describeTable(DescribeTableRequest.builder().tableName(configuration.table).build()).table()
        check(DynamoSchema.tableIsReady(table)) { "Provision an ACTIVE compatible table and indexes before initializing metadata." }
        val existing = metadata()
        if (existing.any { it.isNotEmpty() }) {
            requireCompatible(existing)
            return
        }
        // Administrative-only bounded inspection: missing counters on an occupied table must never be recreated.
        val occupied = client
            .scan(
                ScanRequest
                    .builder()
                    .tableName(configuration.table)
                    .consistentRead(true)
                    .limit(1)
                    .projectionExpression("pk, sk")
                    .build(),
            ).items()
            .isNotEmpty()
        if (occupied) {
            // Another initializer may have committed between the atomic read and the emptiness check.
            requireCompatible(metadata())
            return
        }

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
        val transaction = TransactWriteItemsRequest
            .builder()
            .clientRequestToken(UUID.randomUUID().toString())
            .transactItems(
                listOf(capacity, settings).map { item ->
                    TransactWriteItem
                        .builder()
                        .put(
                            Put
                                .builder()
                                .tableName(configuration.table)
                                .item(item)
                                .conditionExpression("attribute_not_exists(pk)")
                                .build(),
                        ).build()
                },
            ).build()
        try {
            client.transactWriteItems(transaction)
        } catch (_: SdkException) {
            // A concurrent initializer or an ambiguous response is safe only when both durable records agree.
            requireCompatible(metadata())
            return
        }
        requireCompatible(metadata())
    }

    private fun metadata(): List<Map<String, AttributeValue>> = client
        .transactGetItems(
            TransactGetItemsRequest
                .builder()
                .transactItems(
                    listOf(DynamoSchema.capacityKey, DynamoSchema.configurationKey).map { key ->
                        TransactGetItem
                            .builder()
                            .get(
                                Get
                                    .builder()
                                    .tableName(configuration.table)
                                    .key(key)
                                    .build(),
                            ).build()
                    },
                ).build(),
        ).responses()
        .map { it.item() }

    private fun requireCompatible(items: List<Map<String, AttributeValue>>) {
        check(items.size == 2 && DynamoSchema.metadataIsReady(items[0], items[1], configuration)) {
            "Global metadata is partial, incompatible, or initialization was not confirmed. Inspect the table; no repair or counter reset was attempted."
        }
    }
}
