package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.ingestion.Capacity
import software.amazon.awssdk.services.dynamodb.model.*

object DynamoSchema {
    val capacityKey: Map<String, AttributeValue> = mapOf("pk" to string("CAPACITY#GLOBAL"), "sk" to string("META"))
    val configurationKey: Map<String, AttributeValue> = mapOf("pk" to string("CONFIG#GLOBAL"), "sk" to string("META"))

    fun string(value: String): AttributeValue = AttributeValue.builder().s(value).build()

    fun number(value: Int): AttributeValue = AttributeValue.builder().n(value.toString()).build()

    fun keys(
        partition: String,
        sort: String,
    ): List<KeySchemaElement> = listOf(
        KeySchemaElement
            .builder()
            .attributeName(partition)
            .keyType(KeyType.HASH)
            .build(),
        KeySchemaElement
            .builder()
            .attributeName(sort)
            .keyType(KeyType.RANGE)
            .build(),
    )

    fun createTable(table: String): CreateTableRequest = CreateTableRequest
        .builder()
        .tableName(table)
        .billingMode(BillingMode.PAY_PER_REQUEST)
        .attributeDefinitions(
            listOf("pk", "sk", "workPk", "workSk", "duePk", "dueSk").map {
                AttributeDefinition
                    .builder()
                    .attributeName(it)
                    .attributeType(ScalarAttributeType.S)
                    .build()
            },
        ).keySchema(keys("pk", "sk"))
        .globalSecondaryIndexes(
            listOf(
                index("WorkIndex", "workPk", "workSk"),
                index("DueIndex", "duePk", "dueSk"),
            ),
        ).build()

    private fun index(
        name: String,
        partition: String,
        sort: String,
    ): GlobalSecondaryIndex = GlobalSecondaryIndex
        .builder()
        .indexName(name)
        .keySchema(keys(partition, sort))
        .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
        .build()

    fun tableIsReady(table: TableDescription): Boolean {
        val indices = table.globalSecondaryIndexes().associateBy { it.indexName() }
        return table.tableStatus() == TableStatus.ACTIVE && table.keySchema().toSet() == keys("pk", "sk").toSet() &&
            table.attributeDefinitions().associate { it.attributeName() to it.attributeType() } ==
            listOf("pk", "sk", "workPk", "workSk", "duePk", "dueSk").associateWith { ScalarAttributeType.S } &&
            indices.keys == setOf("WorkIndex", "DueIndex") &&
            listOf(Triple("WorkIndex", "workPk", "workSk"), Triple("DueIndex", "duePk", "dueSk")).all { (name, pk, sk) ->
                val index = indices.getValue(name)
                index.indexStatus() == IndexStatus.ACTIVE && index.keySchema().toSet() == keys(pk, sk).toSet() &&
                    index.projection()?.projectionType() == ProjectionType.KEYS_ONLY
            }
    }

    fun metadataIsReady(
        capacity: Map<String, AttributeValue>,
        configuration: Map<String, AttributeValue>,
        settings: DynamoConfiguration,
    ): Boolean = capacity["entityType"]?.s() == "Capacity" && capacity["itemSchemaVersion"]?.n() == "1" &&
        listOf("outstandingRuns", "monitoredIdentities").all { (capacity[it]?.n()?.toLongOrNull() ?: -1) >= 0 } &&
        configuration["entityType"]?.s() == "Configuration" && configuration["itemSchemaVersion"]?.n() == "1" &&
        configuration["workIndexShards"]?.n() == settings.workIndexShards.toString() &&
        configuration["dueIndexShards"]?.n() == settings.dueIndexShards.toString()
}
