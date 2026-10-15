package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.ingestion.Capacity
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.*

class DynamoSchemaTest {
    private val configuration = DynamoConfiguration().apply {
        workIndexShards = 4
        dueIndexShards = 4
    }
    private val capacity = mapOf(
        "entityType" to DynamoSchema.string("Capacity"),
        "itemSchemaVersion" to DynamoSchema.number(1),
        "outstandingRuns" to DynamoSchema.number(7),
        "monitoredIdentities" to DynamoSchema.number(3),
    )
    private val settings = mapOf(
        "entityType" to DynamoSchema.string("Configuration"),
        "itemSchemaVersion" to DynamoSchema.number(1),
        "workIndexShards" to DynamoSchema.number(4),
        "dueIndexShards" to DynamoSchema.number(4),
    )

    @Test
    fun `readiness accepts initialized nonzero counters`() {
        assertTrue(DynamoSchema.metadataIsReady(capacity, settings, configuration))
    }

    @Test
    fun `readiness rejects missing corrupted and mismatched metadata`() {
        assertFalse(DynamoSchema.metadataIsReady(emptyMap(), settings, configuration))
        assertFalse(DynamoSchema.metadataIsReady(capacity - "outstandingRuns", settings, configuration))
        assertFalse(DynamoSchema.metadataIsReady(capacity + ("outstandingRuns" to DynamoSchema.number(-1)), settings, configuration))
        assertFalse(DynamoSchema.metadataIsReady(capacity, settings + ("dueIndexShards" to DynamoSchema.number(8)), configuration))
    }

    @Test
    fun `readiness rejects missing inactive and wrongly keyed indexes`() {
        val request = DynamoSchema.createTable(configuration.table)
        val indices = request.globalSecondaryIndexes().map {
            GlobalSecondaryIndexDescription
                .builder()
                .indexName(it.indexName())
                .keySchema(it.keySchema())
                .projection(it.projection())
                .indexStatus(IndexStatus.ACTIVE)
                .build()
        }
        val table = TableDescription
            .builder()
            .tableStatus(TableStatus.ACTIVE)
            .keySchema(request.keySchema())
            .attributeDefinitions(request.attributeDefinitions())
            .globalSecondaryIndexes(indices)
            .build()
        assertTrue(DynamoSchema.tableIsReady(table))
        assertFalse(DynamoSchema.tableIsReady(table.toBuilder().globalSecondaryIndexes(indices.take(1)).build()))
        assertFalse(DynamoSchema.tableIsReady(table.toBuilder().tableStatus(TableStatus.CREATING).build()))
        assertFalse(
            DynamoSchema.tableIsReady(
                table
                    .toBuilder()
                    .globalSecondaryIndexes(
                        listOf(
                            indices[0],
                            indices[1].toBuilder().keySchema(DynamoSchema.keys("workPk", "workSk")).build(),
                        ),
                    ).build(),
            ),
        )
    }
}
