package com.joaquinduran.llmstxt.persistence.dynamo

import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest

@Singleton
class InfrastructureReadiness(
    private val client: DynamoDbClient,
    private val configuration: DynamoConfiguration,
) {
    fun isReady(): Boolean = try {
        val table = client.describeTable(DescribeTableRequest.builder().tableName(configuration.table).build()).table()
        DynamoSchema.tableIsReady(table) && DynamoSchema.metadataIsReady(
            client
                .getItem(
                    GetItemRequest
                        .builder()
                        .tableName(configuration.table)
                        .key(DynamoSchema.capacityKey)
                        .consistentRead(true)
                        .build(),
                ).item(),
            client
                .getItem(
                    GetItemRequest
                        .builder()
                        .tableName(configuration.table)
                        .key(DynamoSchema.configurationKey)
                        .consistentRead(true)
                        .build(),
                ).item(),
            configuration,
        )
    } catch (exception: SdkException) {
        // Do not expose SDK errors, endpoints, or credentials in public responses or logs.
        logger.warn("DynamoDB readiness check failed ({})", exception.javaClass.simpleName)
        false
    }

    companion object {
        private val logger = LoggerFactory.getLogger(InfrastructureReadiness::class.java)
    }
}
