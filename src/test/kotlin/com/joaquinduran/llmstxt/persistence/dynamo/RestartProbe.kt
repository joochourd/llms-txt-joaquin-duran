package com.joaquinduran.llmstxt.persistence.dynamo

import software.amazon.awssdk.services.dynamodb.model.*

/** A local verification tool; never part of the runtime distribution. */
fun main(args: Array<String>) {
    val configuration = DynamoConfiguration.fromEnvironment()
    configuration.requireLocal()
    val key = mapOf("pk" to DynamoSchema.string("TEST#M0#RESTART"), "sk" to DynamoSchema.string("META"))
    DynamoClients.create(configuration).use { client ->
        when (args.single()) {
            "write" -> client.putItem(
                PutItemRequest
                    .builder()
                    .tableName(configuration.table)
                    .item(
                        key + mapOf(
                            "entityType" to DynamoSchema.string("RestartProbe"),
                            "itemSchemaVersion" to DynamoSchema.number(1),
                            "value" to DynamoSchema.string("survives-container-restart"),
                        ),
                    ).build(),
            )

            "verify" -> check(
                client
                    .getItem(
                        GetItemRequest
                            .builder()
                            .tableName(configuration.table)
                            .key(key)
                            .consistentRead(true)
                            .build(),
                    ).item()["value"]
                    ?.s() == "survives-container-restart",
            ) {
                "DynamoDB data did not survive the restart."
            }

            "delete" -> client.deleteItem(
                DeleteItemRequest
                    .builder()
                    .tableName(configuration.table)
                    .key(key)
                    .build(),
            )

            else -> error("Expected write, verify, or delete.")
        }
    }
    println("DynamoDB Local restart probe: ${args.single()} succeeded.")
}
