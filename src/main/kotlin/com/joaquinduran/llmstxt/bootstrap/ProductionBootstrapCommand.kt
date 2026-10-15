package com.joaquinduran.llmstxt.bootstrap

import com.joaquinduran.llmstxt.persistence.dynamo.DynamoClients
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoConfiguration
import com.joaquinduran.llmstxt.persistence.dynamo.ProductionTableBootstrap
import software.amazon.awssdk.core.exception.SdkException

/** Separate administrative credentials initialize an existing table without starting workers. */
object ProductionBootstrapCommand {
    fun execute() {
        require(!System.getenv("DYNAMODB_TABLE").isNullOrBlank() && !System.getenv("AWS_REGION").isNullOrBlank()) {
            "Production bootstrap requires explicit DYNAMODB_TABLE and AWS_REGION values."
        }
        val configuration = DynamoConfiguration.fromEnvironment()
        require(configuration.environment == "aws") { "Use APP_ENV=aws with no DYNAMODB_ENDPOINT for production bootstrap." }
        try {
            DynamoClients.create(configuration).use { client -> ProductionTableBootstrap(client, configuration).initialize() }
        } catch (exception: SdkException) {
            error(
                "Production bootstrap could not access or confirm the table (${exception.javaClass.simpleName}). Inspect permissions and infrastructure.",
            )
        }
        println("Production table schema and global metadata are ready; existing counters were preserved.")
    }
}
