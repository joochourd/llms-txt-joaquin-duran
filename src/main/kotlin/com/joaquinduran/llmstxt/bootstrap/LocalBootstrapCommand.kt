package com.joaquinduran.llmstxt.bootstrap

import com.joaquinduran.llmstxt.persistence.dynamo.DynamoClients
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoConfiguration
import com.joaquinduran.llmstxt.persistence.dynamo.LocalTableBootstrap

/** Administrative local-only mode runs without starting the application or its background workers. */
object LocalBootstrapCommand {
    fun execute() {
        val configuration = DynamoConfiguration.fromEnvironment()
        configuration.requireLocal()
        DynamoClients.create(configuration).use { LocalTableBootstrap(it, configuration).initialize() }
        println("DynamoDB Local table and bootstrap metadata are ready.")
    }
}
