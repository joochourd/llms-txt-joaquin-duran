package com.joaquinduran.llmstxt.persistence.dynamo

import io.micronaut.context.annotation.ConfigurationProperties
import java.net.URI

@ConfigurationProperties("dynamodb")
class DynamoConfiguration {
    var environment: String = "local"
    var table: String = "llms-txt-local"
    var region: String = "us-east-1"
    var endpoint: String = ""
    var workIndexShards: Int = 1
    var dueIndexShards: Int = 1

    fun validate() {
        require(table.matches(Regex("[a-zA-Z0-9_.-]{3,255}"))) { "Invalid DynamoDB table name." }
        require(region.isNotBlank()) { "AWS_REGION is required." }
        require(workIndexShards > 0 && dueIndexShards > 0) { "Index shard counts must be positive." }
        require(environment in setOf("local", "aws")) { "APP_ENV must be local or aws." }
        if (environment == "local") {
            requireLocal()
        } else {
            require(endpoint.isBlank()) { "DYNAMODB_ENDPOINT must be empty in AWS mode." }
        }
    }

    fun requireLocal() {
        require(environment == "local") { "Table initialization and integration tests are local only." }
        val uri = localEndpoint()
        require(uri.scheme == "http" && uri.host in setOf("localhost", "127.0.0.1", "[::1]", "dynamodb")) {
            "DYNAMODB_ENDPOINT must point to localhost or the Compose dynamodb service."
        }
        require(
            uri.port in 1..65535 && uri.userInfo == null && uri.query == null && uri.fragment == null &&
                uri.path in setOf("", "/"),
        ) { "Invalid DynamoDB Local endpoint." }
    }

    fun localEndpoint(): URI = URI(endpoint.ifBlank { "http://localhost:8000" })

    companion object {
        fun fromEnvironment(env: Map<String, String> = System.getenv()): DynamoConfiguration = DynamoConfiguration().apply {
            environment = env["APP_ENV"] ?: "local"
            table = env["DYNAMODB_TABLE"] ?: "llms-txt-local"
            region = env["AWS_REGION"] ?: "us-east-1"
            endpoint = env["DYNAMODB_ENDPOINT"] ?: if (environment == "local") "http://localhost:8000" else ""
            workIndexShards = (env["WORK_INDEX_SHARDS"] ?: "1").toInt()
            dueIndexShards = (env["DUE_INDEX_SHARDS"] ?: "1").toInt()
            validate()
        }
    }
}
