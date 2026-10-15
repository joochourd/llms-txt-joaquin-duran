package com.joaquinduran.llmstxt.persistence.dynamo

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DynamoConfigurationTest {
    @Test
    fun `local defaults require no AWS account`() {
        val configuration = DynamoConfiguration.fromEnvironment(emptyMap())
        assertEquals("local", configuration.environment)
        assertEquals("http://localhost:8000", configuration.endpoint)
        assertEquals(1, configuration.workIndexShards)
        assertEquals(1, configuration.dueIndexShards)
    }

    @Test
    fun `existing table shard policy remains explicitly configurable`() {
        val configuration = DynamoConfiguration.fromEnvironment(mapOf("WORK_INDEX_SHARDS" to "4", "DUE_INDEX_SHARDS" to "4"))

        assertEquals(4, configuration.workIndexShards)
        assertEquals(4, configuration.dueIndexShards)
    }

    @Test
    fun `local bootstrap rejects remote and ambiguous endpoints`() {
        listOf(
            "https://dynamodb.us-east-1.amazonaws.com",
            "http://example.com:8000",
            "http://user:password@localhost:8000",
            "http://localhost:8000/?target=remote",
        ).forEach { endpoint ->
            assertThrows(IllegalArgumentException::class.java) {
                DynamoConfiguration.fromEnvironment(mapOf("DYNAMODB_ENDPOINT" to endpoint))
            }
        }
    }

    @Test
    fun `cloud mode requires an absent endpoint and cannot initialize tables`() {
        val configuration = DynamoConfiguration.fromEnvironment(mapOf("APP_ENV" to "aws"))
        assertThrows(IllegalArgumentException::class.java) { configuration.requireLocal() }
        assertThrows(IllegalArgumentException::class.java) {
            DynamoConfiguration.fromEnvironment(mapOf("APP_ENV" to "aws", "DYNAMODB_ENDPOINT" to "http://localhost:8000"))
        }
    }

    @Test
    fun `shard counts and table names are validated`() {
        assertThrows(IllegalArgumentException::class.java) {
            DynamoConfiguration.fromEnvironment(mapOf("WORK_INDEX_SHARDS" to "0"))
        }
        assertThrows(IllegalArgumentException::class.java) {
            DynamoConfiguration.fromEnvironment(mapOf("DYNAMODB_TABLE" to "x"))
        }
    }
}
