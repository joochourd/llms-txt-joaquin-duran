package com.joaquinduran.llmstxt.api.controllers

import com.joaquinduran.llmstxt.api.configuration.ApiExecutorFactory
import com.joaquinduran.llmstxt.api.controllers.HealthController
import com.joaquinduran.llmstxt.api.controllers.HealthResponse
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoConfiguration
import com.joaquinduran.llmstxt.persistence.dynamo.InfrastructureReadiness
import io.micronaut.http.HttpStatus
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.lang.reflect.Proxy
import java.util.concurrent.CountDownLatch
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit

class HealthControllerTest {
    private var calls = 0
    private val unavailableClient = Proxy.newProxyInstance(
        DynamoDbClient::class.java.classLoader,
        arrayOf(DynamoDbClient::class.java),
    ) { _, method, _ ->
        calls++
        throw SdkClientException.create("Unavailable: ${method.name}")
    } as DynamoDbClient
    private val controller = HealthController(InfrastructureReadiness(unavailableClient, DynamoConfiguration()))

    @Test
    fun `liveness does not contact DynamoDB`() {
        assertEquals(HealthResponse("UP"), controller.liveness())
        assertEquals(0, calls)
    }

    @Test
    fun `readiness returns 503 when DynamoDB is unavailable`() {
        val response = controller.readiness()
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.status)
        assertEquals(HealthResponse("DOWN"), response.body())
        assertEquals(1, calls)
    }

    @Test
    fun `API executor rejects overload without growing its queue`() {
        val executor = ApiExecutorFactory().executor(1, 1)
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        try {
            executor.execute {
                started.countDown()
                release.await()
            }
            assertTrue(started.await(2, TimeUnit.SECONDS))
            executor.execute { }
            assertThrows(RejectedExecutionException::class.java) { executor.execute { } }
        } finally {
            release.countDown()
            executor.shutdownNow()
        }
    }
}
