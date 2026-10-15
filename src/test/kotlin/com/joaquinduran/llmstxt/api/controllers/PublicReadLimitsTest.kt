package com.joaquinduran.llmstxt.api.controllers

import com.joaquinduran.llmstxt.api.configuration.ApiLimitConfiguration
import com.joaquinduran.llmstxt.api.configuration.ClientIpConfiguration
import com.joaquinduran.llmstxt.api.errors.ApiProblem
import com.joaquinduran.llmstxt.api.limits.ApiRateLimiter
import com.joaquinduran.llmstxt.api.limits.ClientIpResolver
import com.joaquinduran.llmstxt.api.mapping.MonitoringView
import com.joaquinduran.llmstxt.api.query.AdmissionStatus
import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.CrawlResultReader
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoConfiguration
import io.micronaut.http.HttpRequest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.lang.reflect.Proxy
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class PublicReadLimitsTest {
    private var storageCalls = 0
    private val client = Proxy.newProxyInstance(DynamoDbClient::class.java.classLoader, arrayOf(DynamoDbClient::class.java)) { _, _, _ ->
        storageCalls++
        error("Rate-limited requests must not contact storage.")
    } as DynamoDbClient
    private val store = AdmissionStore(client, DynamoConfiguration())
    private val clock = Clock.fixed(Instant.parse("2026-10-15T12:00:00Z"), ZoneOffset.UTC)
    private val configuration = AdmissionConfiguration()
    private val admission = AdmissionService(store, configuration, UrlIdentityResolver(configuration), clock)
    private val clients = ClientIpResolver(ClientIpConfiguration())
    private val rates = ApiRateLimiter(clock, ApiLimitConfiguration())
    private val controller = CrawlController(
        reader = CrawlResultReader(store),
        admission = admission,
        status = AdmissionStatus(store, admission, clock),
        rates = rates,
        clients = clients,
        monitoring = MonitoringView(store, CrawlConfiguration(), clock),
    )

    @Test
    fun `rejected forced refresh performs no metadata read even for an unknown identity`() {
        val request = HttpRequest.POST("/api/crawls/unknown/refresh", "")
        rates.check(clients.client(request), submission = true, force = true)

        val failure = assertThrows(ApiProblem::class.java) { controller.refresh("unknown", request) }

        assertEquals(429, failure.status)
        assertEquals(0, storageCalls)
    }

    @Test
    fun `expensive result resource and artifact reads reject overload before storage`() {
        val request = HttpRequest.GET<Any>("/api/crawls/unknown")
        repeat(120) { rates.check(clients.client(request), submission = false) }

        assertEquals(429, assertThrows(ApiProblem::class.java) { controller.crawl("unknown", request) }.status)
        assertEquals(429, assertThrows(ApiProblem::class.java) { controller.resources("unknown", request) }.status)
        assertEquals(429, assertThrows(ApiProblem::class.java) { controller.latest("unknown", request) }.status)
        assertEquals(0, storageCalls)
    }
}
