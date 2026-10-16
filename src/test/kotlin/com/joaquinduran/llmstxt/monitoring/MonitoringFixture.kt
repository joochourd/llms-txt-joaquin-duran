package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.fetch.OriginBackoff
import com.joaquinduran.llmstxt.crawl.fetch.OriginCoordinator
import com.joaquinduran.llmstxt.crawl.model.CrawlResult
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.crawl.model.SeedContext
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunCoverage
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.ingestion.submitAndProcess
import com.joaquinduran.llmstxt.monitoring.MonitoringAdmissionKind
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoClients
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoConfiguration
import com.joaquinduran.llmstxt.persistence.dynamo.ExecutionStore
import com.joaquinduran.llmstxt.persistence.dynamo.Item
import com.joaquinduran.llmstxt.persistence.dynamo.LocalTableBootstrap
import com.joaquinduran.llmstxt.persistence.dynamo.MonitoringStore
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.util.UUID

abstract class MonitoringFixture {
    protected lateinit var client: DynamoDbClient
    protected lateinit var store: AdmissionStore
    protected lateinit var settings: AdmissionConfiguration
    protected lateinit var config: CrawlConfiguration
    protected lateinit var service: AdmissionService
    protected lateinit var monitoring: MonitoringService
    protected lateinit var monitoringStore: MonitoringStore
    protected lateinit var execution: ExecutionStore
    protected lateinit var origins: OriginCoordinator
    protected val clock = MutableClock()

    @BeforeEach
    fun setup() {
        val dynamo = DynamoConfiguration
            .fromEnvironment(mapOf("DYNAMODB_ENDPOINT" to requireNotNull(System.getenv("DYNAMODB_ENDPOINT"))))
            .apply { table = "llms-txt-m3-${UUID.randomUUID()}" }
        client = DynamoClients.create(dynamo)
        LocalTableBootstrap(client, dynamo).initialize()
        store = AdmissionStore(client, dynamo)
        settings = AdmissionConfiguration()
        config = CrawlConfiguration().apply {
            fetch.minRequestIntervalMillis = 0
        }
        origins = OriginCoordinator(config, OriginBackoff(store, config), clock)
        monitoringStore = MonitoringStore(store, settings)
        monitoring = monitoringService()
        service = AdmissionService(store, settings, UrlIdentityResolver(settings), clock)
        execution = ExecutionStore(store, settings, config, clock)
    }

    @AfterEach
    fun cleanup() {
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName(store.table).build())
        } finally {
            client.close()
        }
    }

    protected fun queued(seed: String = "https://fixture.example/"): CrawlRun {
        val batch = submitAndProcess(service, store, clock, listOf(seed), false)
        return requireNotNull(store.run(requireNotNull(store.rows(batch.batchId, true).single().runId)))
    }

    protected fun result(
        run: CrawlRun,
        label: String = "first",
        content: String = label,
    ): CrawlResult {
        val resource = Resource(
            sha256(run.normalizedSeedUrl),
            run.normalizedSeedUrl,
            run.normalizedSeedUrl,
            run.normalizedSeedUrl,
            "HTML",
            "Fixture",
            "Useful fixture page",
            sha256(content),
            sha256(label),
            "links",
            "OTHER",
            "Overview",
            listOf("SUBSTANTIAL_MAIN_CONTENT"),
            listOf(run.normalizedSeedUrl),
            0,
            10.0,
        )
        return CrawlResult(
            run.normalizedSeedUrl,
            run.originHint,
            SeedContext("GENERIC", "LOW", listOf("Fixture")),
            listOf(resource),
            mapOf(run.normalizedSeedUrl to "VALID"),
            "# Fixture\n\n## Overview\n\n- [Fixture](${run.normalizedSeedUrl}): $label\n".toByteArray(),
            RunCoverage(1, 1, 1, 0, emptyList(), listOf("SEED")),
            emptyList(),
            requireNotNull(run.startedAt),
            clock.now,
        )
    }

    protected fun publish(
        run: CrawlRun,
        value: CrawlResult = result(run),
    ): CrawlRun = execution.publish(run, value)

    protected fun canonical(seed: String = "https://fixture.example/"): CrawlMeta {
        val run = requireNotNull(execution.claim(queued(seed)))
        assertEquals(PublicationDecision.UPDATED, publish(run).publicationDecision)
        return requireNotNull(store.meta(run.identityId))
    }

    protected fun due(id: String): CrawlRun {
        val meta = requireNotNull(store.meta(id))
        clock.now = requireNotNull(meta.nextCheckAt)
        val admitted = monitoring.admit(requireNotNull(store.meta(id)))
        assertEquals(MonitoringAdmissionKind.ENQUEUED, admitted.kind)
        return requireNotNull(store.run(requireNotNull(admitted.runId)))
    }

    protected fun monitoringService(adapter: AdmissionStore = store) = MonitoringService(
        store = adapter,
        persistence = MonitoringStore(adapter, settings),
        admission = settings,
        configuration = config,
        clock = clock,
        origins = origins,
    )

    protected fun scheduler() = MonitoringScheduler(store, monitoring, monitoringStore, config, clock)

    protected fun update(
        key: Item,
        expression: String,
        values: Item = emptyMap(),
    ) {
        val request = UpdateItemRequest
            .builder()
            .tableName(store.table)
            .key(key)
            .updateExpression(expression)
        if (values.isNotEmpty()) request.expressionAttributeValues(values)
        client.updateItem(request.build())
    }

    protected fun await(
        condition: () -> Boolean,
        action: () -> Unit = {},
    ) {
        val deadline = System.nanoTime() + 10_000_000_000
        while (!condition()) {
            action()
            assertTrue(System.nanoTime() < deadline, "The durable condition was not observed before the test deadline.")
            Thread.sleep(20)
        }
    }

    protected fun proxy(intercept: (String, Array<out Any?>, () -> Any?) -> Any?): DynamoDbClient = Proxy.newProxyInstance(
        DynamoDbClient::class.java.classLoader,
        arrayOf(DynamoDbClient::class.java),
    ) { _, method, arguments ->
        val args = arguments ?: emptyArray()
        intercept(method.name, args) {
            try {
                method.invoke(client, *args)
            } catch (exception: InvocationTargetException) {
                throw requireNotNull(exception.cause)
            }
        }
    } as DynamoDbClient
}
