package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.OriginBackoff
import com.joaquinduran.llmstxt.crawl.fetch.OriginCoordinator
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.CrawlResult
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.crawl.model.SeedContext
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.DomainError
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunCounters
import com.joaquinduran.llmstxt.ingestion.RunCoverage
import com.joaquinduran.llmstxt.ingestion.RunPhase
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.ingestion.submitAndProcess
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.CurrentResultCodec
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoClients
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoConfiguration
import com.joaquinduran.llmstxt.persistence.dynamo.ExecutionStore
import com.joaquinduran.llmstxt.persistence.dynamo.LocalTableBootstrap
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Clock
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

@Tag("integration")
class ExecutionIntegrationTest {
    private lateinit var client: DynamoDbClient
    private lateinit var store: AdmissionStore
    private lateinit var settings: AdmissionConfiguration
    private lateinit var config: CrawlConfiguration
    private lateinit var admission: AdmissionService
    private lateinit var execution: ExecutionStore
    private lateinit var urls: UrlIdentityResolver
    private lateinit var origin: OriginCoordinator
    private val clock = MutableClock()

    @BeforeEach
    fun setup() {
        val dynamo = DynamoConfiguration
            .fromEnvironment(mapOf("DYNAMODB_ENDPOINT" to requireNotNull(System.getenv("DYNAMODB_ENDPOINT"))))
            .apply { table = "llms-txt-m2-${UUID.randomUUID()}" }
        client = DynamoClients.create(dynamo)
        LocalTableBootstrap(client, dynamo).initialize()
        store = AdmissionStore(client, dynamo)
        settings = AdmissionConfiguration().apply { maxOpenBulkBatches = 20 }
        config =
            CrawlConfiguration().apply { fetch.minRequestIntervalMillis = 0 }
        urls = UrlIdentityResolver(settings)
        admission = AdmissionService(store, settings, urls, clock)
        execution = ExecutionStore(store, settings, config, clock)
        origin = OriginCoordinator(config, OriginBackoff(store, config), clock)
    }

    @AfterEach
    fun cleanup() {
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName(store.table).build())
        } finally {
            client.close()
        }
    }

    private fun queued(seed: String = "https://fixture.example/"): CrawlRun {
        val batch = submitAndProcess(admission, store, clock, listOf(seed), false)
        return store.run(store.rows(batch.batchId, true).single().runId!!)!!
    }

    private fun newRun(id: String): CrawlRun {
        client.updateItem(
            UpdateItemRequest
                .builder()
                .tableName(store.table)
                .key(DomainKeys.crawl(id))
                .updateExpression("REMOVE activeRunIdHint, activeHintExpiresAt")
                .build(),
        )
        clock.now += 61_000
        val batch = submitAndProcess(admission, store, clock, listOf(store.meta(id)!!.normalizedSeedUrl), true)
        return store.run(store.rows(batch.batchId, true).single().runId!!)!!
    }

    private fun result(
        run: CrawlRun,
        text: String = "first",
        resources: List<Resource>? = null,
    ): CrawlResult {
        val fixtureResource = Resource(
            resourceKey = sha256(run.normalizedSeedUrl),
            sourceUrl = run.normalizedSeedUrl,
            primaryHtmlUrl = run.normalizedSeedUrl,
            preferredLinkUrl = run.normalizedSeedUrl,
            linkFormat = "HTML",
            title = "Fixture",
            description = "A useful fixture page",
            contentHash = sha256(text),
            metadataHash = "metadata",
            relevantLinksHash = "links",
            category = "OTHER",
            section = "Overview",
            reasonCodes = listOf("SUBSTANTIAL_MAIN_CONTENT"),
            aliases = listOf(run.normalizedSeedUrl),
            depth = 0,
            score = 10.0,
        )
        return CrawlResult(
            run.normalizedSeedUrl,
            run.originHint,
            SeedContext("GENERIC", "LOW", listOf("Fixture")),
            resources ?: listOf(fixtureResource),
            (resources ?: listOf(fixtureResource)).associate {
                it.sourceUrl to "VALID"
            },
            "# Fixture\n\n## Overview\n\n- [Fixture](<${run.normalizedSeedUrl}>): $text\n".toByteArray(),
            RunCoverage(1, 1, 1, 0, emptyList(), listOf("SEED")),
            emptyList(),
            run.startedAt!!,
            clock.millis(),
        )
    }

    private fun publish(
        run: CrawlRun,
        result: CrawlResult = result(run),
    ): CrawlRun = execution.publish(run, result)

    @Test
    fun `claim is conditional and assigns a durable sequence before execution`() {
        val queuedRun = queued()
        val pool = Executors.newFixedThreadPool(6)

        try {
            val claimed = pool.invokeAll((1..6).map { Callable { execution.claim(queuedRun) } }).mapNotNull { it.get() }

            assertEquals(1, claimed.size)
            val run = claimed.single()
            assertEquals(RunStatus.RUNNING, run.status)
            assertEquals(1L, run.observationSequence)
            assertTrue(store.get(CurrentResultCodec.key(run.identityId)).isEmpty())
            assertEquals(Capacity(1, 1), store.capacity())
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `successful publication atomically installs current bytes and evidence and releases only work capacity`() {
        val run = execution.claim(queued())!!
        val result = result(run)
        val terminal = publish(run, result)
        val meta = store.meta(run.identityId)!!
        assertEquals(PublicationDecision.UPDATED, terminal.publicationDecision)
        assertEquals(RunStatus.COMPLETED, terminal.status)
        assertFalse(terminal.capacityReserved)
        assertTrue(terminal.workPk == null)
        assertEquals(0L, meta.outstandingRunCount)
        assertEquals(1L, meta.acceptedObservationSequence)
        assertEquals(1L, meta.currentVersionSequence)
        assertEquals(MonitoringState.ACTIVE, meta.monitoringState)
        assertEquals(Capacity(0, 1), store.capacity())
        val currentResult = store.get(CurrentResultCodec.key(run.identityId))
        assertArrayEquals(result.bytes, CurrentResultCodec.bytes(currentResult))
        assertEquals(1L, currentResult.n("observationSequence"))
        assertEquals(result.resources.map { it.contentHash }, execution.baseline(meta).map { it.contentHash })
        assertFalse(
            currentResult
                .resources()
                .single()
                .aliases
                .isNotEmpty(),
        )
        assertEquals(terminal, publish(run, result))
        assertEquals(Capacity(0, 1), store.capacity())
    }

    @Test
    fun `unchanged newer snapshot advances watermark without a new version and supersedes older output`() {
        val first = execution.claim(queued())!!
        val firstResult = result(first)
        publish(first, firstResult)
        val olderRun = execution.claim(newRun(first.identityId))!!
        val newerRun = execution.claim(newRun(first.identityId))!!
        val unchanged = firstResult.copy(observedFrom = newerRun.startedAt!!, observedUntil = clock.now)
        val done = publish(newerRun, unchanged)
        assertEquals(PublicationDecision.UNCHANGED, done.publicationDecision)
        assertEquals(1L, store.meta(first.identityId)!!.currentVersionSequence)
        assertEquals(3L, store.meta(first.identityId)!!.acceptedObservationSequence)
        val older = publish(olderRun, result(olderRun, "older different output"))
        assertEquals(PublicationDecision.SUPERSEDED, older.publicationDecision)
        assertEquals(3L, store.meta(first.identityId)!!.acceptedObservationSequence)
        assertEquals(Capacity(0, 1), store.capacity())
        assertEquals(3L, store.get(CurrentResultCodec.key(first.identityId)).n("observationSequence"))
    }

    @Test
    fun `a newer failure does not prevent an older sufficient result and does not release shared initial monitoring slot`() {
        val olderRun = execution.claim(queued())!!
        val newerRun = execution.claim(newRun(olderRun.identityId))!!
        execution.finish(newerRun.runId, RunStatus.FAILED, PublicationDecision.PRESERVED, DomainError("FETCH_FAILED", "Source unavailable"))
        assertEquals(Capacity(1, 1), store.capacity())
        assertTrue(store.meta(olderRun.identityId)!!.monitoringSlotReserved)
        assertEquals(PublicationDecision.UPDATED, publish(olderRun).publicationDecision)
        assertEquals(Capacity(0, 1), store.capacity())
    }

    @Test
    fun `concurrent finalization releases work and initial monitoring exactly once`() {
        val run = execution.claim(queued())!!
        val pool = Executors.newFixedThreadPool(8)
        try {
            pool
                .invokeAll(
                    (1..8).map {
                        Callable {
                            execution.finish(run.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)
                        }
                    },
                ).forEach { assertEquals(RunStatus.FAILED, it.get().status) }
            assertEquals(Capacity(0, 0), store.capacity())
            assertEquals(0L, store.meta(run.identityId)!!.outstandingRunCount)
            assertEquals(MonitoringState.NONE, store.meta(run.identityId)!!.monitoringState)
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `timeout prevents late progress or publication and preserves last known good dates`() {
        val first = execution.claim(queued())!!
        publish(first)
        val before = store.meta(first.identityId)!!
        val late = execution.claim(newRun(first.identityId))!!
        val staged = result(late, "late")
        clock.now = late.deadlineAt!! + settings.runRecoveryGraceSeconds * 1000
        execution.finish(late.runId, RunStatus.TIMED_OUT, PublicationDecision.PRESERVED)
        assertEquals(RunStatus.TIMED_OUT, execution.publish(late, staged).status)
        assertThrows(CrawlFailure::class.java) { execution.progress(late, RunPhase.PUBLISHING, RunCounters()) }
        val after = store.meta(first.identityId)!!
        assertEquals(before.currentArtifactHash, after.currentArtifactHash)
        assertEquals(before.lastSuccessfulValidationAt, after.lastSuccessfulValidationAt)
        assertEquals(before.lastOutputUpdatedAt, after.lastOutputUpdatedAt)
        assertEquals(Capacity(0, 1), store.capacity())
    }

    @Test
    fun `baseline changed during crawl is reevaluated and unknown prior resources preserve the canonical`() {
        val first = execution.claim(queued())!!
        publish(first)
        val olderRun = execution.claim(newRun(first.identityId))!!
        val newerRun = execution.claim(newRun(first.identityId))!!
        val extra = result(
            newerRun,
        ).resources.single().copy(
            resourceKey = "child",
            sourceUrl = "https://fixture.example/child",
            primaryHtmlUrl = "https://fixture.example/child",
            preferredLinkUrl = "https://fixture.example/child",
            section = "Resources",
        )
        val olderResult = result(olderRun, "a", result(olderRun).resources + extra)
        publish(olderRun, olderResult)
        val newerResult = result(newerRun, "b")
        val terminal = publish(newerRun, newerResult)
        assertEquals(PublicationDecision.PRESERVED, terminal.publicationDecision)
        assertEquals("INCOMPLETE_BASELINE", terminal.error?.code)
        assertEquals(olderRun.observationSequence, store.meta(first.identityId)!!.acceptedObservationSequence)
    }

    @Test
    fun `pipeline change rejects publication without replacing the current result`() {
        val run = execution.claim(queued())!!
        val candidate = result(run)
        client.updateItem(
            UpdateItemRequest
                .builder()
                .tableName(store.table)
                .key(DomainKeys.crawl(run.identityId))
                .updateExpression("SET requiredPipelineVersion = :pipeline ADD metadataRevision :one")
                .expressionAttributeValues(mapOf(":pipeline" to text("future-v2"), ":one" to number(1)))
                .build(),
        )
        assertEquals("PIPELINE_CHANGED", execution.publish(run, candidate).error?.code)
        assertFalse(store.meta(run.identityId)!!.hasCanonical())
        assertEquals(Capacity(0, 0), store.capacity())
    }

    private fun fixtureTransport(
        responses: Map<String, Pair<Int, String>>,
        seen: MutableList<String>,
    ): PageTransport = PageTransport { url, _, _ ->
        seen.add(url.toString())
        val entry = responses[url.encodedPath] ?: (404 to "")
        FetchResponse(
            url,
            entry.first,
            mapOf(
                "Content-Type" to
                    listOf(
                        if (url.encodedPath.endsWith(".md")) {
                            "text/markdown"
                        } else if (url.encodedPath.endsWith(".xml")) {
                            "application/xml"
                        } else {
                            "text/html"
                        },
                    ),
            ),
            entry.second.toByteArray(),
        )
    }

    @Test
    fun `complete fixture URL crawl publish and API download uses verified HTML links`() {
        val seen = mutableListOf<String>()
        val html = "<h1>Documentation platform</h1>" +
            "<meta name='description' content='Practical guidance for using the documented platform.'>" +
            "<nav><a href='/docs/start'>Start guide</a><a href='/docs/api'>API reference</a>" +
            "<a href='/docs/authentication'>Authentication</a></nav>" +
            "<main>${"Useful platform guidance. ".repeat(20)}</main>"
        val transport = fixtureTransport(
            mapOf(
                "/" to (200 to html),
                "/docs/start" to
                    (
                        200 to
                            (
                                "<h1>Getting started</h1><link rel='alternate' type='text/markdown' href='/start.md'>" +
                                    "<main>${"Setup instructions. ".repeat(20)}</main>"
                            )
                    ),
                "/docs/api" to (200 to "<h1>API reference</h1><main>${"Integration reference. ".repeat(20)}</main>"),
                "/docs/authentication" to (200 to "<h1>Authentication</h1><main>${"Secure integration documentation. ".repeat(20)}</main>"),
                "/start.md" to (200 to "# Getting started\n\nUseful setup instructions."),
            ),
            seen,
        )
        val fetcher = SafeFetcher(transport, origin, config, clock, urls)
        val run = execution.claim(queued())!!
        val result = CrawlRunner(
            fetcher,
            config,
            clock,
        ).run(run, emptyList(), CrawlBudget(60, 10000)) { phase, counters -> execution.progress(run, phase, counters) }
        assertTrue(result.coverage.candidatesRetained <= run.budgets.maxCandidates)
        assertEquals(4, result.resources.size)
        assertTrue(result.bytes.toString(Charsets.UTF_8).contains("/docs/start"))
        assertFalse(seen.any { it.endsWith("/start.md") })
        assertTrue(seen.contains("https://fixture.example/docs/authentication"))
        assertEquals("DOCUMENTATION", result.context.type)
        publish(run, result)
        val properties = mapOf<String, Any>(
            "crawl.enabled" to false,
            "micronaut.server.port" to -1,
            "dynamodb.environment" to "local",
            "dynamodb.endpoint" to store.configuration.endpoint,
            "dynamodb.table" to store.table,
        )
        ApplicationContext.run(EmbeddedServer::class.java, properties).use { server ->
            HttpClient.newHttpClient().use { http ->
                val url = "${server.url}/artifacts/${run.identityId}/llms.txt"
                val response = http.send(HttpRequest.newBuilder(URI(url)).GET().build(), HttpResponse.BodyHandlers.ofByteArray())
                assertEquals(200, response.statusCode())
                assertArrayEquals(result.bytes, response.body())
                assertTrue(
                    response
                        .headers()
                        .firstValue("Content-Type")
                        .orElse("")
                        .contains("text/plain"),
                )
                val cached = http.send(
                    HttpRequest
                        .newBuilder(URI(url))
                        .header("If-None-Match", response.headers().firstValue("ETag").orElseThrow())
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofByteArray(),
                )
                assertEquals(304, cached.statusCode())
                assertEquals(0, cached.body().size)
                listOf(
                    "/api/crawls/${run.identityId}",
                    "/api/crawls/${run.identityId}/resources",
                ).forEach { path ->
                    assertEquals(
                        200,
                        http.send(HttpRequest.newBuilder(URI("${server.url}$path")).GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode(),
                    )
                }
            }
        }
    }

    @Test
    fun `robots disallow prevents content request and redirects cannot expand effective origin`() {
        val seen = mutableListOf<String>()
        val transport = PageTransport { url, _, _ ->
            seen.add(url.toString())
            when (url.encodedPath) {
                "/robots.txt" -> FetchResponse(
                    url,
                    200,
                    mapOf("Content-Type" to listOf("text/plain")),
                    "User-agent: LlmsTxtGenerator\nDisallow: /private\n".toByteArray(),
                )

                "/redirect" -> FetchResponse(url, 302, mapOf("Location" to listOf("https://other.example/")), byteArrayOf())

                else -> FetchResponse(url, 200, emptyMap(), byteArrayOf())
            }
        }
        val fetcher = SafeFetcher(transport, origin, config, clock, urls)
        assertEquals(
            "ROBOTS_DISALLOWED",
            assertThrows(CrawlFailure::class.java) {
                fetcher.get("https://fixture.example/private", CrawlBudget(60, 5000))
            }.code,
        )
        assertFalse(seen.any { it.endsWith("/private") })
        assertEquals(
            "OUT_OF_SCOPE",
            assertThrows(CrawlFailure::class.java) {
                fetcher.get("https://fixture.example/redirect", CrawlBudget(60, 5000), "https://fixture.example")
            }.code,
        )
        assertFalse(seen.any { it.startsWith("https://other.example/") })
    }

    @Test
    fun `Retry After is durable monotonic and reconstructing coordinator respects it`() {
        val backoff = OriginBackoff(store, config)
        origin.block("https://fixture.example", "600")
        origin.block("https://fixture.example", "10")
        assertEquals(clock.now + 600000, backoff.until("https://fixture.example"))
        assertTrue(OriginCoordinator(config, backoff, clock).tryCrawl("https://fixture.example") == null)
        val item = store.get(DomainKeys.origin("https://fixture.example"))
        assertTrue(item.n("expiresAtEpochSeconds") * 1000 > item.n("blockedUntil"))
    }

    @Test
    fun `dispatcher recovers running attempts while retaining queued work using the actual WorkIndex`() {
        val queuedRun = queued()
        val runningRun = execution.claim(queued("https://second.example/"))!!
        clock.now = requireNotNull(runningRun.recoveryAt) + 1000
        val fetcher = SafeFetcher(PageTransport { _, _, _ -> fail("No expired run may fetch") }, origin, config, clock, urls)
        val dispatcher = WorkDispatcher(execution, CrawlRunner(fetcher, config, clock), origin, config, settings, clock)
        val occupiedOrigin = requireNotNull(origin.tryCrawl(queuedRun.originHint))
        try {
            val deadline = System.nanoTime() + 5_000_000_000
            while (!store.run(runningRun.runId)!!.status.terminal()) {
                dispatcher.tick()
                assertTrue(System.nanoTime() < deadline)
                Thread.sleep(20)
            }
            assertEquals(RunStatus.QUEUED, store.run(queuedRun.runId)!!.status)
            assertEquals(RunStatus.TIMED_OUT, store.run(runningRun.runId)!!.status)
            assertEquals(Capacity(1, 1), store.capacity())
        } finally {
            occupiedOrigin.close()
            dispatcher.close()
        }
    }

    private fun proxy(intercept: (String, Array<out Any?>, () -> Any?) -> Any?): DynamoDbClient = Proxy.newProxyInstance(
        DynamoDbClient::class.java.classLoader,
        arrayOf(DynamoDbClient::class.java),
    ) { _, method, arguments ->
        val args = arguments ?: emptyArray()
        intercept(method.name, args) {
            try {
                method.invoke(client, *args)
            } catch (exception: InvocationTargetException) {
                throw exception.cause!!
            }
        }
    } as DynamoDbClient

    @Test
    fun `publication stores the complete current result in one guarded transaction without staging writes`() {
        val run = execution.claim(queued())!!
        val candidate = result(run)
        var publications = 0
        val wrapped = proxy { name, args, delegate ->
            assertFalse(name in setOf("batchWriteItem", "putItem", "updateItem"), "Publication must have no preparatory writes.")
            if (name == "transactWriteItems") {
                val request = args[0] as TransactWriteItemsRequest
                val currentResult = request.transactItems().single { it.put() != null }.put()
                assertEquals("RESULT", currentResult.item().s("sk"))
                assertArrayEquals(candidate.bytes, CurrentResultCodec.bytes(currentResult.item()))
                assertEquals(candidate.resources.size, currentResult.item().resources().size)
                publications++
            }
            delegate()
        }
        val adapter = ExecutionStore(AdmissionStore(wrapped, store.configuration), settings, config, clock)
        assertEquals(PublicationDecision.UPDATED, adapter.publish(run, candidate).publicationDecision)
        assertEquals(1, publications)
        assertEquals(Capacity(0, 1), store.capacity())
    }

    @Test
    fun `oversized generated file is rejected before installing any current result`() {
        val run = execution.claim(queued())!!
        val oversized = result(run).copy(bytes = ByteArray(config.content.maxLlmsBytes + 1))
        assertThrows(IllegalArgumentException::class.java) { execution.publish(run, oversized) }
        assertTrue(store.get(CurrentResultCodec.key(run.identityId)).isEmpty())
        assertFalse(store.meta(run.identityId)!!.hasCanonical())
        assertEquals(Capacity(1, 1), store.capacity())
    }

    @Test
    fun `oversized aggregate evidence cannot replace the last accepted result`() {
        val first = requireNotNull(execution.claim(queued()))
        val original = result(first)
        publish(first, original)
        val before = requireNotNull(store.meta(first.identityId))
        val next = requireNotNull(execution.claim(newRun(first.identityId)))
        val individuallyBounded = original.resources.single().copy(description = "Useful details ".repeat(350))
        val candidate = result(next).copy(
            resources = (1..100).map { index -> individuallyBounded.copy(resourceKey = "resource-$index") },
        )
        assertTrue(resourceItem(individuallyBounded).toString().toByteArray().size <= config.content.maxResourceMetadataBytes)

        assertThrows(IllegalArgumentException::class.java) { execution.publish(next, candidate) }

        val after = requireNotNull(store.meta(first.identityId))
        assertEquals(before.acceptedObservationSequence, after.acceptedObservationSequence)
        assertEquals(before.currentArtifactHash, after.currentArtifactHash)
        assertArrayEquals(original.bytes, CurrentResultCodec.bytes(store.get(CurrentResultCodec.key(first.identityId))))
        assertEquals(Capacity(1, 1), store.capacity())
    }

    @Test
    fun `a lost committed publication response resolves from terminal run without extra version or quota release`() {
        val run = execution.claim(queued())!!
        val result = result(run)
        val hash = sha256(result.bytes.toString(Charsets.UTF_8))
        var lost = false
        val wrapped = proxy { name, args, delegate ->
            val response = delegate()
            if (name == "transactWriteItems" && args.firstOrNull() is TransactWriteItemsRequest && !lost) {
                lost = true
                throw SdkClientException
                    .create("Injected lost acknowledgement")
            }
            response
        }
        val adapter = ExecutionStore(AdmissionStore(wrapped, store.configuration), settings, config, clock)
        assertEquals(PublicationDecision.UPDATED, adapter.publish(run, result).publicationDecision)
        assertTrue(lost)
        assertEquals(Capacity(0, 1), store.capacity())
        assertEquals(hash, store.get(CurrentResultCodec.key(run.identityId)).s("outputHash"))
    }

    @Test
    fun `deadline expiration during ambiguous retry prevents a new publication request`() {
        val run = execution.claim(queued())!!
        val result = result(run)
        val hash = sha256(result.bytes.toString(Charsets.UTF_8))
        var writes = 0
        val wrapped = proxy { name, args, delegate ->
            if (name == "transactWriteItems" && args.firstOrNull() is TransactWriteItemsRequest) {
                val transaction = args[0] as TransactWriteItemsRequest
                if (transaction.transactItems().any { it.put()?.item()?.ss("sk") == "RESULT" }) {
                    writes++
                    clock.now = run.deadlineAt!! + 1
                    throw SdkClientException
                        .create("Injected timeout before commit")
                }
            }
            delegate()
        }
        val adapter = ExecutionStore(AdmissionStore(wrapped, store.configuration), settings, config, clock)
        assertEquals(RunStatus.TIMED_OUT, adapter.publish(run, result).status)
        assertEquals(1, writes)
        assertFalse(store.meta(run.identityId)!!.hasCanonical())
        assertEquals(Capacity(0, 0), store.capacity())
    }

    @Test
    fun `changed content with identical rendered bytes advances source hashes without updating output date`() {
        val first = execution.claim(queued())!!
        val original = result(first)
        publish(first, original)
        val before = store.meta(first.identityId)!!
        val next = execution.claim(newRun(first.identityId))!!
        val changed = original.copy(
            resources = original.resources.map { it.copy(contentHash = "changed content hash") },
            observedFrom = next.startedAt!!,
        )
        assertEquals(PublicationDecision.UNCHANGED, publish(next, changed).publicationDecision)
        val after = store.meta(first.identityId)!!
        assertEquals(before.lastOutputUpdatedAt, after.lastOutputUpdatedAt)
        assertEquals(next.observationSequence, after.acceptedObservationSequence)
        val current = store.get(CurrentResultCodec.key(first.identityId))
        assertEquals("changed content hash", current.resources().single().contentHash)
        assertArrayEquals(original.bytes, CurrentResultCodec.bytes(current))
    }

    @Test
    fun `default crawl stops after ten page attempts even when more useful candidates remain`() {
        val running = requireNotNull(execution.claim(queued()))
        val seen = mutableListOf<String>()
        val links = (1..15).joinToString("") { "<a href='/guide-$it'>Integration guide $it</a>" }
        val seedHtml = "<h1>Integration documentation</h1><main>${"Useful integration source guidance. ".repeat(20)}$links</main>"
        val sources = mapOf("/" to (200 to seedHtml)) + (1..15).associate { index ->
            "/guide-$index" to (200 to "<h1>Integration guide $index</h1><main>${"Integration instructions for guide $index. ".repeat(20)}</main>")
        }
        val runner = CrawlRunner(SafeFetcher(fixtureTransport(sources, seen), origin, config, clock, urls), config, clock)

        val generated = runner.run(running, emptyList(), CrawlBudget(60, 5000)) { _, _ -> }

        assertEquals(10, generated.coverage.pagesAttempted)
        assertTrue(generated.resources.size <= 10)
        assertTrue(generated.coverage.limitReasons.contains("PAGE_LIMIT"))
        assertEquals(9, seen.count { URI(it).path.startsWith("/guide-") })
    }

    @Test
    fun `reducing page budget to ten revalidates a previously accepted eleven resource baseline`() {
        settings.maxPages = 30
        settings.maxSelectedResources = 20
        val initial = requireNotNull(execution.claim(queued()))
        val template = result(initial).resources.single()
        val previousResources = (1..11).map { index ->
            val url = "https://fixture.example/prior-$index"
            template.copy(resourceKey = sha256(url), sourceUrl = url, primaryHtmlUrl = url, preferredLinkUrl = url, aliases = listOf(url))
        }
        val accepted = result(initial, resources = previousResources)
        publish(initial, accepted)
        val before = requireNotNull(store.meta(initial.identityId))
        settings.maxPages = 10
        settings.maxSelectedResources = 10
        val refresh = requireNotNull(execution.claim(newRun(initial.identityId)))
        val html = "<h1>Integration documentation</h1><main>${"Useful integration source guidance. ".repeat(20)}</main>"
        val sources = mapOf("/" to (200 to html)) + (1..11).associate { "/prior-$it" to (200 to html) }
        val seen = mutableListOf<String>()
        val runner = CrawlRunner(SafeFetcher(fixtureTransport(sources, seen), origin, config, clock, urls), config, clock)

        val generated = runner.run(refresh, execution.baseline(before), CrawlBudget(60, 5000)) { _, _ -> }
        val completed = publish(refresh, generated)
        val after = requireNotNull(store.meta(initial.identityId))

        assertEquals(RunStatus.COMPLETED, completed.status)
        assertEquals(11, seen.count { URI(it).path.startsWith("/prior-") })
        assertEquals(12, generated.coverage.pagesAttempted)
        assertTrue(generated.resources.size <= 10)
        assertTrue(after.acceptedObservationSequence > before.acceptedObservationSequence)
        assertEquals(0, store.capacity().outstandingRuns)
    }

    @Test
    fun `request budget prevents silent deletion of an unvisited selected baseline`() {
        val run = execution.claim(queued())!!
        val limited = run.copy(budgets = run.budgets.copy(maxPages = 1))
        val seen = mutableListOf<String>()
        val html = "<h1>Useful seed</h1><main>${"Useful detailed source text. ".repeat(20)}</main>"
        val runner = CrawlRunner(SafeFetcher(fixtureTransport(mapOf("/" to (200 to html)), seen), origin, config, clock, urls), config, clock)
        val child = result(run).resources.single().copy(sourceUrl = "https://fixture.example/prior", primaryHtmlUrl = "https://fixture.example/prior")
        val error = assertThrows(CrawlFailure::class.java) { runner.run(limited, listOf(child), CrawlBudget(2, 5000)) { _, _ -> } }
        assertEquals("INCOMPLETE_BASELINE", error.code)
        assertFalse(seen.any { it.endsWith("/prior") })
    }

    @Test
    fun `HTML depth is bounded and action routes do not exclude authentication documentation`() {
        val run = execution.claim(queued())!!.let { it.copy(budgets = it.budgets.copy(maxDepth = 1, maxPages = 3)) }
        val seen = mutableListOf<String>()
        val html = "<h1>Useful seed</h1><main>${"Useful source guidance. ".repeat(
            20,
        )}<a href='/docs/authentication'>Authentication guide</a><a href='/login'>Login</a></main>"
        val child = "<h1>Authentication guide</h1><main>${"Authentication details. ".repeat(20)}<a href='/deep'>Deep page</a></main>"
        val runner = CrawlRunner(
            SafeFetcher(
                fixtureTransport(mapOf("/" to (200 to html), "/docs/authentication" to (200 to child)), seen),
                origin,
                config,
                clock,
                urls,
            ),
            config,
            clock,
        )
        val result = runner.run(run, emptyList(), CrawlBudget(60, 5000)) { _, _ -> }
        assertEquals(2, result.resources.size)
        assertFalse(seen.any { it.endsWith("/login") || it.endsWith("/deep") })
    }

    @Test
    fun `two concurrent publishers finish with the highest sufficient sequence and balanced quotas`() {
        val olderRun = execution.claim(queued())!!
        val newerRun = execution.claim(newRun(olderRun.identityId))!!
        val olderResult = result(olderRun, "older")
        val newerResult = result(newerRun, "newer")
        val newerArtifactHash = sha256(newerResult.bytes.toString(Charsets.UTF_8))
        val pool = Executors.newFixedThreadPool(2)
        val start = CountDownLatch(1)

        try {
            val olderPublication = pool.submit<CrawlRun> {
                start.await()
                execution.publish(olderRun, olderResult)
            }
            val newerPublication = pool.submit<CrawlRun> {
                start.await()
                execution.publish(newerRun, newerResult)
            }
            start.countDown()

            assertTrue(olderPublication.get().status.terminal())
            assertEquals(PublicationDecision.UPDATED, newerPublication.get().publicationDecision)
            val meta = store.meta(olderRun.identityId)!!
            assertEquals(newerRun.observationSequence, meta.acceptedObservationSequence)
            assertEquals(newerArtifactHash, meta.currentArtifactHash)
            assertEquals(Capacity(0, 1), store.capacity())
            assertEquals(0L, meta.outstandingRunCount)
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `a sitemap page expands only after a bounded HTML path is discovered later`() {
        val run = execution.claim(queued())!!.let { it.copy(budgets = it.budgets.copy(maxDepth = 4)) }
        val seen = mutableListOf<String>()

        fun html(
            title: String,
            links: String = "",
        ) = "<h1>$title</h1><main>${("Detailed " + title + " guidance. ").repeat(20)}$links</main>"
        val transport = fixtureTransport(
            mapOf(
                "/" to (200 to html("Seed", "<a href='/one'>One</a>")),
                "/one" to (200 to html("One", "<a href='/two'>Two</a>")),
                "/two" to (200 to html("Two", "<a href='/unknown'>Unknown</a>")),
                "/unknown" to (200 to html("Unknown", "<a href='/nested'>Nested</a>")),
                "/nested" to (200 to html("Nested")),
                "/sitemap.xml" to
                    (
                        200 to
                            (
                                "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>" +
                                    "<url><loc>https://fixture.example/unknown</loc></url></urlset>"
                            )
                    ),
            ),
            seen,
        )
        val runner = CrawlRunner(SafeFetcher(transport, origin, config, clock, urls), config, clock)
        val result = runner.run(run, emptyList(), CrawlBudget(60, 5000)) { _, _ -> }
        assertTrue(seen.indexOf("https://fixture.example/unknown") < seen.indexOf("https://fixture.example/two"))
        assertTrue(seen.contains("https://fixture.example/nested"))
        assertEquals(3, result.resources.first { it.primaryHtmlUrl.endsWith("/unknown") }.depth)
        assertEquals(4, result.resources.first { it.primaryHtmlUrl.endsWith("/nested") }.depth)
    }
}
