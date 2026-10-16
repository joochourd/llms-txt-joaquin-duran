package com.joaquinduran.llmstxt.ingestion

import com.joaquinduran.llmstxt.api.dto.BatchResponse
import com.joaquinduran.llmstxt.api.errors.ApiProblem
import com.joaquinduran.llmstxt.api.query.AdmissionStatus
import com.joaquinduran.llmstxt.crawl.fetch.OriginBackoff
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.BatchCodec
import com.joaquinduran.llmstxt.persistence.dynamo.CrawlMetaCodec
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoClients
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoConfiguration
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoSchema
import com.joaquinduran.llmstxt.persistence.dynamo.Item
import com.joaquinduran.llmstxt.persistence.dynamo.LocalTableBootstrap
import com.joaquinduran.llmstxt.persistence.dynamo.RunCodec
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import io.micronaut.context.ApplicationContext
import io.micronaut.json.JsonMapper
import io.micronaut.runtime.server.EmbeddedServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Tag("integration")
class AdmissionIntegrationTest {
    private lateinit var dynamo: DynamoConfiguration
    private lateinit var client: DynamoDbClient
    private lateinit var store: AdmissionStore
    private lateinit var settings: AdmissionConfiguration
    private lateinit var urls: UrlIdentityResolver
    private lateinit var service: AdmissionService
    private lateinit var status: AdmissionStatus
    private val clock = MutableClock()

    @BeforeEach
    fun setUp() {
        val endpoint = requireNotNull(System.getenv("DYNAMODB_ENDPOINT")) { "DYNAMODB_ENDPOINT is required for integration tests." }
        require(endpoint.isNotBlank())
        dynamo = DynamoConfiguration.fromEnvironment(mapOf("DYNAMODB_ENDPOINT" to endpoint)).apply { table = "llms-txt-m1-${UUID.randomUUID()}" }
        client = DynamoClients.create(dynamo)
        LocalTableBootstrap(client, dynamo).initialize()
        store = AdmissionStore(client, dynamo)
        settings = AdmissionConfiguration()
        urls = UrlIdentityResolver(settings)
        service = AdmissionService(store, settings, urls, clock)
        status = AdmissionStatus(store, service, clock)
    }

    @AfterEach
    fun tearDown() {
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName(dynamo.table).build())
        } finally {
            client.close()
        }
    }

    @Test
    fun `mixed batch preserves positions and inputs while duplicates share durable work`() {
        val inputs = listOf(" example.com/docs ", "https://EXAMPLE.com:443/docs#section", "ftp://invalid.example", "http://127.0.0.1/")
        val batch = submitAndProcess(service, store, clock, inputs, false)
        val result = status.batch(batch.batchId)
        assertEquals("PROCESSING", result.status)
        assertTrue(result.admissionComplete)
        assertEquals(inputs, result.items.map { it.inputUrl })
        assertEquals(listOf("QUEUED", "COALESCED", "REJECTED", "REJECTED"), result.items.map { it.admission })
        assertEquals(result.items[0].runId, result.items[1].runId)
        assertEquals("INVALID_URL", result.items[2].error?.code)
        assertEquals("UNSAFE_TARGET", result.items[3].error?.code)
        val run = status.run(result.items[0].runId!!)
        assertEquals(RunStatus.QUEUED, run.status)
        assertNull(run.observationSequence)
        assertTrue(run.capacityReserved)
        assertNull(run.expiresAtEpochSeconds)
        assertEquals(settings.budgets(), run.budgets)
        assertEquals(Capacity(1, 1), store.capacity())
        val meta = store.meta(run.identityId)!!
        assertEquals(1L, meta.outstandingRunCount)
        assertEquals(MonitoringState.RESERVED, meta.monitoringState)
        assertNull(run.queueDeadlineAt)
        assertEquals(run.queuedAt + 210_000, meta.monitoringReservationExpiresAt)
        assertNull(meta.activeHintExpiresAt)
        assertEquals(run, RunCodec.decode(store.get(DomainKeys.run(run.runId))))
        assertEquals(store.rows(batch.batchId, true)[0], service.admitPending(batch, 0, inputs[0]))
        assertEquals(Capacity(1, 1), store.capacity())
        // Real sparse index entries are discoverable; no Scan is used by application code.
        awaitIndex("WorkIndex", "workPk", run.workPk!!, DomainKeys.run(run.runId))
        awaitIndex("DueIndex", "duePk", meta.duePk!!, DomainKeys.crawl(meta.identityId))
    }

    @Test
    fun `ten entries are accepted and invalid batch envelopes do not write data`() {
        assertThrows(ApiProblem::class.java) { submitAndProcess(service, store, clock, emptyList(), false) }
        assertThrows(ApiProblem::class.java) { submitAndProcess(service, store, clock, List(1001) { "example.com" }, false) }
        val batch = submitAndProcess(service, store, clock, List(10) { "https://example.com/path-$it" }, false)
        assertEquals(10, status.batch(batch.batchId).items.size)
        assertEquals(Capacity(10, 10), store.capacity())
    }

    @Test
    fun `full work capacity defers new work but allows known active reuse`() {
        settings.maxOutstandingRuns = 1
        val first = submitAndProcess(service, store, clock, listOf("example.com"), false)
        val second = status.batch(submitAndProcess(service, store, clock, listOf("example.com", "other.example.com"), true).batchId)
        assertEquals("COALESCED", second.items[0].admission)
        assertEquals("PENDING", second.items[1].status)
        assertNull(second.items[1].error)
        assertEquals(status.batch(first.batchId).items[0].runId, second.items[0].runId)
        assertEquals(Capacity(1, 1), store.capacity())
        assertEquals(0L, store.meta(urls.normalize("other.example.com").identityId)!!.outstandingRunCount)
    }

    @Test
    fun `monitoring quota preserves generation without reserving another monitoring slot`() {
        settings.maxMonitoredIdentities = 1
        val result = status.batch(submitAndProcess(service, store, clock, listOf("one.example.com", "two.example.com"), false).batchId)
        assertEquals("QUEUED", result.items[1].admission)
        assertNull(result.items[1].error)
        assertEquals(Capacity(2, 1), store.capacity())
        assertFalse(store.meta(result.items[1].identityId!!)!!.monitoringSlotReserved)
    }

    @Test
    fun `unknown active work can duplicate but only one initial monitoring slot is reserved and extended`() {
        val first = status.batch(submitAndProcess(service, store, clock, listOf("example.com"), false).batchId).items.single()
        val before = store.meta(first.identityId!!)!!
        // A lost hint is explicitly allowed. It must not stand in for outstandingRunCount.
        client.updateItem(
            UpdateItemRequest
                .builder()
                .tableName(dynamo.table)
                .key(DomainKeys.crawl(before.identityId))
                .updateExpression("REMOVE activeRunIdHint, activeHintExpiresAt ADD metadataRevision :one")
                .expressionAttributeValues(mapOf(":one" to number(1)))
                .build(),
        )
        clock.now += 1000
        val second = status.batch(submitAndProcess(service, store, clock, listOf("example.com"), false).batchId).items.single()
        assertNotEquals(first.runId, second.runId)
        val after = store.meta(before.identityId)!!
        assertEquals(Capacity(2, 1), store.capacity())
        assertEquals(2L, after.outstandingRunCount)
        assertEquals(before.monitoringReservationExpiresAt!! + 1000, after.monitoringReservationExpiresAt)
        assertTrue(store.run(first.runId!!)!!.capacityReserved)
    }

    @Test
    fun `freshness uses observation start and compatible pipeline and force preserves coalescing`() {
        val identity = seedCanonical("example.com", clock.now - 1000)
        val cachedBatch = submitAndProcess(service, store, clock, listOf("example.com"), false)
        val cached = status.batch(cachedBatch.batchId).items.single()
        assertEquals("CACHED", cached.admission)
        assertEquals(7L, cached.versionSequence)
        assertEquals(9L, cached.acceptedObservationSequence)
        assertEquals(Capacity(0, 0), store.capacity())
        val forced = status.batch(submitAndProcess(service, store, clock, listOf("example.com"), true).batchId).items.single()
        assertEquals("QUEUED", forced.admission)
        assertEquals(MonitoringState.EXPIRED, store.meta(identity.identityId)!!.monitoringState)
        val reused = status.batch(submitAndProcess(service, store, clock, listOf("example.com"), true).batchId).items.single()
        assertEquals("COALESCED", reused.admission)
        assertEquals(forced.runId, reused.runId)
        assertEquals(Capacity(1, 0), store.capacity())
        val currentBatch = status.batch(cachedBatch.batchId)
        assertEquals("/artifacts/${identity.identityId}/llms.txt", currentBatch.items.single().artifactUrl)
        assertNull(currentBatch.items.single().error)
    }

    @Test
    fun `failed attempts cannot renew freshness and incompatible pipeline cannot reuse old work`() {
        val acceptedPipeline = settings.pipelineVersion
        val identity = seedCanonical("example.com", clock.now - 901_000)
        val first = status.batch(submitAndProcess(service, store, clock, listOf("example.com"), false).batchId).items.single()
        assertEquals("QUEUED", first.admission)

        settings.pipelineVersion = "test-upgraded-pipeline"
        val second = status.batch(submitAndProcess(service, store, clock, listOf("example.com"), false).batchId).items.single()
        assertEquals("QUEUED", second.admission)
        assertNotEquals(first.runId, second.runId)
        val meta = store.meta(identity.identityId)!!
        assertEquals("test-upgraded-pipeline", meta.requiredPipelineVersion)
        assertEquals(acceptedPipeline, meta.acceptedPipelineVersion)
        assertEquals(9L, meta.acceptedObservationSequence)
        assertEquals(0L, meta.nextObservationSequence)
        assertEquals(Capacity(2, 0), store.capacity())
    }

    @Test
    fun `durable origin backoff remains a row error after reuse checks`() {
        val identity = seedCanonical("example.com", clock.now - 901_000)
        store.putAbsent(base("ORIGIN#${sha256(identity.origin)}", "META", "OriginBackoff") + mapOf("blockedUntil" to number(clock.now + 60_000)))

        val backedOff = status.batch(submitAndProcess(service, store, clock, listOf("example.com"), true).batchId).items.single()

        assertEquals("ORIGIN_BACKOFF", backedOff.error?.code)
        assertEquals(Capacity(0, 0), store.capacity())
    }

    @Test
    fun `parallel admissions of the same known identity coalesce safely`() {
        val rows = parallel(12) { status.batch(submitAndProcess(service, store, clock, listOf("example.com"), it % 2 == 0).batchId).items.single() }
        assertEquals(1, rows.map { it.runId }.toSet().size)
        assertEquals(1, rows.count { it.admission == "QUEUED" })
        assertEquals(Capacity(1, 1), store.capacity())
        assertEquals(1L, store.meta(rows.first().identityId!!)!!.outstandingRunCount)
    }

    @Test
    fun `parallel distinct admissions cannot oversubscribe either global quota`() {
        settings.maxOutstandingRuns = 4
        settings.maxMonitoredIdentities = 3
        val rows =
            parallel(
                12,
            ) { status.batch(submitAndProcess(service, store, clock, listOf("https://site-$it.example.com"), false).batchId).items.single() }
        assertEquals(4, rows.count { it.admission == "QUEUED" })
        assertEquals(Capacity(4, 3), store.capacity())
        assertEquals(12 - settings.maxOutstandingRuns.toInt(), rows.count { it.status == "PENDING" })
        assertTrue(rows.all { it.error == null })
        rows.filter { it.identityId != null }.forEach { row ->
            val meta = store.meta(row.identityId!!)!!
            assertEquals(if (row.runId == null) 0L else 1L, meta.outstandingRunCount)
        }
    }

    @Test
    fun `parallel distinct admissions cannot exceed outstanding work quota`() {
        settings.maxOutstandingRuns = 3
        val rows =
            parallel(
                12,
            ) { status.batch(submitAndProcess(service, store, clock, listOf("https://work-$it.example.com"), false).batchId).items.single() }
        assertEquals(3, rows.count { it.admission == "QUEUED" })
        assertEquals(Capacity(3, 3), store.capacity())
        assertEquals(12 - settings.maxOutstandingRuns.toInt(), rows.count { it.status == "PENDING" })
        assertTrue(rows.all { it.error == null })
    }

    @Test
    fun `deadline repair and competing position admissions leave no orphan run or quota`() {
        val batch = Batch(
            UUID.randomUUID().toString(),
            listOf("example.com", "other.example.com"),
            false,
            clock.now,
            clock.now + 1000,
            clock.now / 1000 + 86400,
        )
        store.putAbsent(BatchCodec.encode(batch))
        service.admit(batch, 0)
        assertEquals("ADMITTING", status.batch(batch.batchId).status)
        clock.now += 1001
        parallel(8) { if (it % 2 == 0) service.repair(batch) else service.admit(batch, 1) }
        val result = status.batch(batch.batchId)
        assertTrue(result.admissionComplete)
        assertEquals("ADMISSION_INTERRUPTED", result.items[1].error?.code)
        assertEquals(Capacity(1, 1), store.capacity())
        assertEquals(2, store.rows(batch.batchId, true).size)
        service.repair(batch)
        assertEquals(Capacity(1, 1), store.capacity())
    }

    @Test
    fun `a transaction sent late loses to deadline repair atomically`() {
        val batch = Batch(
            UUID.randomUUID().toString(),
            listOf("example.com"),
            false,
            clock.now,
            clock.now + 1000,
            clock.now / 1000 + 86400,
        )
        store.putAbsent(BatchCodec.encode(batch))
        val built = CountDownLatch(1)
        val release = CountDownLatch(1)
        val wrapped = wrap { method, args ->
            if (method.name == "transactWriteItems") {
                built.countDown()
                assertTrue(release.await(10, TimeUnit.SECONDS))
            }
            invoke(method, args)
        }
        val lateService = AdmissionService(AdmissionStore(wrapped, dynamo), settings, urls, clock)
        val executor = Executors.newSingleThreadExecutor()
        try {
            val future = executor.submit<BatchItem> { lateService.admit(batch, 0) }
            assertTrue(built.await(10, TimeUnit.SECONDS))
            clock.now += 1001
            service.repair(batch)
            release.countDown()
            assertEquals("ADMISSION_INTERRUPTED", future.get(10, TimeUnit.SECONDS).error?.code)
            assertEquals(Capacity(0, 0), store.capacity())
            assertEquals(0L, store.meta(urls.normalize("example.com").identityId)!!.outstandingRunCount)
            assertEquals("COMPLETED", status.batch(batch.batchId).status)
        } finally {
            release.countDown()
            executor.shutdownNow()
        }
    }

    @Test
    fun `concurrent calls for the same batch position cannot reserve twice`() {
        val batch = Batch(
            UUID.randomUUID().toString(),
            listOf("example.com"),
            false,
            clock.now,
            clock.now + 30_000,
            clock.now / 1000 + 86400,
        )
        store.putAbsent(BatchCodec.encode(batch))
        val rows = parallel(8) { service.admit(batch, 0) }
        assertEquals(1, rows.toSet().size)
        assertEquals(Capacity(1, 1), store.capacity())
        assertEquals(1, store.rows(batch.batchId, true).size)
    }

    @Test
    fun `ambiguous transaction response is resolved from the row without double allocation`() {
        val dropped = AtomicBoolean(false)
        val wrapped = wrap { method, args ->
            val result = invoke(method, args)
            if (method.name == "transactWriteItems" &&
                (args!![0] as TransactWriteItemsRequest).transactItems().any {
                    it
                        .put()
                        ?.item()
                        ?.get("entityType")
                        ?.s() == "CrawlRun"
                } &&
                dropped.compareAndSet(false, true)
            ) {
                throw SdkClientException.create("Simulated response loss")
            }
            result
        }
        val ambiguousStore = AdmissionStore(wrapped, dynamo)
        val ambiguousService = AdmissionService(ambiguousStore, settings, urls, clock)
        val admitted = submitAndProcess(ambiguousService, ambiguousStore, clock, listOf("example.com"), false)
        assertTrue(dropped.get())
        assertNotNull(store.rows(admitted.batchId, true).single().runId)
        assertEquals(Capacity(1, 1), store.capacity())
    }

    @Test
    fun `transaction conflicts are retried with stable tokens and batch reads retry unprocessed keys`() {
        val conflicts = AtomicBoolean(false)
        val unprocessed = AtomicBoolean(false)
        val tokens = mutableListOf<String>()
        val wrapped = wrap { method, args ->
            if (method.name == "transactWriteItems" &&
                (args!![0] as TransactWriteItemsRequest).transactItems().any {
                    it
                        .put()
                        ?.item()
                        ?.get("entityType")
                        ?.s() == "CrawlRun"
                }
            ) {
                tokens.add((args[0] as TransactWriteItemsRequest).clientRequestToken())
                if (conflicts.compareAndSet(false, true)) {
                    throw TransactionCanceledException
                        .builder()
                        .cancellationReasons(CancellationReason.builder().code("TransactionConflict").build())
                        .build()
                }
            }
            if (method.name == "batchGetItem" && unprocessed.compareAndSet(false, true)) {
                BatchGetItemResponse.builder().unprocessedKeys((args!![0] as BatchGetItemRequest).requestItems()).build()
            } else {
                invoke(method, args)
            }
        }
        val retryStore = AdmissionStore(wrapped, dynamo)
        val retryService = AdmissionService(retryStore, settings, urls, clock)
        val batch = submitAndProcess(retryService, retryStore, clock, listOf("example.com"), false)
        assertEquals(tokens[0], tokens[1])
        assertEquals(1, tokens.toSet().size)
        val result = AdmissionStatus(retryStore, retryService, clock).batch(batch.batchId)
        assertNotNull(result.items.single().runId)
        assertTrue(unprocessed.get())
        assertEquals(Capacity(1, 1), store.capacity())
    }

    @Test
    fun `missing recent eventual reads fall back to strong base table reads`() {
        val batch = submitAndProcess(service, store, clock, listOf("example.com"), false)
        val wrapped = wrap { method, args ->
            when {
                method.name == "getItem" && !(args!![0] as GetItemRequest).consistentRead() -> {
                    GetItemResponse.builder().build()
                }

                method.name == "query" && !(args!![0] as QueryRequest).consistentRead() -> {
                    QueryResponse.builder().build()
                }

                method.name == "batchGetItem" && !(args!![0] as BatchGetItemRequest).requestItems().getValue(dynamo.table).consistentRead() -> {
                    BatchGetItemResponse.builder().build()
                }

                else -> {
                    invoke(method, args)
                }
            }
        }
        val reads = AdmissionStatus(AdmissionStore(wrapped, dynamo), service, clock)
        val result = reads.batch(batch.batchId)
        assertEquals("PROCESSING", result.status)
        assertEquals(RunStatus.QUEUED, reads.run(result.items.single().runId!!).status)
    }

    @Test
    fun `unresolved storage response has bounded retries and deadline repair does not replay work`() {
        val tokens = mutableListOf<String>()
        val wrapped = wrap { method, args ->
            if (method.name == "transactWriteItems") {
                tokens.add((args!![0] as TransactWriteItemsRequest).clientRequestToken())
                throw SdkClientException.create("Simulated unavailable storage")
            }
            invoke(method, args)
        }
        val retryStore = AdmissionStore(wrapped, dynamo)
        val batch = Batch(
            UUID.randomUUID().toString(),
            listOf("example.com"),
            false,
            clock.now,
            clock.now + 1000,
            clock.now / 1000 + 86400,
        )
        store.putAbsent(BatchCodec.encode(batch))
        assertThrows(StorageUnavailable::class.java) { AdmissionService(retryStore, settings, urls, clock).admit(batch, 0) }
        assertEquals(4, tokens.size)
        assertEquals(1, tokens.toSet().size)
        assertEquals(Capacity(0, 0), store.capacity())
        clock.now += 1001
        val result = status.batch(batch.batchId)
        assertEquals(
            "ADMISSION_INTERRUPTED",
            result.items
                .single()
                .error
                ?.code,
        )
        assertNull(result.items.single().runId)
    }

    @Test
    fun `HTTP contracts cover malformed requests partial admission and missing or expired metadata`() {
        val properties = mapOf<String, Any>(
            "crawl.enabled" to false,
            "micronaut.server.port" to -1,
            "dynamodb.environment" to "local",
            "dynamodb.endpoint" to dynamo.endpoint,
            "dynamodb.table" to dynamo.table,
            "dynamodb.work-index-shards" to dynamo.workIndexShards,
            "dynamodb.due-index-shards" to dynamo.dueIndexShards,
        )
        ApplicationContext.run(EmbeddedServer::class.java, properties).use { server ->
            HttpClient.newHttpClient().use { http ->
                fun request(
                    path: String,
                    body: String? = null,
                ): HttpResponse<String> {
                    val builder = HttpRequest.newBuilder(URI("${server.url}$path")).timeout(Duration.ofSeconds(20))
                    if (body !=
                        null
                    ) {
                        builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body))
                    } else {
                        builder.GET()
                    }
                    return http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
                }
                listOf(
                    "{",
                    "{}",
                    "[]",
                    "{\"urls\":\"example.com\"}",
                    "{\"urls\":[\"example.com\"],\"force\":\"true\"}",
                    "{\"urls\":[]}",
                    "{\"urls\":[null]}",
                    "{\"urls\":[1]}",
                    "{\"urls\":[{}]}",
                    "{\"urls\":[" + List(1001) { "\"example.com\"" }.joinToString(",") + "]}",
                ).forEach {
                    assertEquals(400, request("/api/batches", it).statusCode(), it)
                }
                assertEquals(413, request("/api/batches", "{\"urls\":[\"" + "x".repeat(2_100_000) + "\"]}").statusCode())
                val submitted = request("/api/batches", "{\"urls\":[\"example.com\",\"ftp://example.com\"]}")
                assertEquals(202, submitted.statusCode())
                val mapper = server.applicationContext.getBean(JsonMapper::class.java)
                val receipt = requireNotNull(mapper.readValue(submitted.body().toByteArray(), BatchResponse::class.java))
                assertTrue(receipt.items.all { it.status == "PENDING" && it.error == null })
                BulkBatchProcessor(store, BulkBatchStore(store, clock), service, clock).use { it.process(receipt.batchId) }
                val response = status.batch(receipt.batchId)
                assertTrue(response.createdAt.matches(Regex(".*\\.\\d{3}Z")))
                assertEquals("INVALID_URL", response.items[1].error?.code)
                assertEquals("/api/batches/${response.batchId}", submitted.headers().firstValue("Location").orElse(""))
                assertEquals(200, request("/api/batches/${response.batchId}").statusCode())
                assertEquals(200, request("/api/runs/${response.items[0].runId}").statusCode())
                assertEquals(404, request("/api/runs/missing").statusCode())
                assertEquals(404, request("/api/batches/missing").statusCode())
                assertEquals(202, request("/api/batches", "{\"urls\":[\"ftp://example.com\"]}").statusCode())
                assertEquals(202, request("/api/batches", "{\"urls\":[\"example.com\"],\"force\":true}").statusCode())
                val repeatedForce = request("/api/batches", "{\"urls\":[\"example.com\"],\"force\":true}")
                assertEquals(202, repeatedForce.statusCode())
                val run = requireNotNull(store.run(requireNotNull(response.items[0].runId)))
                ExecutionStore(
                    store,
                    settings,
                    com.joaquinduran.llmstxt.crawl.configuration
                        .CrawlConfiguration(),
                    clock,
                ).finish(run.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)
                BulkBatchProcessor(store, BulkBatchStore(store, clock), service, clock).use { it.process(response.batchId) }
                client.updateItem(
                    UpdateItemRequest
                        .builder()
                        .tableName(dynamo.table)
                        .key(DomainKeys.batch(response.batchId))
                        .updateExpression("SET expiresAtEpochSeconds = :zero, retentionExpiresAtEpochSeconds = :zero")
                        .expressionAttributeValues(mapOf(":zero" to number(0)))
                        .build(),
                )
                assertEquals(410, request("/api/batches/${response.batchId}").statusCode())
                client.updateItem(
                    UpdateItemRequest
                        .builder()
                        .tableName(dynamo.table)
                        .key(DomainKeys.run(response.items[0].runId!!))
                        .updateExpression("SET expiresAtEpochSeconds = :zero")
                        .expressionAttributeValues(mapOf(":zero" to number(0)))
                        .build(),
                )
                assertEquals(410, request("/api/runs/${response.items[0].runId}").statusCode())
                // No worker or business pipeline is present, and M0 health remains live.
                assertEquals(200, request("/health/liveness").statusCode())
                assertEquals(200, request("/health/readiness").statusCode())
                client.deleteItem(
                    DeleteItemRequest
                        .builder()
                        .tableName(dynamo.table)
                        .key(DynamoSchema.capacityKey)
                        .build(),
                )
                val readinessDeadline = System.nanoTime() + java.time.Duration
                    .ofSeconds(5)
                    .toNanos()
                var unavailable = request("/health/readiness")
                while (unavailable.statusCode() == 200 && System.nanoTime() < readinessDeadline) {
                    Thread.sleep(50)
                    unavailable = request("/health/readiness")
                }
                assertEquals(503, unavailable.statusCode())
                assertTrue(unavailable.body().contains("DOWN"))
            }
        }
    }

    private fun seedCanonical(
        input: String,
        observedAt: Long,
    ): CrawlIdentity {
        val identity = urls.normalize(input)
        val meta = CrawlMeta(
            identity.identityId,
            identity.normalizedSeedUrl,
            identity.intentProfile,
            settings.pipelineVersion,
            acceptedPipelineVersion = settings.pipelineVersion,
            acceptedObservationSequence = 9,
            currentVersionSequence = 7,
            currentArtifactHash = "fixture-hash",
            acceptedObservationStartedAt = observedAt,
            monitoringState = MonitoringState.EXPIRED,
            lastAttemptAt = clock.now,
            lastSuccessfulValidationAt = observedAt + 100,
            lastOutputUpdatedAt = observedAt,
        )
        store.putAbsent(CrawlMetaCodec.encode(meta))
        return identity
    }

    private fun replaceMeta(
        id: String,
        transform: (CrawlMeta) -> CrawlMeta,
    ) {
        client.putItem(
            PutItemRequest
                .builder()
                .tableName(dynamo.table)
                .item(CrawlMetaCodec.encode(transform(store.meta(id)!!)))
                .build(),
        )
    }

    private fun awaitIndex(
        index: String,
        attribute: String,
        value: String,
        key: Item,
    ) {
        val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
        do {
            val items = client
                .query(
                    QueryRequest
                        .builder()
                        .tableName(dynamo.table)
                        .indexName(index)
                        .keyConditionExpression("#key = :value")
                        .expressionAttributeNames(mapOf("#key" to attribute))
                        .expressionAttributeValues(mapOf(":value" to text(value)))
                        .build(),
                ).items()
            if (items.any { it["pk"] == key["pk"] && it["sk"] == key["sk"] }) return
            Thread.sleep(20)
        } while (System.nanoTime() < deadline)
        fail<Unit>("$index entry missing")
    }

    private fun <T> parallel(
        count: Int,
        action: (Int) -> T,
    ): List<T> {
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(count)
        try {
            val tasks = (0 until count).map { index ->
                executor.submit<T> {
                    start.await()
                    action(index)
                }
            }
            start.countDown()
            return tasks.map { it.get(30, TimeUnit.SECONDS) }
        } finally {
            executor.shutdownNow()
        }
    }

    private fun wrap(action: (Method, Array<out Any?>?) -> Any?): DynamoDbClient = Proxy.newProxyInstance(
        DynamoDbClient::class.java.classLoader,
        arrayOf(DynamoDbClient::class.java),
    ) { _, method, args -> action(method, args) } as DynamoDbClient

    private fun invoke(
        method: Method,
        args: Array<out Any?>?,
    ): Any? = try {
        method.invoke(client, *(args ?: emptyArray()))
    } catch (exception: InvocationTargetException) {
        throw exception.targetException
    }
}
