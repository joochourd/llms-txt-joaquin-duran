package com.joaquinduran.llmstxt.ingestion

import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import com.joaquinduran.llmstxt.persistence.dynamo.CrawlMetaCodec
import com.joaquinduran.llmstxt.persistence.dynamo.Item
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest

@Tag("integration")
class BatchAdmissionReadsIntegrationTest : MonitoringFixture() {
    @Test
    fun `250 cached identities use grouped metadata reads without individual metadata lookups`() {
        val inputs = (0 until 250).map { "https://cached-$it.example/" }
        inputs.forEach(::seedCachedMetadata)
        val batch = service.submit(inputs, false)
        var individualMetadataReads = 0
        val groupedReads = mutableListOf<BatchGetItemRequest>()
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                if (method == "getItem") {
                    val request = arguments[0] as GetItemRequest
                    if (request
                            .key()
                            .getValue("pk")
                            .s()
                            .startsWith("CRAWL#")
                    ) {
                        individualMetadataReads++
                    }
                }
                if (method == "batchGetItem") groupedReads += arguments[0] as BatchGetItemRequest
                delegate()
            },
            store.configuration,
        )

        processor(adapter).use { processor -> repeat(5) { processor.process(batch.batchId) } }
        val rows = store.rows(batch.batchId, true)

        assertEquals(250, rows.size)
        assertTrue(rows.all { it.admission == Admission.CACHED })
        assertEquals(Capacity(0, 0), store.capacity())
        assertFalse(requireNotNull(store.batch(batch.batchId)).bulkSlotReserved)
        assertEquals(0, individualMetadataReads)
        assertEquals(5, groupedReads.size)
        assertEquals(
            250,
            groupedReads.sumOf {
                it
                    .requestItems()
                    .getValue(store.table)
                    .keys()
                    .size
            },
        )
        assertTrue(groupedReads.all { it.requestItems().getValue(store.table).consistentRead() })
        assertTrue(
            groupedReads.all {
                it
                    .requestItems()
                    .getValue(store.table)
                    .keys()
                    .size in 1..100
            },
        )
    }

    @Test
    fun `normalized duplicate inputs share grouped metadata and active run reads while preserving positions`() {
        val active = queued("https://shared.example/docs")
        val inputs = List(12) { index ->
            if (index % 2 == 0) "shared.example/docs" else "https://SHARED.example:443/docs#section"
        }
        val batch = service.submit(inputs, false)
        val requestedKeys = mutableListOf<List<Item>>()
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                if (method == "batchGetItem") {
                    val request = arguments[0] as BatchGetItemRequest
                    requestedKeys += request.requestItems().getValue(store.table).keys()
                }
                delegate()
            },
            store.configuration,
        )

        processor(adapter).use { it.process(batch.batchId) }
        val rows = store.rows(batch.batchId, true)

        assertEquals(inputs, rows.map { it.seedUrl })
        assertEquals((0 until 12).toList(), rows.map { it.index })
        assertTrue(rows.all { it.admission == Admission.COALESCED && it.runId == active.runId })
        assertEquals(Capacity(1, 1), store.capacity())
        assertTrue(requestedKeys.any { it.single()["pk"]?.s() == "CRAWL#${active.identityId}" })
        assertTrue(requestedKeys.any { it.single()["pk"]?.s() == "RUN#${active.runId}" })
        assertTrue(requestedKeys.all { it.size == 1 })
    }

    @Test
    fun `partial unordered grouped responses retry only unprocessed keys before deciding freshness`() {
        val inputs = (0 until 12).map { "https://partial-$it.example/" }
        inputs.forEach(::seedCachedMetadata)
        val batch = service.submit(inputs, false)
        val requests = mutableListOf<BatchGetItemRequest>()
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                if (method != "batchGetItem") return@proxy delegate()
                val request = arguments[0] as BatchGetItemRequest
                requests += request
                val response = delegate() as BatchGetItemResponse
                val items = response.responses().getValue(store.table)
                if (requests.size == 1) {
                    val remaining = request
                        .requestItems()
                        .getValue(store.table)
                        .toBuilder()
                        .keys(
                            request
                                .requestItems()
                                .getValue(store.table)
                                .keys()
                                .drop(2),
                        ).build()
                    val returnedKeys = request
                        .requestItems()
                        .getValue(store.table)
                        .keys()
                        .take(2)
                        .map { it.getValue("pk") }
                        .toSet()
                    response
                        .toBuilder()
                        .responses(mapOf(store.table to items.filter { it["pk"] in returnedKeys }.reversed()))
                        .unprocessedKeys(mapOf(store.table to remaining))
                        .build()
                } else {
                    response.toBuilder().responses(mapOf(store.table to items.reversed())).build()
                }
            },
            store.configuration,
        )

        processor(adapter).use { it.process(batch.batchId) }

        assertEquals(
            listOf(12, 10),
            requests.map {
                it
                    .requestItems()
                    .getValue(store.table)
                    .keys()
                    .size
            },
        )
        assertEquals(
            requests[0]
                .requestItems()
                .getValue(store.table)
                .keys()
                .drop(2),
            requests[1].requestItems().getValue(store.table).keys(),
        )
        assertEquals(inputs, store.rows(batch.batchId, true).map { it.seedUrl })
        assertTrue(store.rows(batch.batchId, true).all { it.admission == Admission.CACHED })
        assertEquals(Capacity(0, 0), store.capacity())
    }

    @Test
    fun `exhausted grouped reads leave durable inputs pending and a new processor can recover`() {
        val inputs = (0 until 12).map { "https://retry-$it.example/" }
        inputs.forEach(::seedCachedMetadata)
        val batch = service.submit(inputs, false)
        var attempts = 0
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                if (method != "batchGetItem") return@proxy delegate()
                attempts++
                val request = arguments[0] as BatchGetItemRequest
                BatchGetItemResponse.builder().unprocessedKeys(request.requestItems()).build()
            },
            store.configuration,
        )

        processor(adapter).use { processor ->
            assertThrows(StorageUnavailable::class.java) { processor.process(batch.batchId) }
        }

        assertEquals(5, attempts)
        assertTrue(store.rows(batch.batchId, true).isEmpty())
        assertEquals(0, store.batch(batch.batchId)?.nextAdmissionIndex)
        assertTrue(requireNotNull(store.batch(batch.batchId)).bulkSlotReserved)
        assertEquals(Capacity(0, 0), store.capacity())

        processor(store).use { it.process(batch.batchId) }

        assertTrue(store.rows(batch.batchId, true).all { it.admission == Admission.CACHED })
        assertFalse(requireNotNull(store.batch(batch.batchId)).bulkSlotReserved)
    }

    @Test
    fun `a publication after grouped metadata loading forces a fresh read after the admission CAS loses`() {
        val seed = "https://publication-race.example/"
        val original = canonical(seed)
        val batch = service.submit(List(12) { seed }, false)
        var publishedDuringRead = false
        val adapter = AdmissionStore(
            proxy { method, _, delegate ->
                val response = delegate()
                if (method == "batchGetItem" && !publishedDuringRead) {
                    publishedDuringRead = true
                    val forced = submitAndProcess(service, store, clock, listOf(seed), true)
                    val runId = requireNotNull(store.rows(forced.batchId, true).single().runId)
                    val run = requireNotNull(execution.claim(requireNotNull(store.run(runId))))
                    publish(run, result(run, label = "new publication"))
                }
                response
            },
            store.configuration,
        )

        processor(adapter).use { it.process(batch.batchId) }
        val accepted = requireNotNull(store.meta(original.identityId))
        val rows = store.rows(batch.batchId, true)

        assertTrue(publishedDuringRead)
        assertNotEquals(original.currentArtifactHash, accepted.currentArtifactHash)
        assertEquals(12, rows.size)
        assertTrue(rows.all { it.admission == Admission.CACHED })
        assertTrue(rows.all { it.outputHash == accepted.currentArtifactHash && it.versionSequence == accepted.currentVersionSequence })
        assertEquals(Capacity(0, 1), store.capacity())
    }

    @Test
    fun `a durable position winner created after grouped reads is reused without another reservation`() {
        val seed = "https://position-race.example/"
        seedCachedMetadata(seed)
        val batch = service.submit(List(12) { seed }, true)
        var competingDecision: BatchItem? = null
        val adapter = AdmissionStore(
            proxy { method, _, delegate ->
                val response = delegate()
                if (method == "batchGetItem" && competingDecision == null) {
                    competingDecision = service.admitPending(batch, 0, seed)
                }
                response
            },
            store.configuration,
        )

        processor(adapter).use { it.process(batch.batchId) }
        val rows = store.rows(batch.batchId, true)

        assertNotNull(competingDecision)
        assertEquals(competingDecision, rows.first())
        assertEquals(1, rows.count { it.admission == Admission.QUEUED })
        assertEquals(11, rows.count { it.admission == Admission.COALESCED })
        assertEquals(1, rows.map { it.runId }.distinct().size)
        assertEquals(Capacity(1, 0), store.capacity())
    }

    @Test
    fun `a run completed after grouped run loading is reevaluated as a fresh result`() {
        val active = queued("https://active-race.example/")
        val batch = service.submit(List(12) { active.normalizedSeedUrl }, false)
        var completedDuringRead = false
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                val response = delegate()
                if (method == "batchGetItem" && !completedDuringRead) {
                    val request = arguments[0] as BatchGetItemRequest
                    val hasActiveRun = request
                        .requestItems()
                        .getValue(store.table)
                        .keys()
                        .any { it["pk"]?.s() == "RUN#${active.runId}" }
                    if (hasActiveRun) {
                        completedDuringRead = true
                        publish(requireNotNull(execution.claim(active)))
                    }
                }
                response
            },
            store.configuration,
        )

        processor(adapter).use { it.process(batch.batchId) }
        val rows = store.rows(batch.batchId, true)

        assertTrue(completedDuringRead)
        assertEquals(12, rows.size)
        assertTrue(rows.all { it.admission == Admission.CACHED && it.runId == null })
        assertEquals(Capacity(0, 1), store.capacity())
    }

    @Test
    fun `grouped metadata reads split at the DynamoDB item limit and omit absent identities`() {
        val inputs = (0 until 250).map { "https://group-limit-$it.example/" }
        inputs.forEach(::seedCachedMetadata)
        val identities = UrlIdentityResolver(settings).let { resolver -> inputs.map { resolver.normalize(it).identityId }.toSet() }
        val requests = mutableListOf<BatchGetItemRequest>()
        val adapter = AdmissionStore(
            proxy { method, arguments, delegate ->
                if (method == "batchGetItem") requests += arguments[0] as BatchGetItemRequest
                delegate()
            },
            store.configuration,
        )

        val metadata = adapter.metas(identities + "absent-identity")

        assertEquals(identities, metadata.keys)
        assertEquals(
            listOf(100, 100, 51),
            requests.map {
                it
                    .requestItems()
                    .getValue(store.table)
                    .keys()
                    .size
            },
        )
        assertTrue(requests.all { it.requestItems().getValue(store.table).consistentRead() })
    }

    private fun processor(adapter: AdmissionStore): BulkBatchProcessor {
        val admission = AdmissionService(adapter, settings, UrlIdentityResolver(settings), clock)
        return BulkBatchProcessor(adapter, BulkBatchStore(adapter, clock), admission, clock)
    }

    private fun seedCachedMetadata(seed: String) {
        val identity = UrlIdentityResolver(settings).normalize(seed)
        store.putAbsent(
            CrawlMetaCodec.encode(
                CrawlMeta(
                    identityId = identity.identityId,
                    normalizedSeedUrl = identity.normalizedSeedUrl,
                    intentProfile = identity.intentProfile,
                    requiredPipelineVersion = settings.pipelineVersion,
                    acceptedPipelineVersion = settings.pipelineVersion,
                    acceptedObservationSequence = 1,
                    currentVersionSequence = 1,
                    currentArtifactHash = "cached-fixture-hash",
                    acceptedObservationStartedAt = clock.now,
                    monitoringState = MonitoringState.EXPIRED,
                ),
            ),
        )
    }
}
