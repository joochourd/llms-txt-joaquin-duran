package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.Admission
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.Batch
import com.joaquinduran.llmstxt.ingestion.BatchItem
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.CrawlIdentity
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.RunStatus
import jakarta.inject.Singleton
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

class StorageUnavailable : RuntimeException("Storage is temporarily unavailable. Some admission decisions may already be durable.")

@Singleton
class AdmissionStore(
    val client: DynamoDbClient,
    val configuration: DynamoConfiguration,
) {
    private companion object {
        const val MAX_BATCH_READ_ATTEMPTS = 5
        const val MAX_BATCH_READ_ITEMS = 100
        const val MAX_TRANSACTION_ATTEMPTS = 4
        const val MIN_RETRY_DELAY_MILLIS = 5L
        const val RETRY_DELAY_STEP_MILLIS = 15L
    }

    val table get() = configuration.table

    fun get(
        key: Item,
        strong: Boolean = true,
    ): Item = client
        .getItem(
            GetItemRequest
                .builder()
                .tableName(table)
                .key(key)
                .consistentRead(strong)
                .build(),
        ).item()

    fun meta(id: String): CrawlMeta? = get(DomainKeys.crawl(id)).takeIf { it.isNotEmpty() }?.let(CrawlMetaCodec::decode)

    fun run(
        id: String,
        strong: Boolean = true,
    ): CrawlRun? = get(DomainKeys.run(id), strong).takeIf { it.isNotEmpty() }?.let(RunCodec::decode)

    fun batch(
        id: String,
        strong: Boolean = true,
    ): Batch? = get(DomainKeys.batch(id), strong).takeIf { it.isNotEmpty() }?.let(BatchCodec::decode)

    fun row(
        batch: String,
        index: Int,
    ): BatchItem? = get(DomainKeys.batchItem(batch, index)).takeIf { it.isNotEmpty() }?.let(BatchItemCodec::decode)

    fun capacity(): Capacity = try {
        CapacityCodec.decode(get(DynamoSchema.capacityKey))
    } catch (_: IllegalArgumentException) {
        throw StorageUnavailable()
    }

    fun putAbsent(
        item: Item,
        condition: String = "attribute_not_exists(pk)",
    ): Boolean = try {
        client.putItem(
            PutItemRequest
                .builder()
                .tableName(table)
                .item(item)
                .conditionExpression(condition)
                .build(),
        )
        true
    } catch (_: ConditionalCheckFailedException) {
        false
    }

    fun saveBatch(batch: Batch) {
        putAbsent(BatchCodec.encode(batch))
    }

    fun ensureIdentity(
        identity: CrawlIdentity,
        pipelineVersion: String,
    ) {
        putAbsent(
            CrawlMetaCodec.encode(
                CrawlMeta(
                    identity.identityId,
                    identity.normalizedSeedUrl,
                    identity.intentProfile,
                    pipelineVersion,
                ),
            ),
        )
    }

    /** Prepare the current pipeline without touching accepted output or lifecycle counters. */
    fun preparePipeline(
        meta: CrawlMeta,
        pipelineVersion: String,
    ): Boolean {
        val update = Update
            .builder()
            .tableName(table)
            .key(DomainKeys.crawl(meta.identityId))
            .updateExpression("SET requiredPipelineVersion = :target ADD metadataRevision :one")
            .conditionExpression(metaCondition(meta).conditionExpression())
            .expressionAttributeValues(metaValues(meta) + mapOf(":target" to text(pipelineVersion), ":one" to number(1)))
            .build()
        return transact(listOf(TransactWriteItem.builder().update(update).build())) {
            this.meta(meta.identityId)?.requiredPipelineVersion == pipelineVersion
        }
    }

    fun blockedUntil(origin: String): Long = get(DomainKeys.origin(origin)).ns("blockedUntil") ?: 0

    fun recordDecision(
        decision: BatchItem,
        meta: CrawlMeta,
        batch: Batch? = null,
    ): BatchItem? = commitDecision(
        decision,
        listOf(rowPut(decision), TransactWriteItem.builder().conditionCheck(metaCondition(meta)).build()),
        batch,
    )

    fun recordCoalescedDecision(
        decision: BatchItem,
        meta: CrawlMeta,
        active: CrawlRun,
        pipelineVersion: String,
        now: Long,
        batch: Batch? = null,
    ): BatchItem? {
        val values = mutableMapOf(
            ":status" to text(active.status.name),
            ":pipeline" to text(pipelineVersion),
        )
        val runningDeadline = if (active.status == RunStatus.RUNNING) {
            values[":now"] = number(now)
            " AND deadlineAt > :now"
        } else {
            ""
        }
        val condition = ConditionCheck
            .builder()
            .tableName(table)
            .key(DomainKeys.run(active.runId))
            .conditionExpression("#status = :status AND pipelineVersion = :pipeline$runningDeadline")
            .expressionAttributeNames(mapOf("#status" to "status"))
            .expressionAttributeValues(values)
            .build()
        return commitDecision(
            decision,
            listOf(
                rowPut(decision),
                TransactWriteItem.builder().conditionCheck(metaCondition(meta)).build(),
                TransactWriteItem.builder().conditionCheck(condition).build(),
            ),
            batch,
        )
    }

    fun recordRejection(
        decision: BatchItem,
        batch: Batch? = null,
    ): BatchItem {
        if (batch?.bulk == true) return commitDecision(decision, listOf(rowPut(decision)), batch) ?: throw StorageUnavailable()
        putAbsent(BatchItemCodec.encode(decision), "attribute_not_exists(sk)")
        return row(decision.batchId, decision.index) ?: throw StorageUnavailable()
    }

    /** One transaction reserves capacity and creates META hints, RUN and the immutable decision. */
    fun createRun(
        batch: Batch,
        index: Int,
        identity: CrawlIdentity,
        meta: CrawlMeta,
        admission: AdmissionConfiguration,
        now: Long,
        monitoringDelta: Long,
        input: String = batch.inputs[index],
    ): BatchItem? {
        val runId = UUID.randomUUID().toString()
        val reservationCheckAt = now + (admission.maxCrawlDurationSeconds + admission.runRecoveryGraceSeconds) * 1000
        val shard = DomainKeys.shard(runId, configuration.workIndexShards)
        val run = CrawlRun(
            runId = runId,
            identityId = identity.identityId,
            normalizedSeedUrl = identity.normalizedSeedUrl,
            pipelineVersion = admission.pipelineVersion,
            originHint = meta.effectiveOrigin ?: identity.origin,
            queuedAt = now,
            queueDeadlineAt = null,
            availableAt = now,
            workShard = shard,
            workPk = "WORK#QUEUED#$shard",
            workSk = "${DomainKeys.ordered(now)}#$runId",
            budgets = admission.budgets(),
        )
        val decision = BatchItem(
            batch.batchId,
            index,
            input,
            Admission.QUEUED,
            identity.identityId,
            runId,
            expiresAtEpochSeconds = if (batch.bulk) null else batch.expiresAtEpochSeconds,
        )
        val metadata = runAdmissionUpdate(meta, run, reservationCheckAt, monitoringDelta)
        val capacity = capacityReservation(admission, monitoringDelta)
        val runPut = Put
            .builder()
            .tableName(table)
            .item(RunCodec.encode(run))
            .conditionExpression("attribute_not_exists(pk)")
            .build()
        return commitDecision(
            decision,
            listOf(
                TransactWriteItem.builder().update(metadata).build(),
                TransactWriteItem.builder().put(runPut).build(),
                rowPut(decision),
                TransactWriteItem.builder().update(capacity).build(),
            ),
            batch,
        )
    }

    private fun runAdmissionUpdate(
        meta: CrawlMeta,
        run: CrawlRun,
        reservationCheckAt: Long,
        monitoringDelta: Long,
    ): Update {
        val metaValues = metaValues(meta).toMutableMap().apply {
            put(":one", number(1))
            put(":run", text(run.runId))
        }
        val set = mutableListOf("activeRunIdHint = :run")
        if (monitoringDelta == 1L || meta.monitoringState == MonitoringState.RESERVED) {
            val reservationExpiry = maxOf(meta.monitoringReservationExpiresAt ?: 0, reservationCheckAt)
            val dueShard = meta.dueShard ?: DomainKeys.shard(run.identityId, configuration.dueIndexShards)
            val nextAction = reservationExpiry
            set.addAll(
                listOf(
                    "monitoringState = :reserved",
                    "monitoringSlotReserved = :yes",
                    "monitoringReservationExpiresAt = :reservation",
                    "dueShard = :dueShard",
                    "duePk = :duePk",
                    "dueSk = :dueSk",
                ),
            )
            metaValues.putAll(
                mapOf(
                    ":reserved" to text("RESERVED"),
                    ":yes" to bool(true),
                    ":reservation" to number(reservationExpiry),
                    ":dueShard" to number(dueShard.toLong()),
                    ":duePk" to text("DUE#$dueShard"),
                    ":dueSk" to text("${DomainKeys.ordered(nextAction)}#${run.identityId}"),
                ),
            )
        }
        return Update
            .builder()
            .tableName(table)
            .key(DomainKeys.crawl(run.identityId))
            .updateExpression("SET ${set.joinToString(", ")} REMOVE activeHintExpiresAt ADD metadataRevision :one, outstandingRunCount :one")
            .conditionExpression(metaCondition(meta).conditionExpression())
            .expressionAttributeValues(metaValues)
            .build()
    }

    private fun capacityReservation(
        admission: AdmissionConfiguration,
        monitoringDelta: Long,
    ): Update = Update
        .builder()
        .tableName(table)
        .key(DynamoSchema.capacityKey)
        .updateExpression("ADD outstandingRuns :one, monitoredIdentities :delta")
        .conditionExpression("attribute_exists(pk) AND outstandingRuns < :maxRuns AND (:delta = :zero OR monitoredIdentities < :maxMonitored)")
        .expressionAttributeValues(
            mapOf(
                ":one" to number(1),
                ":delta" to number(monitoringDelta),
                ":zero" to number(0),
                ":maxRuns" to number(admission.maxOutstandingRuns),
                ":maxMonitored" to number(admission.maxMonitoredIdentities),
            ),
        ).build()

    /** Null requests CAS reevaluation; ambiguous retries stay inside transact with one token. */
    private fun commitDecision(
        decision: BatchItem,
        items: List<TransactWriteItem>,
        batch: Batch? = null,
    ): BatchItem? {
        val operations = if (batch?.bulk == true) items + resolvedInputUpdate(batch) else items
        if (!transact(operations) { row(decision.batchId, decision.index) != null }) return null
        // Success or a commit witness guarantees a durable winner, which may be deadline repair.
        return row(decision.batchId, decision.index)!!
    }

    /** The immutable decision and its progress count commit together, including cache and rejection decisions. */
    private fun resolvedInputUpdate(batch: Batch): TransactWriteItem = TransactWriteItem
        .builder()
        .update(
            Update
                .builder()
                .tableName(table)
                .key(DomainKeys.batch(batch.batchId))
                .updateExpression("SET resolvedInputs = if_not_exists(resolvedInputs, :known) + :one")
                .conditionExpression("received = :yes AND attribute_exists(workPk)")
                .expressionAttributeValues(mapOf(":known" to number(batch.resolvedInputs.toLong()), ":one" to number(1), ":yes" to bool(true)))
                .build(),
        ).build()

    fun rows(
        id: String,
        strong: Boolean,
    ): List<BatchItem> {
        val rows = mutableListOf<BatchItem>()
        var last: Item = emptyMap()
        do {
            val request = QueryRequest
                .builder()
                .tableName(table)
                .consistentRead(strong)
                .keyConditionExpression("pk = :pk AND begins_with(sk, :prefix)")
                .expressionAttributeValues(mapOf(":pk" to text("BATCH#$id"), ":prefix" to text("ITEM#")))
            if (last.isNotEmpty()) request.exclusiveStartKey(last)
            val result = client.query(request.build())
            rows.addAll(result.items().map(BatchItemCodec::decode))
            last = result.lastEvaluatedKey()
        } while (last.isNotEmpty())
        return rows
    }

    fun rowsPage(
        id: String,
        offset: Int,
        limit: Int,
        strong: Boolean = true,
    ): List<BatchItem> {
        require(offset >= 0 && limit in 1..100)
        val request = QueryRequest
            .builder()
            .tableName(table)
            .consistentRead(strong)
            .keyConditionExpression("pk = :pk AND sk BETWEEN :first AND :last")
            .expressionAttributeValues(
                mapOf(
                    ":pk" to text("BATCH#$id"),
                    ":first" to text("ITEM#${DomainKeys.ordered(offset.toLong())}"),
                    ":last" to text("ITEM#${DomainKeys.ordered(offset.toLong() + limit - 1)}"),
                ),
            ).limit(limit)
            .build()
        return client.query(request).items().map(BatchItemCodec::decode)
    }

    fun metas(ids: Set<String>): Map<String, CrawlMeta> = readItems(ids.map(DomainKeys::crawl), strong = true)
        .map(CrawlMetaCodec::decode)
        .associateBy { it.identityId }

    fun runs(
        ids: Set<String>,
        strong: Boolean,
    ): Map<String, CrawlRun> = readItems(ids.map(DomainKeys::run), strong)
        .map(RunCodec::decode)
        .associateBy { it.runId }

    private fun readItems(
        keys: List<Item>,
        strong: Boolean,
    ): List<Item> = keys.chunked(MAX_BATCH_READ_ITEMS).flatMap { readChunk(it, strong) }

    private fun readChunk(
        keys: List<Item>,
        strong: Boolean,
    ): List<Item> {
        var pending = mapOf(
            table to KeysAndAttributes
                .builder()
                .keys(keys)
                .consistentRead(strong)
                .build(),
        )
        val found = mutableListOf<Item>()
        repeat(MAX_BATCH_READ_ATTEMPTS) { attempt ->
            val response = client.batchGetItem(BatchGetItemRequest.builder().requestItems(pending).build())
            found += response.responses()[table].orEmpty()
            pending = response.unprocessedKeys()
            if (pending.isEmpty()) return found
            pause(attempt)
        }
        throw StorageUnavailable()
    }

    fun metaCondition(meta: CrawlMeta): ConditionCheck = ConditionCheck
        .builder()
        .tableName(table)
        .key(DomainKeys.crawl(meta.identityId))
        .conditionExpression("metadataRevision = :revision AND acceptedObservationSequence = :accepted AND requiredPipelineVersion = :pipeline")
        .expressionAttributeValues(metaValues(meta))
        .build()

    fun metaValues(meta: CrawlMeta): Item = mapOf(
        ":revision" to number(meta.metadataRevision),
        ":accepted" to number(meta.acceptedObservationSequence),
        ":pipeline" to text(meta.requiredPipelineVersion),
    )

    fun rowPut(row: BatchItem): TransactWriteItem = TransactWriteItem
        .builder()
        .put(
            Put
                .builder()
                .tableName(table)
                .item(BatchItemCodec.encode(row))
                .conditionExpression("attribute_not_exists(sk)")
                .build(),
        ).build()

    // An unchanged request keeps its token across ambiguous retries. A reevaluated CAS uses a new token.
    // A durable BatchItem is the commit witness for the entire admission transaction.
    fun transact(
        items: List<TransactWriteItem>,
        beforeAttempt: () -> Boolean = { true },
        committed: () -> Boolean,
    ): Boolean {
        val request = TransactWriteItemsRequest
            .builder()
            .clientRequestToken(UUID.randomUUID().toString())
            .transactItems(items)
            .build()
        repeat(MAX_TRANSACTION_ATTEMPTS) { attempt ->
            if (!beforeAttempt()) return committed()
            try {
                client.transactWriteItems(request)
                return true
            } catch (exception: TransactionCanceledException) {
                if (committed()) return true
                val codes = exception.cancellationReasons().mapNotNull { it.code() }.filter { it != "None" }
                if ("ConditionalCheckFailed" in codes) return false
                if (codes.isEmpty() || codes.any { it !in setOf("TransactionConflict", "ProvisionedThroughputExceeded", "ThrottlingError") }) {
                    throw StorageUnavailable()
                }
            } catch (exception: SdkException) {
                if (committed()) return true
                // Client timeouts and server failures may have committed; retry exactly the same request.
                if (exception is DynamoDbException && exception.statusCode() in 400..499 &&
                    exception !is TransactionInProgressException && exception !is TransactionConflictException &&
                    exception !is ProvisionedThroughputExceededException && exception !is RequestLimitExceededException
                ) {
                    throw StorageUnavailable()
                }
            }
            pause(attempt)
        }
        throw StorageUnavailable()
    }

    fun closeBatch(batch: Batch) {
        // All positions must exist before admissionComplete is set. Rows are immutable PutIfAbsent.
        check(rows(batch.batchId, true).map { it.index }.toSet() == batch.inputs.indices.toSet())
        client.updateItem(
            UpdateItemRequest
                .builder()
                .tableName(table)
                .key(DomainKeys.batch(batch.batchId))
                .updateExpression("SET admissionComplete = :yes")
                .conditionExpression("attribute_exists(pk)")
                .expressionAttributeValues(mapOf(":yes" to bool(true)))
                .build(),
        )
    }

    private fun pause(attempt: Int) =
        Thread.sleep(ThreadLocalRandom.current().nextLong(MIN_RETRY_DELAY_MILLIS, RETRY_DELAY_STEP_MILLIS * (attempt + 1) + 1))
}
