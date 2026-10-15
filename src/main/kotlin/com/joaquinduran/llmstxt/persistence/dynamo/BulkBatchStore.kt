package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.api.errors.ApiProblem
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.Batch
import jakarta.inject.Singleton
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Clock

/** Durable receipt is separate from crawl admission; no source work starts before the receipt is sealed. */
@Singleton
class BulkBatchStore(
    private val store: AdmissionStore,
    private val clock: Clock,
    private val configuration: AdmissionConfiguration = AdmissionConfiguration(),
) {
    data class DiscoveryPage(
        val batchIds: List<String>,
        val continuation: Item,
    )

    companion object {
        const val INPUTS_PER_CHUNK = 50

        // One transaction item is reserved for the durable progress checkpoint.
        private const val RETENTION_CHILDREN_PER_TRANSACTION = 99
        private val capacityKey = DomainKeys.key("CAPACITY#BULK", "META")
    }

    private val table get() = store.table

    fun receive(
        batch: Batch,
        maxOpenBatches: Int,
    ): Batch {
        validateCapacity()
        reserveReceipt(batch, maxOpenBatches)
        persistInputs(batch)
        sealReceipt(batch)
        return batch.copy(inputs = emptyList(), received = true)
    }

    private fun validateCapacity() {
        val existingCapacity = store.get(capacityKey)
        if (existingCapacity.isNotEmpty()) {
            try {
                existingCapacity.check("BulkCapacity")
                require(existingCapacity.n("activeBatches") >= 0)
            } catch (_: IllegalArgumentException) {
                throw StorageUnavailable()
            }
        }
    }

    private fun reserveReceipt(
        batch: Batch,
        maxOpenBatches: Int,
    ) {
        val header = BatchCodec.encode(batch) + workKeys(batch, requireNotNull(batch.receiptDeadlineAt))
        val reserve = Update
            .builder()
            .tableName(table)
            .key(capacityKey)
            .updateExpression("SET entityType = :type, itemSchemaVersion = :schema ADD activeBatches :one")
            .conditionExpression(
                "(attribute_not_exists(pk) OR (entityType = :type AND itemSchemaVersion = :schema)) " +
                    "AND (attribute_not_exists(activeBatches) OR activeBatches < :max)",
            ).expressionAttributeValues(
                mapOf(":type" to text("BulkCapacity"), ":schema" to number(1), ":one" to number(1), ":max" to number(maxOpenBatches.toLong())),
            ).build()
        val created = store.transact(
            listOf(put(header), TransactWriteItem.builder().update(reserve).build()),
        ) { store.batch(batch.batchId) != null }
        if (!created) throw ApiProblem(429, "BACKLOG_FULL", "The durable batch backlog is full. Please retry later.", 5)
    }

    private fun persistInputs(batch: Batch) {
        for ((chunkIndex, inputs) in batch.inputs.chunked(INPUTS_PER_CHUNK).withIndex()) {
            val chunk = base("BATCH#${batch.batchId}", chunkKey(chunkIndex), "BatchInputChunk") + mapOf("inputs" to strings(inputs))
            val liveReceipt = ConditionCheck
                .builder()
                .tableName(table)
                .key(DomainKeys.batch(batch.batchId))
                .conditionExpression("received = :no AND bulkSlotReserved = :yes AND nextCompletionIndex = :zero AND receiptDeadlineAt > :now")
                .expressionAttributeValues(
                    mapOf(":no" to bool(false), ":yes" to bool(true), ":zero" to number(0), ":now" to number(clock.millis())),
                ).build()
            val stored = store.transact(
                listOf(put(chunk), TransactWriteItem.builder().conditionCheck(liveReceipt).build()),
                beforeAttempt = { clock.millis() < requireNotNull(batch.receiptDeadlineAt) },
            ) {
                store.get(DomainKeys.key("BATCH#${batch.batchId}", chunkKey(chunkIndex)))["inputs"] == chunk["inputs"]
            }
            if (!stored) throw StorageUnavailable()
        }
    }

    private fun sealReceipt(batch: Batch) {
        val now = clock.millis()
        val seal = Update
            .builder()
            .tableName(table)
            .key(DomainKeys.batch(batch.batchId))
            .updateExpression("SET received = :yes, workSk = :work")
            .conditionExpression("received = :no AND bulkSlotReserved = :yes AND nextCompletionIndex = :zero AND receiptDeadlineAt > :now")
            .expressionAttributeValues(
                mapOf(
                    ":yes" to bool(true),
                    ":no" to bool(false),
                    ":zero" to number(0),
                    ":now" to number(now),
                    ":work" to text("${DomainKeys.ordered(now)}#${batch.batchId}"),
                ),
            ).build()
        val sealed = store.transact(
            listOf(TransactWriteItem.builder().update(seal).build()),
            beforeAttempt = { clock.millis() < requireNotNull(batch.receiptDeadlineAt) },
        ) { store.batch(batch.batchId)?.received == true }
        if (!sealed) throw StorageUnavailable()
    }

    fun inputs(
        batch: Batch,
        offset: Int,
        limit: Int,
    ): List<String> {
        if (!batch.bulk) return batch.inputs.drop(offset).take(limit)
        val end = minOf(batch.totalInputs.toLong(), offset.toLong() + limit).toInt()
        if (offset >= end) return emptyList()
        val inputs = mutableListOf<String>()
        for (chunkIndex in offset / INPUTS_PER_CHUNK..(end - 1) / INPUTS_PER_CHUNK) {
            val chunk = store.get(DomainKeys.key("BATCH#${batch.batchId}", chunkKey(chunkIndex)))
            if (chunk.isEmpty()) throw StorageUnavailable()
            chunk.check("BatchInputChunk")
            val values = chunk.list("inputs")
            val firstIndex = maxOf(offset, chunkIndex * INPUTS_PER_CHUNK)
            val lastIndex = minOf(end, (chunkIndex + 1) * INPUTS_PER_CHUNK)
            inputs.addAll(values.subList(firstIndex % INPUTS_PER_CHUNK, lastIndex - chunkIndex * INPUTS_PER_CHUNK))
        }
        return inputs
    }

    fun advanceAdmission(
        batch: Batch,
        nextIndex: Int,
    ): Boolean {
        require(nextIndex in batch.nextAdmissionIndex + 1..batch.totalInputs)
        val update = Update
            .builder()
            .tableName(table)
            .key(DomainKeys.batch(batch.batchId))
            .updateExpression("SET nextAdmissionIndex = :next, admissionComplete = :complete")
            .conditionExpression("received = :yes AND bulkSlotReserved = :yes AND nextAdmissionIndex = :previous")
            .expressionAttributeValues(
                mapOf(
                    ":yes" to bool(true),
                    ":next" to number(nextIndex.toLong()),
                    ":previous" to number(batch.nextAdmissionIndex.toLong()),
                    ":complete" to bool(nextIndex == batch.totalInputs),
                ),
            ).build()
        return store.transact(listOf(TransactWriteItem.builder().update(update).build())) {
            (store.batch(batch.batchId)?.nextAdmissionIndex ?: 0) >= nextIndex
        }
    }

    /** Only a contiguous terminal prefix advances; no run-to-batch fan-out or per-row completion counters are needed. */
    fun advanceCompletion(
        batch: Batch,
        nextIndex: Int,
    ): Boolean {
        require(nextIndex in batch.nextCompletionIndex + 1..batch.totalInputs)
        require(nextIndex <= batch.nextAdmissionIndex)
        if (nextIndex == batch.totalInputs) return startRetention(batch)
        val update = Update
            .builder()
            .tableName(table)
            .key(DomainKeys.batch(batch.batchId))
            .updateExpression("SET nextCompletionIndex = :next")
            .conditionExpression("bulkSlotReserved = :yes AND nextCompletionIndex = :previous")
            .expressionAttributeValues(
                mapOf(":yes" to bool(true), ":previous" to number(batch.nextCompletionIndex.toLong()), ":next" to number(nextIndex.toLong())),
            ).build()
        return store.transact(listOf(TransactWriteItem.builder().update(update).build())) {
            (store.batch(batch.batchId)?.nextCompletionIndex ?: 0) >= nextIndex
        }
    }

    /** Only an unacknowledged upload may expire; accepted inputs remain durable while waiting. */
    fun expireReceipt(batch: Batch): Boolean {
        if (batch.received || clock.millis() < requireNotNull(batch.receiptDeadlineAt)) return false
        if (batch.nextCompletionIndex != batch.totalInputs && !startRetention(batch)) return false
        finalizeRetention(requireNotNull(store.batch(batch.batchId)))
        return true
    }

    private fun startRetention(batch: Batch): Boolean {
        val expiry = clock.millis() / 1000 + configuration.batchRetentionDays * 86400
        val update = Update
            .builder()
            .tableName(table)
            .key(DomainKeys.batch(batch.batchId))
            .updateExpression("SET nextCompletionIndex = :total, retentionExpiresAtEpochSeconds = :ttl")
            // Seal and abandoned-upload cleanup must not both win from the same unsealed receipt.
            .conditionExpression("bulkSlotReserved = :yes AND received = :received AND nextCompletionIndex = :previous")
            .expressionAttributeValues(
                mapOf(
                    ":total" to number(batch.totalInputs.toLong()),
                    ":ttl" to number(expiry),
                    ":yes" to bool(true),
                    ":received" to bool(batch.received),
                    ":previous" to number(batch.nextCompletionIndex.toLong()),
                ),
            ).build()
        return store.transact(listOf(TransactWriteItem.builder().update(update).build())) {
            store.batch(batch.batchId)?.nextCompletionIndex == batch.totalInputs
        }
    }

    /** Attach retention in bounded transactions before releasing the durable receipt witness and slot. */
    fun finalizeRetention(batch: Batch) {
        if (!batch.bulkSlotReserved || batch.nextCompletionIndex != batch.totalInputs) return
        val chunkCount = (batch.totalInputs + INPUTS_PER_CHUNK - 1) / INPUTS_PER_CHUNK
        val childCount = chunkCount + if (batch.received) batch.totalInputs else 0
        val end = minOf(childCount, batch.nextRetentionIndex + RETENTION_CHILDREN_PER_TRANSACTION)
        if (batch.nextRetentionIndex < end) {
            val operations = mutableListOf<TransactWriteItem>()
            for (index in batch.nextRetentionIndex until end) {
                val key = if (index < chunkCount) {
                    DomainKeys.key("BATCH#${batch.batchId}", chunkKey(index))
                } else {
                    DomainKeys.batchItem(batch.batchId, index - chunkCount)
                }
                // A partial upload has absent chunks. Already-expired retained children may have been deleted by TTL.
                if ((!batch.received || batch.expiresAtEpochSeconds <= clock.millis() / 1000) && store.get(key).isEmpty()) continue
                operations.add(
                    TransactWriteItem
                        .builder()
                        .update(
                            Update
                                .builder()
                                .tableName(table)
                                .key(key)
                                .updateExpression("SET expiresAtEpochSeconds = :ttl")
                                .conditionExpression("attribute_exists(pk)")
                                .expressionAttributeValues(mapOf(":ttl" to number(batch.expiresAtEpochSeconds)))
                                .build(),
                        ).build(),
                )
            }
            val checkpoint = Update
                .builder()
                .tableName(table)
                .key(DomainKeys.batch(batch.batchId))
                .updateExpression("SET nextRetentionIndex = :next")
                .conditionExpression(
                    "bulkSlotReserved = :yes AND nextCompletionIndex = :total AND " +
                        if (batch.nextRetentionIndex == 0) {
                            "(attribute_not_exists(nextRetentionIndex) OR nextRetentionIndex = :previous)"
                        } else {
                            "nextRetentionIndex = :previous"
                        },
                ).expressionAttributeValues(
                    mapOf(
                        ":yes" to bool(true),
                        ":total" to number(batch.totalInputs.toLong()),
                        ":previous" to number(batch.nextRetentionIndex.toLong()),
                        ":next" to number(end.toLong()),
                    ),
                ).build()
            operations.add(TransactWriteItem.builder().update(checkpoint).build())
            if (!store.transact(operations) { (store.batch(batch.batchId)?.nextRetentionIndex ?: 0) >= end }) return
        }
        val current = store.batch(batch.batchId) ?: throw StorageUnavailable()
        if (current.nextRetentionIndex >= childCount) finish(current)
    }

    private fun finish(batch: Batch): Boolean {
        val terminal = Update
            .builder()
            .tableName(table)
            .key(DomainKeys.batch(batch.batchId))
            .updateExpression("SET bulkSlotReserved = :no, nextCompletionIndex = :total, expiresAtEpochSeconds = :ttl REMOVE workPk, workSk")
            .conditionExpression("bulkSlotReserved = :yes AND nextCompletionIndex = :previous AND nextRetentionIndex = :children")
            .expressionAttributeValues(
                mapOf(
                    ":children" to
                        number(
                            ((batch.totalInputs + INPUTS_PER_CHUNK - 1) / INPUTS_PER_CHUNK + if (batch.received) batch.totalInputs else 0).toLong(),
                        ),
                    ":no" to bool(false),
                    ":yes" to bool(true),
                    ":total" to number(batch.totalInputs.toLong()),
                    ":previous" to number(batch.nextCompletionIndex.toLong()),
                    ":ttl" to number(batch.expiresAtEpochSeconds),
                ),
            ).build()
        val release = Update
            .builder()
            .tableName(table)
            .key(capacityKey)
            .updateExpression("ADD activeBatches :minus")
            .conditionExpression("activeBatches >= :one")
            .expressionAttributeValues(mapOf(":minus" to number(-1), ":one" to number(1)))
            .build()
        return store.transact(
            listOf(TransactWriteItem.builder().update(terminal).build(), TransactWriteItem.builder().update(release).build()),
        ) { store.batch(batch.batchId)?.bulkSlotReserved == false }
    }

    fun due(
        shard: Int,
        now: Long,
        limit: Int,
        continuation: Item,
    ): DiscoveryPage {
        val request = QueryRequest
            .builder()
            .tableName(table)
            .indexName("WorkIndex")
            .keyConditionExpression("workPk = :pk AND workSk <= :now")
            .expressionAttributeValues(mapOf(":pk" to text("WORK#BATCH#$shard"), ":now" to text("${DomainKeys.ordered(now)}#~")))
            .limit(limit)
        if (continuation.isNotEmpty()) request.exclusiveStartKey(continuation)
        val response = store.client.query(request.build())
        return DiscoveryPage(response.items().map { it.s("pk").removePrefix("BATCH#") }, response.lastEvaluatedKey())
    }

    private fun workKeys(
        batch: Batch,
        availableAt: Long,
    ): Item {
        val shard = DomainKeys.shard(batch.batchId, store.configuration.workIndexShards)
        return mapOf("workPk" to text("WORK#BATCH#$shard"), "workSk" to text("${DomainKeys.ordered(availableAt)}#${batch.batchId}"))
    }

    private fun chunkKey(index: Int) = "INPUT#${DomainKeys.ordered(index.toLong())}"

    private fun put(item: Item): TransactWriteItem = TransactWriteItem
        .builder()
        .put(
            Put
                .builder()
                .tableName(table)
                .item(item)
                .conditionExpression("attribute_not_exists(sk)")
                .build(),
        ).build()
}
