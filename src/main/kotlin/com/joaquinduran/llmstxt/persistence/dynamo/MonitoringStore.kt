package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.RunStatus
import jakarta.inject.Singleton
import software.amazon.awssdk.services.dynamodb.model.*
import java.net.URI
import java.util.UUID

enum class MonitoringControlWrite { COMMITTED, CONFLICT, CAPACITY_UNAVAILABLE }

data class DuePage(val identities: List<String>, val continuation: Item)

/** Every transition keeps its original guards and commit witness in one atomic transaction. */
@Singleton
class MonitoringStore(
    private val store: AdmissionStore,
    private val admission: AdmissionConfiguration,
) {
    fun due(
        shard: Int,
        now: Long,
        limit: Int,
        continuation: Item,
    ): DuePage {
        val request = QueryRequest
            .builder()
            .tableName(store.table)
            .indexName("DueIndex")
            .keyConditionExpression("duePk = :pk AND dueSk <= :now")
            .expressionAttributeValues(
                mapOf(
                    ":pk" to text("DUE#" + shard),
                    ":now" to text(DomainKeys.ordered(now) + "#~"),
                ),
            ).limit(limit)
        if (continuation.isNotEmpty()) request.exclusiveStartKey(continuation)
        val response = store.client.query(request.build())
        val ids = response.items().map { it.s("pk").removePrefix("CRAWL#") }
        return DuePage(ids, response.lastEvaluatedKey())
    }

    /** Older rows may still carry an index hint for removed historical cleanup work. */
    fun reconcileSchedule(meta: CrawlMeta): CrawlMeta? {
        val set = mutableListOf<String>()
        val remove = mutableListOf<String>()
        val values = store.metaValues(meta).toMutableMap()
        values[":one"] = number(1)
        DueSchedule.append(meta, store.configuration.dueIndexShards, set, remove, values)
        val expectedShard = values[":dueShard"]?.n()?.toInt()
        val expectedPartition = values[":duePk"]?.s()
        val expectedSort = values[":dueSk"]?.s()

        fun matches(current: CrawlMeta): Boolean = current.dueShard == expectedShard &&
            current.duePk == expectedPartition && current.dueSk == expectedSort

        if (matches(meta)) return meta
        val committed = store.transact(listOf(TransactWriteItem.builder().update(update(meta, set, remove, values)).build())) {
            store.meta(meta.identityId)?.let(::matches) == true
        }
        return if (committed) store.meta(meta.identityId) else null
    }

    fun enable(
        meta: CrawlMeta,
        expiry: Long,
        nextCheck: Long,
        resetFailures: Boolean,
    ): MonitoringControlWrite = change(meta, enabled = true, expiry = expiry, nextCheck = nextCheck, resetFailures = resetFailures)

    fun disable(meta: CrawlMeta): MonitoringControlWrite = change(meta, enabled = false, expiry = null, nextCheck = null, resetFailures = false)

    private fun change(
        meta: CrawlMeta,
        enabled: Boolean,
        expiry: Long?,
        nextCheck: Long?,
        resetFailures: Boolean,
    ): MonitoringControlWrite {
        val set = mutableListOf<String>()
        val remove = mutableListOf<String>()
        val values = store.metaValues(meta).toMutableMap()
        values[":one"] = number(1)
        val nextState = if (enabled) MonitoringState.ACTIVE else MonitoringState.NONE
        val delta = if (enabled) {
            if (meta.monitoringSlotReserved) 0L else 1L
        } else {
            if (meta.monitoringSlotReserved) -1L else 0L
        }
        if (enabled) {
            set.addAll(
                listOf("monitoringState = :state", "monitoringSlotReserved = :slot", "monitoringExpiresAt = :expires", "nextCheckAt = :next"),
            )
            values[":expires"] = number(requireNotNull(expiry))
            values[":next"] = number(requireNotNull(nextCheck))
            if (resetFailures) {
                set.add("failureCount = :zero")
                values[":zero"] = number(0)
            }
        } else {
            set.addAll(listOf("monitoringState = :state", "monitoringSlotReserved = :slot"))
            remove.add("nextCheckAt")
        }
        remove.add("monitoringReservationExpiresAt")
        values[":state"] = text(nextState.name)
        values[":slot"] = bool(enabled)
        DueSchedule.append(
            meta,
            store.configuration.dueIndexShards,
            set,
            remove,
            values,
            state = nextState,
            nextCheckAt = nextCheck,
            expiresAt = expiry,
        )
        val operations = mutableListOf(TransactWriteItem.builder().update(update(meta, set, remove, values)).build())
        if (delta != 0L) operations.add(TransactWriteItem.builder().update(monitoringCapacity(delta)).build())
        if (delta > 0 && store.capacity().monitoredIdentities >= admission.maxMonitoredIdentities) {
            return MonitoringControlWrite.CAPACITY_UNAVAILABLE
        }
        val committed = store.transact(operations) {
            store.meta(meta.identityId)?.let { current ->
                current.monitoringState == nextState && current.monitoringSlotReserved == enabled &&
                    (!enabled || (current.monitoringExpiresAt ?: 0) >= requireNotNull(expiry))
            } == true
        }
        return if (committed) MonitoringControlWrite.COMMITTED else MonitoringControlWrite.CONFLICT
    }

    fun expire(
        meta: CrawlMeta,
        now: Long,
        reservation: Boolean,
    ): Boolean {
        val state = if (reservation) MonitoringState.NONE else MonitoringState.EXPIRED
        val set = mutableListOf("monitoringState = :state", "monitoringSlotReserved = :no")
        val remove = mutableListOf("nextCheckAt", "monitoringReservationExpiresAt")
        val values = store.metaValues(meta).toMutableMap()
        values[":state"] = text(state.name)
        values[":no"] = bool(false)
        values[":one"] = number(1)
        values[":now"] = number(now)
        DueSchedule.append(meta, store.configuration.dueIndexShards, set, remove, values, state = state)
        val deadlineField = if (reservation) "monitoringReservationExpiresAt" else "monitoringExpiresAt"
        var condition = " AND " + deadlineField + " <= :now"
        if (reservation) {
            condition += " AND outstandingRunCount = :zero"
            values[":zero"] = number(0)
        }
        val operations = mutableListOf(TransactWriteItem.builder().update(update(meta, set, remove, values, condition)).build())
        if (meta.monitoringSlotReserved) operations.add(TransactWriteItem.builder().update(monitoringCapacity(-1)).build())
        return store.transact(operations) {
            store.meta(meta.identityId)?.let { it.monitoringState == state && !it.monitoringSlotReserved } == true
        }
    }

    fun deferReservation(
        meta: CrawlMeta,
        next: Long,
    ): Boolean {
        // The watchdog must terminalize expired runs before their initial reservation can be released.
        val set = mutableListOf("monitoringReservationExpiresAt = :reservation")
        val remove = mutableListOf<String>()
        val values = store.metaValues(meta).toMutableMap()
        values[":reservation"] = number(next)
        values[":one"] = number(1)
        DueSchedule.append(meta, store.configuration.dueIndexShards, set, remove, values, reservationExpiresAt = next)
        return store.transact(listOf(TransactWriteItem.builder().update(update(meta, set, remove, values)).build())) {
            (store.meta(meta.identityId)?.monitoringReservationExpiresAt ?: 0) >= next
        }
    }

    fun preparePipeline(meta: CrawlMeta): Boolean {
        val values = store.metaValues(meta) + mapOf(":target" to text(admission.pipelineVersion), ":one" to number(1))
        return store.transact(
            listOf(
                TransactWriteItem
                    .builder()
                    .update(
                        update(meta, listOf("requiredPipelineVersion = :target"), emptyList(), values),
                    ).build(),
            ),
        ) { store.meta(meta.identityId)?.requiredPipelineVersion == admission.pipelineVersion }
    }

    fun coalesce(
        meta: CrawlMeta,
        active: CrawlRun,
        now: Long,
        next: Long,
    ): Boolean {
        val values = mutableMapOf(":status" to text(active.status.name), ":pipeline" to text(admission.pipelineVersion))
        val deadlineGuard = if (active.status == RunStatus.RUNNING) {
            values[":now"] = number(now)
            " AND deadlineAt > :now"
        } else {
            ""
        }
        val condition = ConditionCheck
            .builder()
            .tableName(store.table)
            .key(DomainKeys.run(active.runId))
            .conditionExpression("#status = :status AND pipelineVersion = :pipeline$deadlineGuard")
            .expressionAttributeNames(mapOf("#status" to "status"))
            .expressionAttributeValues(values)
            .build()
        return defer(meta, now, next, listOf(TransactWriteItem.builder().conditionCheck(condition).build()))
    }

    fun enqueue(
        meta: CrawlMeta,
        now: Long,
        next: Long,
    ): CrawlRun? {
        val run = queuedRun(meta, now)
        val set = mutableListOf(
            "activeRunIdHint = :run",
            "nextCheckAt = :next",
            "lastMonitoringRunId = :run",
        )
        val remove = mutableListOf("activeHintExpiresAt")
        val values = dueValues(meta, now, next)
        values[":run"] = text(run.runId)
        DueSchedule.append(meta, store.configuration.dueIndexShards, set, remove, values, nextCheckAt = next)
        val operations = listOf(
            TransactWriteItem
                .builder()
                .update(
                    update(
                        meta,
                        set,
                        remove,
                        values,
                        DUE_CONDITION,
                        "outstandingRunCount :one",
                    ),
                ).build(),
            TransactWriteItem
                .builder()
                .put(
                    Put
                        .builder()
                        .tableName(store.table)
                        .item(RunCodec.encode(run))
                        .conditionExpression("attribute_not_exists(pk)")
                        .build(),
                ).build(),
            TransactWriteItem
                .builder()
                .update(
                    Update
                        .builder()
                        .tableName(store.table)
                        .key(DynamoSchema.capacityKey)
                        .updateExpression("ADD outstandingRuns :one")
                        .conditionExpression("attribute_exists(pk) AND outstandingRuns < :maximum")
                        .expressionAttributeValues(mapOf(":one" to number(1), ":maximum" to number(admission.maxOutstandingRuns)))
                        .build(),
                ).build(),
        )
        return if (store.transact(operations) { store.run(run.runId) != null }) run else null
    }

    fun defer(
        meta: CrawlMeta,
        now: Long,
        next: Long,
    ): Boolean = defer(meta, now, next, emptyList())

    private fun queuedRun(
        meta: CrawlMeta,
        now: Long,
    ): CrawlRun {
        val id = UUID.randomUUID().toString()
        val shard = DomainKeys.shard(id, store.configuration.workIndexShards)
        val origin = meta.effectiveOrigin ?: URI(meta.normalizedSeedUrl).let { it.scheme + "://" + it.authority }
        return CrawlRun(
            id,
            meta.identityId,
            meta.normalizedSeedUrl,
            admission.pipelineVersion,
            origin,
            now,
            null,
            now,
            shard,
            "WORK#QUEUED#" + shard,
            DomainKeys.ordered(now) + "#" + id,
            admission.budgets(),
            trigger = "MONITORING",
        )
    }

    private fun defer(
        meta: CrawlMeta,
        now: Long,
        next: Long,
        extra: List<TransactWriteItem>,
    ): Boolean {
        val set = mutableListOf("nextCheckAt = :next")
        val remove = mutableListOf<String>()
        val values = dueValues(meta, now, next)
        DueSchedule.append(meta, store.configuration.dueIndexShards, set, remove, values, nextCheckAt = next)
        return store.transact(listOf(TransactWriteItem.builder().update(update(meta, set, remove, values, DUE_CONDITION)).build()) + extra) {
            store.meta(meta.identityId)?.let { it.metadataRevision > meta.metadataRevision && (it.nextCheckAt ?: 0) >= next } == true
        }
    }

    private fun dueValues(
        meta: CrawlMeta,
        now: Long,
        next: Long,
    ): MutableMap<String, AttributeValue> = store.metaValues(meta).toMutableMap().apply {
        put(":now", number(now))
        put(":next", number(next))
        put(":active", text("ACTIVE"))
        put(":yes", bool(true))
        put(":one", number(1))
    }

    private fun monitoringCapacity(delta: Long): Update = Update
        .builder()
        .tableName(store.table)
        .key(DynamoSchema.capacityKey)
        .updateExpression("ADD monitoredIdentities :delta")
        .conditionExpression(if (delta > 0) "attribute_exists(pk) AND monitoredIdentities < :bound" else "monitoredIdentities >= :bound")
        .expressionAttributeValues(
            mapOf(
                ":delta" to number(delta),
                ":bound" to number(if (delta > 0) admission.maxMonitoredIdentities else 1),
            ),
        ).build()

    private fun update(
        meta: CrawlMeta,
        set: List<String>,
        remove: List<String>,
        values: Item,
        extraCondition: String = "",
        additionalCounter: String? = null,
    ): Update {
        val expression = listOfNotNull(
            set.takeIf { it.isNotEmpty() }?.joinToString(", ", prefix = "SET "),
            remove.takeIf { it.isNotEmpty() }?.joinToString(", ", prefix = "REMOVE "),
            "ADD metadataRevision :one" + (additionalCounter?.let { ", " + it } ?: ""),
        ).joinToString(" ")
        return Update
            .builder()
            .tableName(store.table)
            .key(DomainKeys.crawl(meta.identityId))
            .updateExpression(expression)
            .conditionExpression(store.metaCondition(meta).conditionExpression() + extraCondition)
            .expressionAttributeValues(values)
            .build()
    }

    companion object {
        private const val DUE_CONDITION = " AND monitoringState = :active AND monitoringSlotReserved = :yes " +
            "AND monitoringExpiresAt > :now AND nextCheckAt <= :now"
    }
}
