package com.joaquinduran.llmstxt.ingestion

import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import com.joaquinduran.llmstxt.persistence.dynamo.Item
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Requires
import io.micronaut.runtime.event.annotation.EventListener
import io.micronaut.runtime.server.event.ServerStartupEvent
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import java.time.Clock
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** Promotes durable inputs into the existing bounded work queue; it never runs source crawls itself. */
@Context
@Requires(property = "crawl.enabled", value = "true", defaultValue = "true")
class BulkBatchProcessor(
    private val store: AdmissionStore,
    private val batches: BulkBatchStore,
    private val admission: AdmissionService,
    private val clock: Clock,
) : AutoCloseable {
    private companion object {
        const val INPUTS_PER_TICK = 20
        const val COMPLETIONS_PER_TICK = 100
        const val AVAILABLE_INPUTS_PER_TICK = 100
        const val BATCHES_PER_SHARD_TICK = 5
        const val POLL_SECONDS = 2L
    }

    private val log = LoggerFactory.getLogger(BulkBatchProcessor::class.java)
    private val executor = Executors.newSingleThreadScheduledExecutor {
        Thread(it, "bulk-batch-admission").apply { isDaemon = true }
    }
    private val cursors = mutableMapOf<Int, Item>()
    private val availableOffsets = mutableMapOf<String, Int>()

    @Volatile private var stopping = false

    @EventListener
    fun start(event: ServerStartupEvent) {
        executor.schedule({ cycle() }, 0, TimeUnit.SECONDS)
    }

    private fun cycle() {
        if (stopping) return
        try {
            tick()
        } catch (failure: Exception) {
            log.warn("Bulk batch discovery will retry errorType={}", failure.javaClass.simpleName)
        } finally {
            if (!stopping) executor.schedule({ cycle() }, POLL_SECONDS, TimeUnit.SECONDS)
        }
    }

    @Synchronized
    fun tick() {
        for (shard in 0 until store.configuration.workIndexShards) {
            if (stopping) return
            val page = batches.due(shard, clock.millis(), BATCHES_PER_SHARD_TICK, cursors[shard].orEmpty())
            cursors[shard] = page.continuation
            for (id in page.batchIds) {
                try {
                    process(id)
                } catch (failure: Exception) {
                    log.warn("Bulk batch remains recoverable batchId={} errorType={}", id, failure.javaClass.simpleName)
                }
            }
        }
    }

    fun process(id: String) {
        val initial = store.batch(id)
        if (initial == null || !initial.bulk || !initial.bulkSlotReserved) {
            availableOffsets.remove(id)
            return
        }
        var batch: Batch = initial
        if (!batch.received) {
            batches.expireReceipt(batch)
            return
        }
        if (batch.nextCompletionIndex == batch.totalInputs) {
            availableOffsets.remove(id)
            batches.finalizeRetention(batch)
            return
        }
        val firstPendingIndex = batch.nextAdmissionIndex
        val pending = batches.inputs(batch, firstPendingIndex, INPUTS_PER_TICK)
        val pendingReads = admission.prefetch(batch, firstPendingIndex, pending)
        for ((position, input) in pending.withIndex()) {
            if (stopping) return
            val index = firstPendingIndex + position
            if (batch.nextAdmissionIndex != index) return
            if (admission.admitPending(batch, index, input, pendingReads) == null) break
            if (!batches.advanceAdmission(batch, index + 1)) return
            batch = store.batch(id) ?: return
        }
        resolveAvailableInputs(batch)
        batch = store.batch(id) ?: return
        val rows = store.rowsPage(id, batch.nextAdmissionIndex, AVAILABLE_INPUTS_PER_TICK)
        var next = batch.nextAdmissionIndex
        for (row in rows) {
            if (row.index != next) break
            next++
        }
        if (next > batch.nextAdmissionIndex) {
            batches.advanceAdmission(batch, next)
            batch = store.batch(id) ?: return
        }
        advanceCompletion(batch)
        val current = store.batch(id) ?: return
        if (current.nextCompletionIndex == current.totalInputs) availableOffsets.remove(id)
        batches.finalizeRetention(current)
    }

    /** Capacity-blocked work cannot prevent cache, invalid-input or shared-run decisions elsewhere in the receipt. */
    private fun resolveAvailableInputs(batch: Batch) {
        if (batch.nextAdmissionIndex == batch.totalInputs) return
        val previousOffset = availableOffsets[batch.batchId] ?: batch.nextAdmissionIndex
        val offset = if (previousOffset >= batch.totalInputs) batch.nextAdmissionIndex else maxOf(previousOffset, batch.nextAdmissionIndex)
        val inputs = batches.inputs(batch, offset, AVAILABLE_INPUTS_PER_TICK)
        val reads = admission.prefetch(batch, offset, inputs)
        for ((position, input) in inputs.withIndex()) {
            if (stopping) return
            admission.resolveAvailable(batch, offset + position, input, reads)
        }
        availableOffsets[batch.batchId] = offset + inputs.size
    }

    private fun advanceCompletion(batch: Batch) {
        val first = batch.nextCompletionIndex
        val count = minOf(COMPLETIONS_PER_TICK, batch.nextAdmissionIndex - first)
        if (count <= 0) return
        val rows = store.rowsPage(batch.batchId, first, count)
        val runs = store.runs(rows.mapNotNull { it.runId }.toSet(), strong = true)
        var next = first
        for (row in rows.sortedBy { it.index }) {
            if (row.index != next) break
            val terminal = row.runId?.let { runId ->
                runs[runId]?.status?.terminal()
                    // Only terminal runs have TTL. A missing run cannot close work whose identity still owns a reservation.
                    ?: row.identityId?.let { store.meta(it)?.outstandingRunCount == 0L }
                    ?: false
            } ?: true
            if (!terminal) break
            next++
        }
        if (next > first) batches.advanceCompletion(batch, next)
    }

    @PreDestroy
    override fun close() {
        stopping = true
        executor.shutdownNow()
    }
}
