package com.joaquinduran.llmstxt.api.query

import com.joaquinduran.llmstxt.api.dto.ApiError
import com.joaquinduran.llmstxt.api.dto.BatchResponse
import com.joaquinduran.llmstxt.api.dto.BatchRowResponse
import com.joaquinduran.llmstxt.api.errors.ApiProblem
import com.joaquinduran.llmstxt.api.mapping.dto
import com.joaquinduran.llmstxt.api.mapping.timestamp
import com.joaquinduran.llmstxt.ingestion.Admission
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.ingestion.Batch
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import jakarta.inject.Singleton
import java.time.Clock

@Singleton
class AdmissionStatus(
    private val store: AdmissionStore,
    private val admission: AdmissionService,
    private val clock: Clock,
    private val bulkBatches: BulkBatchStore = BulkBatchStore(store, clock),
) {
    fun run(id: String): CrawlRun {
        val run = store.run(id, false) ?: store.run(id, true) ?: throw ApiProblem(404, "NOT_FOUND", "Run not found.")
        if ((run.expiresAtEpochSeconds ?: Long.MAX_VALUE) <= clock.millis() / 1000) throw ApiProblem(410, "EXPIRED", "Run metadata has expired.")
        return run
    }

    fun batch(
        id: String,
        page: BatchPage = BatchPage(),
    ): BatchResponse {
        var batch = store.batch(id, false) ?: store.batch(id, true) ?: throw ApiProblem(404, "NOT_FOUND", "Batch not found.")
        if ((!batch.bulk || !batch.workPending) &&
            batch.expiresAtEpochSeconds <= clock.millis() / 1000
        ) {
            throw ApiProblem(410, "EXPIRED", "Batch metadata has expired.")
        }
        if (batch.bulk && !batch.received) batch = store.batch(id, true) ?: throw StorageUnavailable()
        if (batch.bulk && !batch.received) throw ApiProblem(409, "BATCH_NOT_READY", "The batch receipt is incomplete.")
        if (!batch.bulk && !batch.admissionComplete && clock.millis() >= requireNotNull(batch.admissionDeadlineAt)) {
            // A stale META must not trigger repair. Reread both META and positions strongly.
            batch = store.batch(id, true) ?: throw StorageUnavailable()
            batch = admission.repair(batch)
        }
        var rows = if (batch.bulk) store.rowsPage(id, page.offset, page.limit) else store.rows(id, false)
        if (!batch.bulk && rows.size != batch.inputs.size) rows = store.rows(id, true)
        val runIds = rows.mapNotNull { it.runId }.toSet()
        val runs = store.runs(runIds, false).toMutableMap()
        val missing = runIds - runs.keys
        if (missing.isNotEmpty()) runs.putAll(store.runs(missing, true))
        val expiredRuns = (runIds - runs.keys).associateWith { runId ->
            val row = rows.first { it.runId == runId }
            val meta = row.identityId?.let { store.meta(it) } ?: throw StorageUnavailable()
            val completedPrefix = batch.bulk && row.index < batch.nextCompletionIndex
            // Pending runs have no TTL. Without a terminal prefix or a settled identity, fail closed.
            if (!completedPrefix && meta.outstandingRunCount != 0L) throw StorageUnavailable()
            meta
        }
        val byIndex = rows.associateBy { it.index }
        val inputs = if (batch.bulk) bulkBatches.inputs(batch, page.offset, page.limit) else batch.inputs
        val items = inputs.mapIndexed { pageIndex, input ->
            val index = if (batch.bulk) page.offset + pageIndex else pageIndex
            val row = byIndex[index]
            if (row == null) {
                BatchRowResponse(index, input, null, if (batch.bulk) "PENDING" else "ADMITTING")
            } else {
                val run = row.runId?.let(runs::get)
                val expired = row.runId?.let(expiredRuns::get)
                val versionSequence = row.versionSequence ?: run?.versionSequence ?: expired?.currentVersionSequence
                BatchRowResponse(
                    index,
                    input,
                    row.admission.name,
                    if (expired != null) "EXPIRED" else run?.status?.name ?: if (row.admission == Admission.REJECTED) "FAILED" else "COMPLETED",
                    row.identityId,
                    row.runId,
                    run?.phase?.name,
                    versionSequence,
                    row.acceptedObservationSequence,
                    row.outputHash ?: run?.outputHash ?: expired?.currentArtifactHash,
                    if (versionSequence != null) "/artifacts/${row.identityId}/llms.txt" else null,
                    row.error?.dto() ?: run?.error?.dto() ?: expired?.let {
                        ApiError("RUN_DETAILS_EXPIRED", "Execution details expired. The current website file remains available when present.")
                    },
                )
            }
        }
        val status = when {
            batch.bulk && batch.nextCompletionIndex == batch.totalInputs -> "COMPLETED"
            !batch.admissionComplete -> "ADMITTING"
            batch.bulk -> "PROCESSING"
            items.any { it.status in setOf("QUEUED", "RUNNING") } -> "PROCESSING"
            else -> "COMPLETED"
        }
        return BatchResponse(
            batchId = id,
            status = status,
            admissionComplete = batch.admissionComplete,
            createdAt = timestamp(batch.createdAt),
            admissionDeadlineAt = batch.admissionDeadlineAt?.let(::timestamp),
            items = if (batch.bulk) items else items.drop(page.offset).take(page.limit),
            totalItems = batch.totalInputs,
            offset = page.offset,
            limit = page.limit,
            nextOffset = page.nextOffset(batch.totalInputs),
            admittedItems = if (batch.bulk) batch.resolvedInputs else rows.size,
        )
    }
}
