package com.joaquinduran.llmstxt.ingestion

import com.joaquinduran.llmstxt.api.errors.ApiProblem
import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.monitoring.MonitoringAdmission
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import com.joaquinduran.llmstxt.persistence.dynamo.MonitoringStore
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import jakarta.inject.Singleton
import java.time.Clock
import java.util.UUID

/** Initial reads for one bounded page; conditional writes remain the authority for every decision. */
internal data class AdmissionReads(
    val decisions: Map<Int, BatchItem>,
    val metadata: Map<String, CrawlMeta>,
    val activeRuns: Map<String, CrawlRun>,
)

@Singleton
class AdmissionService(
    private val store: AdmissionStore,
    private val configuration: AdmissionConfiguration,
    private val identities: UrlIdentityResolver,
    private val clock: Clock,
    private val bulkBatches: BulkBatchStore = BulkBatchStore(store, clock, configuration),
) {
    private companion object {
        const val MAX_ADMISSION_EVALUATIONS = 16
        const val RECEIPT_TIMEOUT_MILLIS = 60_000L
        const val MAX_PREFETCH_INPUTS = 100
    }

    fun submit(
        inputs: List<String>,
        force: Boolean,
    ): Batch {
        validateInputs(inputs)
        val now = clock.millis()
        val batch = Batch(
            batchId = UUID.randomUUID().toString(),
            inputs = inputs.toList(),
            force = force,
            createdAt = now,
            admissionDeadlineAt = null,
            expiresAtEpochSeconds = now / 1000 + configuration.batchRetentionDays * 86400,
            bulk = true,
            received = false,
            workPending = true,
            receiptDeadlineAt = now + RECEIPT_TIMEOUT_MILLIS,
        )
        return bulkBatches.receive(batch)
    }

    fun validateInputs(inputs: List<String>) {
        if (inputs.isEmpty() || inputs.size > configuration.maxBatchUrls) {
            throw ApiProblem(400, "INVALID_REQUEST", "Provide between 1 and ${configuration.maxBatchUrls} URLs.")
        }
        if (inputs.any { it.toByteArray(Charsets.UTF_8).size > configuration.maxUrlBytes }) {
            throw ApiProblem(400, "INVALID_REQUEST", "Each input must fit within ${configuration.maxUrlBytes} UTF-8 bytes.")
        }
    }

    // Position is the durable idempotency boundary. HTTP submissions themselves create independent batches.
    fun admit(
        batch: Batch,
        index: Int,
    ): BatchItem = requireNotNull(evaluateAdmission(batch, index, batch.inputs[index], waitForCapacity = false))

    internal fun prefetch(
        batch: Batch,
        offset: Int,
        inputs: List<String>,
    ): AdmissionReads {
        require(inputs.size <= MAX_PREFETCH_INPUTS && offset >= 0 && offset + inputs.size <= batch.totalInputs)
        if (inputs.isEmpty()) return AdmissionReads(emptyMap(), emptyMap(), emptyMap())
        val decisions = store.rowsPage(batch.batchId, offset, inputs.size, strong = true).associateBy { it.index }
        val pendingInputs = inputs.filterIndexed { position, _ -> offset + position !in decisions }
        val identityIds = pendingInputs
            .distinct()
            .mapNotNull { input ->
                try {
                    identities.normalize(input).identityId
                } catch (_: InvalidSeed) {
                    null
                }
            }.toSet()
        val metadata = store.metas(identityIds)
        val activeRuns = store.runs(metadata.values.mapNotNull { it.activeRunIdHint }.toSet(), strong = true)
        return AdmissionReads(decisions, metadata, activeRuns)
    }

    internal fun admitPending(
        batch: Batch,
        index: Int,
        input: String,
        reads: AdmissionReads? = null,
    ): BatchItem? = evaluateAdmission(batch, index, input, waitForCapacity = true, reads = reads)

    /** Resolve already available work without taking a new execution or monitoring slot. */
    internal fun resolveAvailable(
        batch: Batch,
        index: Int,
        input: String,
        reads: AdmissionReads? = null,
    ): BatchItem? = evaluateAdmission(batch, index, input, waitForCapacity = true, allowNewWork = false, reads = reads)

    private fun evaluateAdmission(
        batch: Batch,
        index: Int,
        input: String,
        waitForCapacity: Boolean,
        allowNewWork: Boolean = true,
        reads: AdmissionReads? = null,
    ): BatchItem? {
        require(index in 0 until batch.totalInputs)
        val existingDecision = if (reads != null) reads.decisions[index] else store.row(batch.batchId, index)
        existingDecision?.let { return it }
        if (!batch.bulk && clock.millis() >= requireNotNull(batch.admissionDeadlineAt)) return reject(batch, index, input, interrupted())
        val identity = try {
            identities.normalize(input)
        } catch (exception: InvalidSeed) {
            return reject(batch, index, input, exception.error)
        }
        // Recheck absent identities: an earlier position in this page may just have created one.
        val initialMeta = reads?.metadata?.get(identity.identityId) ?: store.meta(identity.identityId)
        if (initialMeta == null) {
            if (!allowNewWork) return null
            store.ensureIdentity(identity, configuration.pipelineVersion)
        }
        // A lost conditional race reenters here to load the durable decision and current state.
        repeat(MAX_ADMISSION_EVALUATIONS) { attempt ->
            if (attempt > 0 || reads == null) store.row(batch.batchId, index)?.let { return it }
            val now = clock.millis()
            if (!batch.bulk && now >= requireNotNull(batch.admissionDeadlineAt)) return reject(batch, index, input, interrupted())
            // A conflict discards the prefetched state, including its active-run observation.
            val firstRead = initialMeta.takeIf { attempt == 0 && reads != null }
            val meta = firstRead ?: store.meta(identity.identityId) ?: throw StorageUnavailable()
            if (meta.requiredPipelineVersion != configuration.pipelineVersion) {
                store.preparePipeline(meta, configuration.pipelineVersion)
                return@repeat
            }
            val active = compatibleActiveRun(meta, identity.identityId, now, reads.takeIf { attempt == 0 })
            if (active != null) {
                val row = row(batch, index, input, Admission.COALESCED, identity.identityId, active.runId)
                store.recordCoalescedDecision(row, meta, active, configuration.pipelineVersion, now, batch)?.let { return it }
                return@repeat
            }
            if (hasFreshResult(meta, batch.force, now)) {
                val row = row(batch, index, input, Admission.CACHED, identity.identityId).copy(
                    versionSequence = meta.currentVersionSequence,
                    acceptedObservationSequence = meta.acceptedObservationSequence,
                    outputHash = meta.currentArtifactHash,
                )
                store.recordDecision(row, meta, batch)?.let { return it }
                return@repeat
            }
            val blockedUntil = store.blockedUntil(meta.effectiveOrigin ?: identity.origin)
            if (blockedUntil > now) {
                val decision = row(batch, index, input, Admission.REJECTED, identity.identityId).copy(
                    error = DomainError("ORIGIN_BACKOFF", "The origin is temporarily backing off. Please retry later."),
                )
                store.recordDecision(decision, meta, batch)?.let { return it }
                return@repeat
            }
            if (!allowNewWork) return null
            val capacity = store.capacity()
            val monitoringDelta = if (
                !meta.hasCanonical() && !meta.monitoringSlotReserved && capacity.monitoredIdentities < configuration.maxMonitoredIdentities
            ) {
                1L
            } else {
                0L
            }
            val quotaError = when {
                capacity.outstandingRuns >= configuration.maxOutstandingRuns -> {
                    DomainError("QUEUE_FULL", "Outstanding work capacity is full. Please retry later.")
                }

                else -> {
                    null
                }
            }
            if (quotaError != null) {
                if (waitForCapacity) return null
                val decision = row(batch, index, input, Admission.REJECTED, identity.identityId).copy(error = quotaError)
                store.recordDecision(decision, meta, batch)?.let { return it }
                return@repeat
            }
            store
                .createRun(
                    batch = batch,
                    index = index,
                    identity = identity,
                    meta = meta,
                    admission = configuration,
                    now = now,
                    monitoringDelta = monitoringDelta,
                    input = input,
                )?.let { return it }
        }
        throw StorageUnavailable()
    }

    private fun compatibleActiveRun(
        meta: CrawlMeta,
        identityId: String,
        now: Long,
        reads: AdmissionReads? = null,
    ): CrawlRun? {
        val active = meta.activeRunIdHint?.let { reads?.activeRuns?.get(it) ?: store.run(it) }
        return active?.takeIf {
            it.identityId == identityId && it.pipelineVersion == configuration.pipelineVersion &&
                !it.status.terminal() &&
                (it.status == RunStatus.QUEUED || (it.deadlineAt ?: 0) > now)
        }
    }

    private fun hasFreshResult(
        meta: CrawlMeta,
        force: Boolean,
        now: Long,
    ): Boolean {
        val observedAt = meta.acceptedObservationStartedAt
        return !force && meta.hasCanonical() && meta.acceptedPipelineVersion == configuration.pipelineVersion &&
            observedAt != null && now >= observedAt && now - observedAt < configuration.crawlFreshnessTtlSeconds * 1000
    }

    fun repair(batch: Batch): Batch {
        if (batch.bulk || batch.admissionComplete || clock.millis() < requireNotNull(batch.admissionDeadlineAt)) return batch
        val present = store.rows(batch.batchId, true).map { it.index }.toSet()
        batch.inputs.indices
            .filter { it !in present }
            .forEach { reject(batch, it, batch.inputs[it], interrupted()) }
        store.closeBatch(batch)
        return batch.copy(admissionComplete = true)
    }

    private fun reject(
        batch: Batch,
        index: Int,
        input: String,
        error: DomainError,
    ): BatchItem {
        val row = row(batch, index, input, Admission.REJECTED).copy(error = error)
        return store.recordRejection(row, batch)
    }

    private fun row(
        batch: Batch,
        index: Int,
        input: String,
        admission: Admission,
        identityId: String? = null,
        runId: String? = null,
    ) = BatchItem(
        batch.batchId,
        index,
        input,
        admission,
        identityId,
        runId,
        expiresAtEpochSeconds = if (batch.bulk) null else batch.expiresAtEpochSeconds,
    )

    private fun interrupted() = DomainError("ADMISSION_INTERRUPTED", "Admission did not finish before its deadline. Submit this URL again if needed.")
}
