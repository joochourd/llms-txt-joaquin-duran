package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.Admission
import com.joaquinduran.llmstxt.ingestion.Batch
import com.joaquinduran.llmstxt.ingestion.BatchItem
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.CrawlIdentity
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.DomainError
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunBudgets
import com.joaquinduran.llmstxt.ingestion.RunCounters
import com.joaquinduran.llmstxt.ingestion.RunCoverage
import com.joaquinduran.llmstxt.ingestion.RunPhase
import com.joaquinduran.llmstxt.ingestion.RunStatus
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

internal typealias Item = Map<String, AttributeValue>

internal fun text(value: String) = DynamoSchema.string(value)

internal fun number(value: Long) = AttributeValue.builder().n(value.toString()).build()

internal fun bool(value: Boolean) = AttributeValue.builder().bool(value).build()

internal fun strings(values: List<String>) = AttributeValue.builder().l(values.map(::text)).build()

internal fun Item.s(name: String) = requireNotNull(this[name]?.s()) { "Missing string field: $name" }

internal fun Item.n(name: String) = requireNotNull(this[name]?.n()?.toLongOrNull()) { "Missing number field: $name" }

internal fun Item.b(name: String) = requireNotNull(this[name]?.bool()) { "Missing boolean field: $name" }

internal fun Item.ns(name: String) = this[name]?.n()?.let(String::toLong)

internal fun Item.ss(name: String) = this[name]?.s()

internal fun Item.list(name: String) = requireNotNull(this[name]) { "Missing list: $name" }.l().map { it.s() }

internal fun MutableMap<String, AttributeValue>.optional(
    name: String,
    value: String?,
) {
    if (value != null) put(name, text(value))
}

internal fun MutableMap<String, AttributeValue>.optional(
    name: String,
    value: Long?,
) {
    if (value != null) put(name, number(value))
}

internal fun base(
    pk: String,
    sk: String,
    entity: String,
) = linkedMapOf("pk" to text(pk), "sk" to text(sk), "entityType" to text(entity), "itemSchemaVersion" to number(1))

internal fun Item.check(entity: String) {
    require(s("entityType") == entity && n("itemSchemaVersion") == 1L) { "Unsupported item type or schema." }
}

internal fun encodeError(error: DomainError) = AttributeValue.builder().m(mapOf("code" to text(error.code), "message" to text(error.message))).build()

internal fun Item.error() = this["error"]?.m()?.let { DomainError(it.s("code"), it.s("message")) }

object DomainKeys {
    fun crawl(id: String) = key("CRAWL#$id", "META")

    fun run(id: String) = key("RUN#$id", "META")

    fun batch(id: String) = key("BATCH#$id", "META")

    fun batchItem(
        id: String,
        index: Int,
    ) = key("BATCH#$id", "ITEM#${ordered(index.toLong())}")

    fun origin(origin: String) = key("ORIGIN#${sha256(origin)}", "META")

    fun key(
        pk: String,
        sk: String,
    ): Map<String, AttributeValue> = mapOf("pk" to text(pk), "sk" to text(sk))

    fun ordered(value: Long): String {
        require(value >= 0)
        return value.toString().padStart(20, '0')
    }

    fun shard(
        id: String,
        count: Int,
    ): Int = (sha256(id).take(8).toLong(16) % count).toInt()
}

object CrawlMetaCodec {
    fun encode(meta: CrawlMeta): Map<String, AttributeValue> = base("CRAWL#${meta.identityId}", "META", "CrawlIdentity").apply {
        put("identityId", text(meta.identityId))
        put("normalizedSeedUrl", text(meta.normalizedSeedUrl))
        put("intentProfile", text(meta.intentProfile))
        put("requiredPipelineVersion", text(meta.requiredPipelineVersion))
        put("metadataRevision", number(meta.metadataRevision))
        put("nextObservationSequence", number(meta.nextObservationSequence))
        put("acceptedObservationSequence", number(meta.acceptedObservationSequence))
        put("outstandingRunCount", number(meta.outstandingRunCount))
        put("monitoringState", text(meta.monitoringState.name))
        put("monitoringSlotReserved", bool(meta.monitoringSlotReserved))
        put("failureCount", number(meta.failureCount))
        optional("acceptedPipelineVersion", meta.acceptedPipelineVersion)
        optional("currentVersionSequence", meta.currentVersionSequence)
        optional("currentArtifactHash", meta.currentArtifactHash)
        optional("acceptedObservationStartedAt", meta.acceptedObservationStartedAt)
        optional("finalSeedUrl", meta.finalSeedUrl)
        optional("effectiveOrigin", meta.effectiveOrigin)
        optional("lastAttemptAt", meta.lastAttemptAt)
        optional("lastAttemptRunId", meta.lastAttemptRunId)
        optional("lastAttemptSummary", meta.lastAttemptSummary)
        optional("lastMonitoringRunId", meta.lastMonitoringRunId)
        optional("lastSuccessfulValidationAt", meta.lastSuccessfulValidationAt)
        optional("lastOutputUpdatedAt", meta.lastOutputUpdatedAt)
        optional("activeRunIdHint", meta.activeRunIdHint)
        optional("activeHintExpiresAt", meta.activeHintExpiresAt)
        optional("monitoringReservationExpiresAt", meta.monitoringReservationExpiresAt)
        optional("monitoringExpiresAt", meta.monitoringExpiresAt)
        optional("nextCheckAt", meta.nextCheckAt)
        optional("lastForceCreatedAt", meta.lastForceCreatedAt)
        optional("dueShard", meta.dueShard?.toLong())
        optional("duePk", meta.duePk)
        optional("dueSk", meta.dueSk)
    }

    fun decode(item: Map<String, AttributeValue>): CrawlMeta {
        item.check("CrawlIdentity")
        return CrawlMeta(
            identityId = item.s("identityId"),
            normalizedSeedUrl = item.s("normalizedSeedUrl"),
            intentProfile = item.s("intentProfile"),
            requiredPipelineVersion = item.s("requiredPipelineVersion"),
            metadataRevision = item.n("metadataRevision"),
            nextObservationSequence = item.n("nextObservationSequence"),
            acceptedObservationSequence = item.n("acceptedObservationSequence"),
            outstandingRunCount = item.n("outstandingRunCount"),
            monitoringState = MonitoringState.valueOf(item.s("monitoringState")),
            monitoringSlotReserved = item.b("monitoringSlotReserved"),
            lastMonitoringRunId = item.ss("lastMonitoringRunId"),
            acceptedPipelineVersion = item.ss("acceptedPipelineVersion"),
            currentVersionSequence = item.ns("currentVersionSequence"),
            currentArtifactHash = item.ss("currentArtifactHash"),
            acceptedObservationStartedAt = item.ns("acceptedObservationStartedAt"),
            finalSeedUrl = item.ss("finalSeedUrl"),
            effectiveOrigin = item.ss("effectiveOrigin"),
            lastAttemptAt = item.ns("lastAttemptAt"),
            lastAttemptRunId = item.ss("lastAttemptRunId"),
            lastAttemptSummary = item.ss("lastAttemptSummary"),
            lastSuccessfulValidationAt = item.ns("lastSuccessfulValidationAt"),
            lastOutputUpdatedAt = item.ns("lastOutputUpdatedAt"),
            activeRunIdHint = item.ss("activeRunIdHint"),
            activeHintExpiresAt = item.ns("activeHintExpiresAt"),
            monitoringReservationExpiresAt = item.ns("monitoringReservationExpiresAt"),
            monitoringExpiresAt = item.ns("monitoringExpiresAt"),
            nextCheckAt = item.ns("nextCheckAt"),
            failureCount = item.n("failureCount"),
            lastForceCreatedAt = item.ns("lastForceCreatedAt"),
            dueShard = item.ns("dueShard")?.toInt(),
            duePk = item.ss("duePk"),
            dueSk = item.ss("dueSk"),
        )
    }
}

object RunCodec {
    fun encode(run: CrawlRun): Map<String, AttributeValue> = base("RUN#${run.runId}", "META", "CrawlRun").apply {
        put("runId", text(run.runId))
        put("identityId", text(run.identityId))
        put("normalizedSeedUrl", text(run.normalizedSeedUrl))
        put("pipelineVersion", text(run.pipelineVersion))
        put("originHint", text(run.originHint))
        put("status", text(run.status.name))
        put("phase", text(run.phase.name))
        put("capacityReserved", bool(run.capacityReserved))
        put("trigger", text(run.trigger))
        put("queuedAt", number(run.queuedAt))
        optional("queueDeadlineAt", run.queueDeadlineAt)
        put("availableAt", number(run.availableAt))
        put("workShard", number(run.workShard.toLong()))
        optional("workPk", run.workPk)
        optional("workSk", run.workSk)
        put(
            "budgets",
            AttributeValue
                .builder()
                .m(
                    mapOf(
                        "maxDepth" to number(run.budgets.maxDepth.toLong()),
                        "maxPages" to number(run.budgets.maxPages.toLong()),
                        "maxCandidates" to number(run.budgets.maxCandidates.toLong()),
                        "maxUrlEvaluations" to number(run.budgets.maxUrlEvaluations.toLong()),
                        "maxLinksInspected" to number(run.budgets.maxLinksInspected.toLong()),
                        "maxRequests" to number(run.budgets.maxRequests.toLong()),
                        "maxSelectedResources" to number(run.budgets.maxSelectedResources.toLong()),
                        "maxCrawlDurationSeconds" to number(run.budgets.maxCrawlDurationSeconds),
                    ),
                ).build(),
        )
        put(
            "counters",
            AttributeValue
                .builder()
                .m(
                    mapOf(
                        "pages" to number(run.counters.pages),
                        "candidates" to number(run.counters.candidates),
                        "requests" to number(run.counters.requests),
                    ),
                ).build(),
        )
        put("warnings", strings(run.warnings))
        optional("attemptId", run.attemptId)
        optional("observationSequence", run.observationSequence)
        run.coverage?.let { coverage ->
            put(
                "coverage",
                AttributeValue
                    .builder()
                    .m(
                        mapOf(
                            "pagesAttempted" to number(coverage.pagesAttempted),
                            "pagesUseful" to number(coverage.pagesUseful),
                            "candidatesRetained" to number(coverage.candidatesRetained),
                            "candidatesOmitted" to number(coverage.candidatesOmitted),
                            "limitReasons" to strings(coverage.limitReasons),
                            "discoverySources" to strings(coverage.discoverySources),
                        ),
                    ).build(),
            )
        }
        optional("startedAt", run.startedAt)
        optional("finishedAt", run.finishedAt)
        optional("deadlineAt", run.deadlineAt)
        optional("recoveryAt", run.recoveryAt)
        optional("publicationDecision", run.publicationDecision?.name)
        optional("versionSequence", run.versionSequence)
        optional("outputHash", run.outputHash)
        optional("expiresAtEpochSeconds", run.expiresAtEpochSeconds)
        if (run.error != null) put("error", encodeError(run.error))
    }

    fun decode(item: Map<String, AttributeValue>): CrawlRun {
        item.check("CrawlRun")
        val budgets = item.getValue("budgets").m()
        val counters = item.getValue("counters").m()
        return CrawlRun(
            runId = item.s("runId"),
            identityId = item.s("identityId"),
            normalizedSeedUrl = item.s("normalizedSeedUrl"),
            pipelineVersion = item.s("pipelineVersion"),
            originHint = item.s("originHint"),
            queuedAt = item.n("queuedAt"),
            queueDeadlineAt = item.ns("queueDeadlineAt"),
            availableAt = item.n("availableAt"),
            workShard = item.n("workShard").toInt(),
            workPk = item.ss("workPk"),
            workSk = item.ss("workSk"),
            budgets = RunBudgets(
                budgets.n("maxDepth").toInt(),
                budgets.n("maxPages").toInt(),
                budgets.n("maxCandidates").toInt(),
                budgets.n("maxUrlEvaluations").toInt(),
                budgets.n("maxLinksInspected").toInt(),
                budgets.n("maxRequests").toInt(),
                budgets.n("maxSelectedResources").toInt(),
                budgets.n("maxCrawlDurationSeconds"),
            ),
            status = RunStatus.valueOf(item.s("status")),
            phase = RunPhase.valueOf(item.s("phase")),
            capacityReserved = item.b("capacityReserved"),
            trigger = item.s("trigger"),
            attemptId = item.ss("attemptId"),
            observationSequence = item.ns("observationSequence"),
            startedAt = item.ns("startedAt"),
            finishedAt = item.ns("finishedAt"),
            deadlineAt = item.ns("deadlineAt"),
            recoveryAt = item.ns("recoveryAt"),
            counters = RunCounters(
                counters.n("pages"),
                counters.n("candidates"),
                counters.n("requests"),
            ),
            warnings = item.list("warnings"),
            coverage = item["coverage"]?.m()?.let {
                RunCoverage(
                    it.n("pagesAttempted"),
                    it.n("pagesUseful"),
                    it.n("candidatesRetained"),
                    it.n("candidatesOmitted"),
                    it.list("limitReasons"),
                    it.list("discoverySources"),
                )
            },
            publicationDecision = item.ss("publicationDecision")?.let(PublicationDecision::valueOf),
            error = item.error(),
            versionSequence = item.ns("versionSequence"),
            outputHash = item.ss("outputHash"),
            expiresAtEpochSeconds = item.ns("expiresAtEpochSeconds"),
        )
    }
}

object BatchCodec {
    fun encode(batch: Batch): Map<String, AttributeValue> {
        val item = base("BATCH#${batch.batchId}", "META", "Batch") + mapOf(
            "batchId" to text(batch.batchId),
            "force" to bool(batch.force),
            "createdAt" to number(batch.createdAt),
            "admissionComplete" to bool(batch.admissionComplete),
            "expiresAtEpochSeconds" to number(batch.expiresAtEpochSeconds),
        )
        if (!batch.bulk) {
            return item +
                mapOf("inputs" to strings(batch.inputs), "admissionDeadlineAt" to number(requireNotNull(batch.admissionDeadlineAt)))
        }
        // Active backlog metadata must outlive TTL so a restarted processor can release its reservation.
        return item.minus("expiresAtEpochSeconds") + mapOf(
            "retentionExpiresAtEpochSeconds" to number(batch.expiresAtEpochSeconds),
            "bulk" to bool(true),
            "totalInputs" to number(batch.totalInputs.toLong()),
            "received" to bool(batch.received),
            "nextAdmissionIndex" to number(batch.nextAdmissionIndex.toLong()),
            "nextCompletionIndex" to number(batch.nextCompletionIndex.toLong()),
            "bulkSlotReserved" to bool(batch.bulkSlotReserved),
            "resolvedInputs" to number(batch.resolvedInputs.toLong()),
            "nextRetentionIndex" to number(batch.nextRetentionIndex.toLong()),
            "receiptDeadlineAt" to number(requireNotNull(batch.receiptDeadlineAt)),
        )
    }

    fun decode(item: Map<String, AttributeValue>): Batch {
        item.check("Batch")
        return Batch(
            item.s("batchId"),
            item["inputs"]?.l()?.map { it.s() }.orEmpty(),
            item.b("force"),
            item.n("createdAt"),
            item.ns("admissionDeadlineAt"),
            item.ns("retentionExpiresAtEpochSeconds") ?: item.n("expiresAtEpochSeconds"),
            item.b("admissionComplete"),
            bulk = item["bulk"]?.bool() ?: false,
            totalInputs = item.ns("totalInputs")?.toInt() ?: item.list("inputs").size,
            received = item["received"]?.bool() ?: true,
            nextAdmissionIndex = item.ns("nextAdmissionIndex")?.toInt() ?: 0,
            nextCompletionIndex = item.ns("nextCompletionIndex")?.toInt() ?: 0,
            bulkSlotReserved = item["bulkSlotReserved"]?.bool() ?: false,
            receiptDeadlineAt = item.ns("receiptDeadlineAt"),
            resolvedInputs = item.ns("resolvedInputs")?.toInt() ?: item.ns("nextAdmissionIndex")?.toInt() ?: 0,
            nextRetentionIndex = item.ns("nextRetentionIndex")?.toInt() ?: 0,
        )
    }
}

object BatchItemCodec {
    fun encode(row: BatchItem): Map<String, AttributeValue> =
        base("BATCH#${row.batchId}", "ITEM#${DomainKeys.ordered(row.index.toLong())}", "BatchItem").apply {
            put("batchId", text(row.batchId))
            put("index", number(row.index.toLong()))
            put("seedUrl", text(row.seedUrl))
            put("admission", text(row.admission.name))
            optional("expiresAtEpochSeconds", row.expiresAtEpochSeconds)
            optional("identityId", row.identityId)
            optional("runId", row.runId)
            optional("versionSequence", row.versionSequence)
            optional("acceptedObservationSequence", row.acceptedObservationSequence)
            optional("outputHash", row.outputHash)
            if (row.error != null) put("error", encodeError(row.error))
        }

    fun decode(item: Map<String, AttributeValue>): BatchItem {
        item.check("BatchItem")
        return BatchItem(
            item.s("batchId"),
            item.n("index").toInt(),
            item.s("seedUrl"),
            Admission.valueOf(item.s("admission")),
            item.ss("identityId"),
            item.ss("runId"),
            item.ns("versionSequence"),
            item.ns("acceptedObservationSequence"),
            item.ss("outputHash"),
            item.error(),
            item.ns("expiresAtEpochSeconds"),
        )
    }
}

object CapacityCodec {
    fun decode(item: Map<String, AttributeValue>): Capacity {
        item.check("Capacity")
        return Capacity(item.n("outstandingRuns"), item.n("monitoredIdentities")).also {
            require(it.outstandingRuns >= 0 && it.monitoredIdentities >= 0) { "Invalid capacity counters." }
        }
    }
}
