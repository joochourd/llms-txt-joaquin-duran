package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlRunner
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.CrawlResult
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.DomainError
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunCounters
import com.joaquinduran.llmstxt.ingestion.RunPhase
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.monitoring.MonitoringPolicy
import com.joaquinduran.llmstxt.monitoring.MonitoringProfile
import jakarta.inject.Singleton
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Clock
import java.util.UUID

@Singleton
class ExecutionStore(
    val store: AdmissionStore,
    private val admission: AdmissionConfiguration,
    private val config: CrawlConfiguration,
    private val clock: Clock,
) {
    private companion object {
        const val MAX_TRANSITION_EVALUATIONS = 16
        const val MAX_CURRENT_RESULT_BYTES = 350 * 1024
    }

    private val client get() = store.client
    private val table get() = store.table
    private val monitoringPolicy = MonitoringPolicy(config)
    private val monitoringProfile get() = MonitoringProfile(config.monitoring.monitorIntervalSeconds, config.monitoring.monitorWindowSeconds)

    fun baseline(meta: CrawlMeta): List<Resource> {
        if (!meta.hasCanonical()) return emptyList()
        return CurrentResultCodec.snapshot(store, meta).resources()
    }

    fun claim(queued: CrawlRun): CrawlRun? {
        val now = clock.millis()
        val attempt = UUID.randomUUID().toString()
        val deadline = now + queued.budgets.maxCrawlDurationSeconds * 1000
        val recovery = deadline + admission.runRecoveryGraceSeconds * 1000
        try {
            client.updateItem(
                UpdateItemRequest
                    .builder()
                    .tableName(table)
                    .key(DomainKeys.run(queued.runId))
                    .updateExpression(
                        "SET #status = :running, attemptId = :attempt, startedAt = :now, deadlineAt = :deadline, " +
                            "recoveryAt = :recovery, workPk = :workpk, workSk = :worksk",
                    ).conditionExpression("#status = :queued AND capacityReserved = :yes")
                    .expressionAttributeNames(mapOf("#status" to "status"))
                    .expressionAttributeValues(
                        mapOf(
                            ":running" to text("RUNNING"),
                            ":queued" to text("QUEUED"),
                            ":attempt" to text(attempt),
                            ":now" to number(now),
                            ":deadline" to number(deadline),
                            ":recovery" to number(recovery),
                            ":yes" to bool(true),
                            ":workpk" to text("WORK#RUNNING#${queued.workShard}"),
                            ":worksk" to text("${DomainKeys.ordered(recovery)}#${queued.runId}"),
                        ),
                    ).build(),
            )
        } catch (_: ConditionalCheckFailedException) {
            return null
        }
        // A lost increment response can leave gaps. No HTTP occurs until the returned sequence is persisted.
        val sequence = client
            .updateItem(
                UpdateItemRequest
                    .builder()
                    .tableName(table)
                    .key(DomainKeys.crawl(queued.identityId))
                    .updateExpression("SET lastAttemptAt = :now, lastAttemptRunId = :run ADD nextObservationSequence :one")
                    .conditionExpression("attribute_exists(pk)")
                    .expressionAttributeValues(
                        mapOf(
                            ":now" to number(now),
                            ":run" to text(queued.runId),
                            ":one" to number(1),
                        ),
                    ).returnValues(ReturnValue.UPDATED_NEW)
                    .build(),
            ).attributes()
            .n("nextObservationSequence")
        val run = store.run(queued.runId) ?: throw StorageUnavailable()
        val sequenceUpdate = Update
            .builder()
            .tableName(table)
            .key(DomainKeys.run(run.runId))
            .updateExpression(
                "SET observationSequence = :seq",
            ).conditionExpression("#status = :running AND attemptId = :attempt AND attribute_not_exists(observationSequence)")
            .expressionAttributeNames(mapOf("#status" to "status"))
            .expressionAttributeValues(mapOf(":seq" to number(sequence), ":running" to text("RUNNING"), ":attempt" to text(attempt)))
            .build()
        val applied = store.transact(listOf(TransactWriteItem.builder().update(sequenceUpdate).build())) {
            store.run(run.runId)?.let { it.attemptId == attempt && it.observationSequence == sequence } == true
        }
        if (!applied) return null
        return store.run(run.runId)
    }

    fun progress(
        run: CrawlRun,
        phase: RunPhase,
        counters: RunCounters,
    ) {
        val item = RunCodec.encode(run.copy(counters = counters))
        try {
            client.updateItem(
                UpdateItemRequest
                    .builder()
                    .tableName(table)
                    .key(DomainKeys.run(run.runId))
                    .updateExpression("SET phase = :phase, counters = :counters")
                    .conditionExpression("#status = :running AND attemptId = :attempt")
                    .expressionAttributeNames(mapOf("#status" to "status"))
                    .expressionAttributeValues(
                        mapOf(
                            ":phase" to text(phase.name),
                            ":counters" to item.getValue("counters"),
                            ":running" to text("RUNNING"),
                            ":attempt" to text(requireNotNull(run.attemptId)),
                        ),
                    ).build(),
            )
        } catch (_: ConditionalCheckFailedException) {
            throw CrawlFailure("CANCELLED", "The run is already terminal.")
        }
    }

    private fun terminalUpdate(
        run: CrawlRun,
        status: RunStatus,
        decision: PublicationDecision,
        now: Long,
        version: Long?,
        hash: String?,
        error: DomainError?,
        result: CrawlResult? = null,
        publication: Boolean = false,
        counters: RunCounters? = null,
    ): Update {
        val values = linkedMapOf(
            ":status" to text(status.name),
            ":decision" to text(decision.name),
            ":now" to number(now),
            ":no" to bool(false),
            ":yes" to bool(true),
            ":old" to text(run.status.name),
            ":ttl" to number(now / 1000 + config.execution.runRetentionDays * 86400),
        )
        var set = "#status = :status, publicationDecision = :decision, finishedAt = :now, capacityReserved = :no, expiresAtEpochSeconds = :ttl"
        if (version != null) {
            values[":version"] = number(version)
            set += ", versionSequence = :version"
        }
        if (hash != null) {
            values[":hash"] = text(hash)
            set += ", outputHash = :hash"
        }
        if (error != null) {
            values[":error"] = encodeError(error)
            set += ", #error = :error"
        }
        if (result != null) {
            values[":coverage"] = AttributeValue.builder().m(coverageItem(result.coverage)).build()
            values[":warnings"] = strings(result.warnings)
            set += ", coverage = :coverage, warnings = :warnings"
        }
        if (counters != null) {
            values[":counters"] = RunCodec.encode(run.copy(counters = counters)).getValue("counters")
            set += ", counters = :counters"
        }
        var condition = "#status = :old AND capacityReserved = :yes"
        if (run.attemptId != null) {
            values[":attempt"] = text(run.attemptId)
            condition += " AND attemptId = :attempt"
        }
        if (publication) {
            values[":seq"] = number(requireNotNull(run.observationSequence))
            condition += " AND observationSequence = :seq AND deadlineAt > :now"
        }
        val names = mutableMapOf("#status" to "status")
        if (error != null) names["#error"] = "error"
        return Update
            .builder()
            .tableName(table)
            .key(DomainKeys.run(run.runId))
            .updateExpression("SET $set REMOVE workPk, workSk")
            .conditionExpression(condition)
            .expressionAttributeNames(names)
            .expressionAttributeValues(values)
            .build()
    }

    private fun capacityUpdate(releaseMonitor: Boolean): Update = Update
        .builder()
        .tableName(table)
        .key(DynamoSchema.capacityKey)
        .updateExpression("ADD outstandingRuns :minus" + if (releaseMonitor) ", monitoredIdentities :minus" else "")
        .conditionExpression("outstandingRuns >= :one" + if (releaseMonitor) " AND monitoredIdentities >= :one" else "")
        .expressionAttributeValues(mapOf(":minus" to number(-1), ":one" to number(1)))
        .build()

    /** Single terminal transaction: no partial terminal state and no duplicate quota release. */
    fun finish(
        runId: String,
        status: RunStatus,
        decision: PublicationDecision,
        error: DomainError? = null,
        counters: RunCounters? = null,
    ): CrawlRun {
        repeat(MAX_TRANSITION_EVALUATIONS) {
            val run = store.run(runId) ?: throw StorageUnavailable()
            if (run.status.terminal()) return run
            val meta = store.meta(run.identityId) ?: throw StorageUnavailable()
            val now = clock.millis()
            val release = shouldReleaseInitialReservation(meta)
            val failures = failureCountAfterFinish(meta, run, status)
            val nextCheck = nextCheckAfterFinish(meta, failures, now)
            val items = terminalTransaction(
                runId = runId,
                run = run,
                meta = meta,
                status = status,
                decision = decision,
                error = error,
                now = now,
                releaseMonitor = release,
                failureCount = failures,
                nextCheck = nextCheck,
                counters = counters,
            )
            commitTerminalTransaction(runId, items)?.let { return it }
        }
        throw StorageUnavailable()
    }

    fun publish(
        run: CrawlRun,
        result: CrawlResult,
    ): CrawlRun {
        val hash = sha256(result.bytes.toString(Charsets.UTF_8))
        validateResultSize(run, result, hash)
        val sequence = requireNotNull(run.observationSequence)
        repeat(MAX_TRANSITION_EVALUATIONS) {
            val currentRun = store.run(run.runId) ?: throw StorageUnavailable()
            if (currentRun.status.terminal()) return currentRun
            val meta = store.meta(run.identityId) ?: throw StorageUnavailable()
            val now = clock.millis()
            if (now >= requireNotNull(run.deadlineAt)) {
                return finish(
                    run.runId,
                    RunStatus.TIMED_OUT,
                    PublicationDecision.PRESERVED,
                    DomainError("TIME_BUDGET", "The publication deadline expired."),
                )
            }
            if (sequence <= meta.acceptedObservationSequence) {
                return finish(run.runId, RunStatus.COMPLETED, PublicationDecision.SUPERSEDED)
            }
            if (meta.requiredPipelineVersion != run.pipelineVersion) {
                return finish(
                    run.runId,
                    RunStatus.FAILED,
                    PublicationDecision.PRESERVED,
                    DomainError("PIPELINE_CHANGED", "The required pipeline changed during execution."),
                )
            }
            // Always reevaluate against the CURRENT accepted snapshot after any CAS conflict.
            val previous = try {
                baseline(meta)
            } catch (_: StorageUnavailable) {
                return@repeat
            }
            try {
                CrawlRunner.validateBaseline(previous, result.checks, result.effectiveOrigin)
            } catch (e: CrawlFailure) {
                return finish(run.runId, RunStatus.FAILED, PublicationDecision.PRESERVED, DomainError(e.code, e.message!!))
            }
            val first = !meta.hasCanonical()
            val changed = hash != meta.currentArtifactHash
            val items = acceptanceTransaction(
                run = run,
                currentRun = currentRun,
                meta = meta,
                result = result,
                hash = hash,
                sequence = sequence,
                now = now,
                first = first,
                changed = changed,
                sourceChanged = sourceHashes(previous) != sourceHashes(result.resources),
                profile = monitoringProfile,
            )
            commitTerminalTransaction(
                run.runId,
                items,
                beforeAttempt = { clock.millis() < run.deadlineAt },
            )?.let { return it }
        }
        throw StorageUnavailable()
    }

    private fun shouldReleaseInitialReservation(meta: CrawlMeta): Boolean =
        !meta.hasCanonical() && meta.monitoringState == MonitoringState.RESERVED &&
            meta.monitoringSlotReserved && meta.outstandingRunCount == 1L

    private fun failureCountAfterFinish(
        meta: CrawlMeta,
        run: CrawlRun,
        status: RunStatus,
    ): Long? = if (status in setOf(RunStatus.FAILED, RunStatus.TIMED_OUT) &&
        meta.monitoringState == MonitoringState.ACTIVE && isCurrentFailedAttempt(meta, run)
    ) {
        meta.failureCount + 1
    } else {
        null
    }

    private fun nextCheckAfterFinish(
        meta: CrawlMeta,
        failures: Long?,
        now: Long,
    ): Long? = if (failures != null) {
        monitoringPolicy.failure(now, failures, monitoringProfile, blockedUntil(meta))
    } else {
        meta.nextCheckAt
    }

    private fun terminalTransaction(
        runId: String,
        run: CrawlRun,
        meta: CrawlMeta,
        status: RunStatus,
        decision: PublicationDecision,
        error: DomainError?,
        now: Long,
        releaseMonitor: Boolean,
        failureCount: Long?,
        nextCheck: Long?,
        counters: RunCounters?,
    ): List<TransactWriteItem> {
        val metadata = finalizationMetadataUpdate(runId, run, meta, decision, now, releaseMonitor, failureCount, nextCheck)
        val finalCounters = counters?.let {
            // A late finalizer cannot regress progress already recorded by this attempt.
            RunCounters(
                pages = maxOf(run.counters.pages, it.pages),
                candidates = maxOf(run.counters.candidates, it.candidates),
                requests = maxOf(run.counters.requests, it.requests),
            )
        }
        val terminal = terminalUpdate(
            run = run,
            status = status,
            decision = decision,
            now = now,
            version = meta.currentVersionSequence,
            hash = meta.currentArtifactHash,
            error = error,
            counters = finalCounters,
        )
        return listOf(
            TransactWriteItem.builder().update(metadata).build(),
            TransactWriteItem.builder().update(terminal).build(),
            TransactWriteItem.builder().update(capacityUpdate(releaseMonitor)).build(),
        )
    }

    private fun finalizationMetadataUpdate(
        runId: String,
        run: CrawlRun,
        meta: CrawlMeta,
        decision: PublicationDecision,
        now: Long,
        release: Boolean,
        failureCount: Long?,
        nextCheck: Long?,
    ): Update {
        val set = mutableListOf<String>()
        val remove = mutableListOf<String>()
        val values = store.metaValues(meta).toMutableMap()
        values[":one"] = number(1)
        values[":minus"] = number(-1)
        if (meta.activeRunIdHint == runId) {
            remove.add("activeRunIdHint")
            remove.add("activeHintExpiresAt")
        }
        if (release) {
            set.add("monitoringState = :none")
            set.add("monitoringSlotReserved = :no")
            values[":none"] = text("NONE")
            values[":no"] = bool(false)
            remove.add("monitoringReservationExpiresAt")
        }
        var failureGuard = ""
        if (failureCount != null) {
            set.addAll(listOf("failureCount = :failures", "nextCheckAt = :next"))
            values[":failures"] = number(failureCount)
            values[":next"] = number(requireNotNull(nextCheck))
            if (run.observationSequence == null) {
                failureGuard = if (meta.lastAttemptRunId == null) {
                    " AND attribute_not_exists(lastAttemptRunId)"
                } else {
                    values[":failedLast"] = text(meta.lastAttemptRunId)
                    " AND lastAttemptRunId = :failedLast"
                }
            }
        }
        DueSchedule.append(
            meta,
            store.configuration.dueIndexShards,
            set,
            remove,
            values,
            state = if (release) MonitoringState.NONE else meta.monitoringState,
            nextCheckAt = nextCheck,
        )
        var condition = "metadataRevision = :revision AND acceptedObservationSequence = :accepted " +
            "AND requiredPipelineVersion = :pipeline AND outstandingRunCount >= :one"
        condition += failureGuard
        if (meta.lastAttemptRunId == runId) {
            set.add("lastAttemptSummary = :summary")
            values[":summary"] = text(decision.name)
            values[":last"] = text(runId)
            condition += " AND lastAttemptRunId = :last"
        }
        return Update
            .builder()
            .tableName(table)
            .key(DomainKeys.crawl(meta.identityId))
            .updateExpression(
                (if (set.isEmpty()) "" else "SET " + set.joinToString(", ")) +
                    (if (remove.isEmpty()) "" else " REMOVE " + remove.joinToString(", ")) +
                    " ADD metadataRevision :one, outstandingRunCount :minus",
            ).conditionExpression(condition)
            .expressionAttributeValues(values)
            .build()
    }

    private fun acceptanceTransaction(
        run: CrawlRun,
        currentRun: CrawlRun,
        meta: CrawlMeta,
        result: CrawlResult,
        hash: String,
        sequence: Long,
        now: Long,
        first: Boolean,
        changed: Boolean,
        sourceChanged: Boolean,
        profile: MonitoringProfile,
    ): List<TransactWriteItem> {
        val metadata = publicationMetadataUpdate(run, meta, result, hash, sequence, now, first, changed, profile)
        val currentResult = Put
            .builder()
            .tableName(table)
            .item(CurrentResultCodec.encode(run, result, hash, now, sourceChanged))
            .build()
        val decision = if (changed) PublicationDecision.UPDATED else PublicationDecision.UNCHANGED
        val terminal = terminalUpdate(
            run = currentRun,
            status = RunStatus.COMPLETED,
            decision = decision,
            now = now,
            version = if (changed) sequence else meta.currentVersionSequence,
            hash = hash,
            error = null,
            result = result,
            publication = true,
        )
        val items = mutableListOf(
            TransactWriteItem.builder().update(metadata).build(),
            TransactWriteItem.builder().put(currentResult).build(),
            TransactWriteItem.builder().update(terminal).build(),
            TransactWriteItem.builder().update(capacityUpdate(false)).build(),
        )
        return items
    }

    private fun publicationMetadataUpdate(
        run: CrawlRun,
        meta: CrawlMeta,
        result: CrawlResult,
        hash: String,
        sequence: Long,
        now: Long,
        first: Boolean,
        changed: Boolean,
        profile: MonitoringProfile,
    ): Update {
        val values = store.metaValues(meta).toMutableMap().apply {
            put(":seq", number(sequence))
            put(":now", number(now))
            put(":started", number(requireNotNull(run.startedAt)))
            put(":one", number(1))
            put(":minus", number(-1))
            put(":zero", number(0))
            put(":final", text(result.finalSeedUrl))
            put(":origin", text(result.effectiveOrigin))
        }
        val set = mutableListOf(
            "acceptedObservationSequence = :seq",
            "acceptedPipelineVersion = :pipeline",
            "acceptedObservationStartedAt = :started",
            "lastSuccessfulValidationAt = :now",
            "finalSeedUrl = :final",
            "effectiveOrigin = :origin",
            "failureCount = :zero",
        )
        val remove = mutableListOf<String>()
        if (changed) {
            values[":hash"] = text(hash)
            set.addAll(listOf("currentVersionSequence = :seq", "currentArtifactHash = :hash", "lastOutputUpdatedAt = :now"))
        }
        if (meta.activeRunIdHint == run.runId) remove.addAll(listOf("activeRunIdHint", "activeHintExpiresAt"))
        var nextCheck = meta.nextCheckAt
        var expiry = meta.monitoringExpiresAt
        if (first) {
            expiry = monitoringPolicy.initialWindow(now, profile)
            values[":expiry"] = number(expiry)
            set.add("monitoringExpiresAt = :expiry")
            if (meta.monitoringSlotReserved && meta.monitoringState == MonitoringState.RESERVED) {
                nextCheck = monitoringPolicy.success(meta.identityId, now, profile, blockedUntil(meta))
                values[":active"] = text("ACTIVE")
                values[":next"] = number(nextCheck)
                set.addAll(listOf("monitoringState = :active", "nextCheckAt = :next"))
            } else {
                nextCheck = null
                remove.add("nextCheckAt")
            }
            remove.add("monitoringReservationExpiresAt")
        } else if (meta.monitoringState == MonitoringState.ACTIVE) {
            nextCheck = monitoringPolicy.success(meta.identityId, now, profile, blockedUntil(meta))
            values[":next"] = number(nextCheck)
            set.add("nextCheckAt = :next")
        }
        DueSchedule.append(
            meta,
            store.configuration.dueIndexShards,
            set,
            remove,
            values,
            state = if (first && meta.monitoringSlotReserved) MonitoringState.ACTIVE else meta.monitoringState,
            nextCheckAt = nextCheck,
            expiresAt = expiry,
        )
        var condition = "metadataRevision = :revision AND acceptedObservationSequence = :accepted " +
            "AND acceptedObservationSequence < :seq AND requiredPipelineVersion = :pipeline AND outstandingRunCount >= :one"
        if (meta.lastAttemptRunId == run.runId) {
            set.add("lastAttemptSummary = :summary")
            values[":summary"] = text(if (changed) "UPDATED" else "UNCHANGED")
            values[":last"] = text(run.runId)
            condition += " AND lastAttemptRunId = :last"
        }
        return Update
            .builder()
            .tableName(table)
            .key(DomainKeys.crawl(run.identityId))
            .updateExpression(
                (if (set.isEmpty()) "" else "SET " + set.joinToString(", ")) +
                    (if (remove.isEmpty()) "" else " REMOVE " + remove.joinToString(", ")) +
                    " ADD metadataRevision :one, outstandingRunCount :minus",
            ).conditionExpression(condition)
            .expressionAttributeValues(values)
            .build()
    }

    private fun sourceHashes(resources: List<Resource>): Map<String, List<String>> = resources.associate { resource ->
        resource.resourceKey to listOf(resource.contentHash, resource.metadataHash, resource.relevantLinksHash)
    }

    private fun validateResultSize(
        run: CrawlRun,
        result: CrawlResult,
        hash: String,
    ) {
        require(result.bytes.size <= config.content.maxLlmsBytes) { "The generated file exceeds its limit." }
        result.resources.forEach { resource ->
            require(resourceItem(resource).toString().toByteArray().size <= config.content.maxResourceMetadataBytes) {
                "Resource metadata exceeds its limit."
            }
        }
        // The current result is one DynamoDB item. The conservative encoded bound leaves room below DynamoDB's 400 KiB limit.
        val encoded = CurrentResultCodec.encode(run, result, hash, clock.millis(), sourceChanged = false).toMutableMap()
        encoded.remove("body")
        require(encoded.toString().toByteArray().size + result.bytes.size <= MAX_CURRENT_RESULT_BYTES) {
            "The accepted result exceeds the storage limit."
        }
    }

    private fun commitTerminalTransaction(
        runId: String,
        items: List<TransactWriteItem>,
        beforeAttempt: () -> Boolean = { true },
    ): CrawlRun? {
        if (!store.transact(items, beforeAttempt) { store.run(runId)?.status?.terminal() == true }) return null
        // A terminal RUN is the witness for every write and capacity release in this transaction.
        return store.run(runId) ?: throw StorageUnavailable()
    }

    private fun isCurrentFailedAttempt(
        meta: CrawlMeta,
        run: CrawlRun,
    ): Boolean {
        val sequence = run.observationSequence
        if (sequence != null) {
            return meta.lastAttemptRunId == run.runId && sequence > meta.acceptedObservationSequence
        }
        return run.trigger == "MONITORING" && meta.lastMonitoringRunId == run.runId &&
            (meta.lastAttemptAt ?: 0) < run.queuedAt && (meta.lastSuccessfulValidationAt ?: 0) <= run.queuedAt
    }

    private fun blockedUntil(meta: CrawlMeta): Long {
        val origin = meta.effectiveOrigin ?: java.net.URI(meta.normalizedSeedUrl).let { it.scheme + "://" + it.authority }
        return store.blockedUntil(origin)
    }
}
