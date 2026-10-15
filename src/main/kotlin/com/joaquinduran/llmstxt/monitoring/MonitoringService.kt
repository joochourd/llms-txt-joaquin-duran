package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.fetch.OriginCoordinator
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.MonitoringControlWrite
import com.joaquinduran.llmstxt.persistence.dynamo.MonitoringStore
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import jakarta.inject.Singleton
import java.net.URI
import java.time.Clock

@Singleton
class MonitoringService(
    private val store: AdmissionStore,
    private val persistence: MonitoringStore,
    private val admission: AdmissionConfiguration,
    private val configuration: CrawlConfiguration,
    private val clock: Clock,
    private val origins: OriginCoordinator? = null,
) {
    private val policy = MonitoringPolicy(configuration)

    fun change(
        identityId: String,
        enabled: Boolean,
    ): CrawlMeta {
        repeat(MAX_TRANSITION_ATTEMPTS) {
            val meta = requireCanonical(identityId)
            val now = clock.millis()
            if (!enabled && meta.monitoringState == MonitoringState.NONE && !meta.monitoringSlotReserved) return meta
            val outcome = if (enabled) enable(meta, now) else persistence.disable(meta)
            when (outcome) {
                MonitoringControlWrite.COMMITTED -> return requireNotNull(store.meta(identityId))

                MonitoringControlWrite.CONFLICT -> Unit

                MonitoringControlWrite.CAPACITY_UNAVAILABLE -> throw MonitoringRejected(
                    "MONITORING_CAPACITY_EXCEEDED",
                    "Monitoring capacity is full. Your current result remains available.",
                )
            }
        }
        throw StorageUnavailable()
    }

    private fun enable(
        meta: CrawlMeta,
        now: Long,
    ): MonitoringControlWrite {
        val expiry = meta.monitoringExpiresAt
        if (expiry == null || expiry <= now) {
            throw MonitoringRejected("MONITORING_EXPIRED", "The automatic monitoring window has expired. You can still refresh the result manually.")
        }
        if (meta.monitoringState == MonitoringState.ACTIVE && meta.monitoringSlotReserved) {
            return MonitoringControlWrite.COMMITTED
        }
        return persistence.enable(
            meta = meta,
            expiry = expiry,
            nextCheck = maxOf(now, blockedUntil(meta)),
            resetFailures = true,
        )
    }

    fun expire(meta: CrawlMeta): Boolean {
        val now = clock.millis()
        val reservation = meta.monitoringState == MonitoringState.RESERVED
        val deadline = if (reservation) meta.monitoringReservationExpiresAt else meta.monitoringExpiresAt
        if (meta.monitoringState !in setOf(MonitoringState.ACTIVE, MonitoringState.RESERVED) || deadline == null || deadline > now) return false
        if (reservation && meta.outstandingRunCount > 0) return persistence.deferReservation(meta, policy.pending(now))
        return persistence.expire(meta, now, reservation)
    }

    fun admit(hint: CrawlMeta): MonitoringAdmission {
        var meta = hint
        repeat(MAX_TRANSITION_ATTEMPTS) {
            val now = clock.millis()
            if (!eligible(meta, now)) return MonitoringAdmission(MonitoringAdmissionKind.SKIPPED)
            if (meta.requiredPipelineVersion != admission.pipelineVersion) {
                if (!persistence.preparePipeline(meta)) {
                    meta = requireNotNull(store.meta(meta.identityId))
                    return@repeat
                }
                meta = requireNotNull(store.meta(meta.identityId))
            }
            val active = knownActive(meta, now)
            if (active != null) {
                if (persistence.coalesce(meta, active, now, policy.pending(now))) {
                    return MonitoringAdmission(MonitoringAdmissionKind.COALESCED, active.runId)
                }
            } else {
                val blocked = blockedUntil(meta)
                val origin = meta.effectiveOrigin ?: URI(meta.normalizedSeedUrl).let { it.scheme + "://" + it.authority }
                if (blocked > now || origins?.canCrawl(origin) == false || store.capacity().outstandingRuns >= admission.maxOutstandingRuns) {
                    if (persistence.defer(meta, now, policy.pending(now, blocked))) return MonitoringAdmission(MonitoringAdmissionKind.DEFERRED)
                } else {
                    persistence.enqueue(meta, now, policy.pending(now))?.let { run ->
                        return MonitoringAdmission(MonitoringAdmissionKind.ENQUEUED, run.runId)
                    }
                }
            }
            meta = requireNotNull(store.meta(meta.identityId))
        }
        throw StorageUnavailable()
    }

    private fun eligible(
        meta: CrawlMeta,
        now: Long,
    ) = meta.hasCanonical() && meta.monitoringState == MonitoringState.ACTIVE && meta.monitoringSlotReserved &&
        (meta.monitoringExpiresAt ?: 0) > now && (meta.nextCheckAt ?: Long.MAX_VALUE) <= now

    private fun knownActive(
        meta: CrawlMeta,
        now: Long,
    ): CrawlRun? {
        val id = meta.activeRunIdHint ?: return null
        return store.run(id)?.takeIf {
            it.identityId == meta.identityId && it.pipelineVersion == admission.pipelineVersion &&
                !it.status.terminal() && (it.status == RunStatus.QUEUED || (it.deadlineAt ?: 0) > now)
        }
    }

    private fun requireCanonical(id: String): CrawlMeta {
        val meta = store.meta(id) ?: throw MonitoringRejected("NOT_FOUND", "Crawl not found.")
        if (!meta.hasCanonical()) throw MonitoringRejected("NO_RESULT", "Monitoring controls require an accepted result.")
        return meta
    }

    private fun blockedUntil(meta: CrawlMeta): Long {
        val origin = meta.effectiveOrigin ?: URI(meta.normalizedSeedUrl).let { it.scheme + "://" + it.authority }
        return store.blockedUntil(origin)
    }

    companion object {
        private const val MAX_TRANSITION_ATTEMPTS = 16
    }
}
