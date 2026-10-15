package com.joaquinduran.llmstxt.api.mapping

import com.joaquinduran.llmstxt.api.dto.MonitoringResponse
import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import io.micronaut.http.annotation.*
import java.time.Clock

@jakarta.inject.Singleton
class MonitoringView(
    private val store: AdmissionStore,
    private val config: CrawlConfiguration,
    private val clock: Clock,
) {
    fun response(meta: CrawlMeta): MonitoringResponse {
        val active = meta.monitoringState == MonitoringState.ACTIVE && meta.monitoringSlotReserved &&
            (meta.monitoringExpiresAt ?: 0) > clock.millis()
        val state = if (meta.monitoringState == MonitoringState.ACTIVE && !active) MonitoringState.EXPIRED else meta.monitoringState
        return MonitoringResponse(
            state.name,
            active && config.execution.enabled,
            meta.nextCheckAt?.takeIf { active }?.let(::timestamp),
            meta.monitoringExpiresAt?.let(::timestamp),
            meta.hasCanonical() && (meta.monitoringExpiresAt ?: 0) > clock.millis(),
            meta.failureCount,
            meta.lastMonitoringRunId?.let { id ->
                store
                    .run(id)
                    ?.takeIf {
                        (it.expiresAtEpochSeconds ?: Long.MAX_VALUE) > clock.millis() / 1000
                    }?.dto()
            },
        )
    }
}
