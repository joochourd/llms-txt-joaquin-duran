package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.Item
import com.joaquinduran.llmstxt.persistence.dynamo.MonitoringStore
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Requires
import io.micronaut.runtime.event.annotation.EventListener
import io.micronaut.runtime.server.event.ServerStartupEvent
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import java.time.Clock
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Context
@Requires(property = "crawl.enabled", value = "true", defaultValue = "true")
class MonitoringScheduler(
    private val store: AdmissionStore,
    private val monitoring: MonitoringService,
    private val due: MonitoringStore,
    private val config: CrawlConfiguration,
    private val clock: Clock,
) {
    private val log = LoggerFactory.getLogger(MonitoringScheduler::class.java)
    private val executor = Executors.newSingleThreadScheduledExecutor {
        Thread(it, "monitoring-scheduler").apply { isDaemon = true }
    }
    private val cursors = mutableMapOf<Int, Item>()
    private var shard = 0

    @Volatile private var stopping = false

    @EventListener
    fun start(event: ServerStartupEvent) {
        executor.schedule({ cycle() }, 0, TimeUnit.SECONDS)
    }

    private fun cycle() {
        if (stopping) return
        try {
            tick()
        } catch (_: Exception) {
            log.warn("Monitoring discovery will retry after an infrastructure failure.")
        } finally {
            if (!stopping) executor.schedule({ cycle() }, config.monitoring.schedulerTickSeconds, TimeUnit.SECONDS)
        }
    }

    @Synchronized
    fun tick(): Int {
        var examined = 0
        val shards = store.configuration.dueIndexShards
        for (offset in 0 until shards) {
            val remaining = config.monitoring.schedulerCandidateLimit - examined
            if (remaining <= 0 || stopping) break
            val current = (shard + offset) % shards
            val page = due.due(
                current,
                clock.millis(),
                minOf(remaining, maxOf(1, config.monitoring.schedulerCandidateLimit / shards)),
                cursors[current].orEmpty(),
            )
            cursors[current] = page.continuation
            examined += page.identities.size
            for (id in page.identities.distinct()) {
                try {
                    val observed = store.meta(id) ?: continue
                    val meta = due.reconcileSchedule(observed) ?: continue
                    monitoring.expire(meta)
                    val currentMeta = store.meta(id) ?: continue
                    monitoring.admit(currentMeta)
                } catch (_: Exception) {
                    log.warn("Monitoring action remains recoverable identityId={}", id)
                }
            }
        }
        shard = (shard + 1) % shards
        return examined
    }

    @PreDestroy
    fun close() {
        stopping = true
        executor.shutdownNow()
    }
}
