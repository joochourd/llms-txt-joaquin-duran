package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.fetch.OriginCoordinator
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.DomainError
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunCounters
import com.joaquinduran.llmstxt.ingestion.RunPhase
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.ExecutionStore
import com.joaquinduran.llmstxt.persistence.dynamo.RunCodec
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Requires
import io.micronaut.runtime.event.annotation.EventListener
import io.micronaut.runtime.server.event.ServerStartupEvent
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import java.time.Clock
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean

@Context
@Requires(property = "crawl.enabled", value = "true", defaultValue = "true")
class WorkDispatcher(
    private val execution: ExecutionStore,
    private val runner: CrawlRunner,
    private val origins: OriginCoordinator,
    private val config: CrawlConfiguration,
    private val admission: AdmissionConfiguration,
    private val clock: Clock,
) {
    private companion object {
        const val MIN_IDLE_POLL_SECONDS = 10L
    }

    private val log = LoggerFactory.getLogger(WorkDispatcher::class.java)
    private val workers = ThreadPoolExecutor(
        config.execution.maxConcurrentCrawls,
        config.execution.maxConcurrentCrawls,
        0,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(config.execution.maxConcurrentCrawls),
        ThreadFactory {
            Thread(it, "crawl-worker").apply { isDaemon = true }
        },
        ThreadPoolExecutor.AbortPolicy(),
    )
    private val dispatcher = Executors.newSingleThreadScheduledExecutor { Thread(it, "work-dispatch").apply { isDaemon = true } }
    private val slots = Semaphore(config.execution.maxConcurrentCrawls)
    private val active = ConcurrentHashMap<String, CrawlBudget>()
    private val cursors = mutableMapOf<String, Map<String, AttributeValue>>()
    private var shard = 0

    @Volatile private var stopping = false
    private var idle = false

    /** Submission rejection and worker completion share exactly one local cleanup owner. */
    private inner class DispatchRegistration(
        private val origin: AutoCloseable,
    ) : AutoCloseable {
        private val closed = AtomicBoolean(false)
        private var registered: Pair<String, CrawlBudget>? = null

        fun register(
            runId: String,
            budget: CrawlBudget,
        ) {
            registered = runId to budget
            active[runId] = budget
        }

        override fun close() {
            if (!closed.compareAndSet(false, true)) return
            registered?.let { (runId, budget) -> active.remove(runId, budget) }
            try {
                origin.close()
            } finally {
                slots.release()
            }
        }
    }

    @EventListener fun start(event: ServerStartupEvent) {
        dispatcher.schedule({ cycle() }, 0, TimeUnit.SECONDS)
    }

    private fun cycle() {
        if (stopping) return
        try {
            tick()
        } catch (_: Exception) {
            log.warn("Durable work dispatch will retry after an infrastructure failure.")
        } finally {
            if (!stopping) {
                dispatcher.schedule(
                    {
                        cycle()
                    },
                    if (idle) {
                        maxOf(
                            MIN_IDLE_POLL_SECONDS,
                            config.execution.dispatchPollSeconds,
                        )
                    } else {
                        config.execution.dispatchPollSeconds
                    },
                    TimeUnit.SECONDS,
                )
            }
        }
    }

    fun tick() {
        val store = execution.store
        val discoverQueued = slots.availablePermits() > 0
        val candidates = mutableListOf<CrawlRun>()
        val examined = mutableMapOf("RUNNING" to 0, "QUEUED" to 0)
        for (offset in 0 until store.configuration.workIndexShards) {
            val indexShard = (shard + offset) % store.configuration.workIndexShards
            for (status in listOf("RUNNING", "QUEUED")) {
                // Waiting work does not expire. Running recovery remains independent of worker availability.
                if (status == "QUEUED" && !discoverQueued) continue
                val remaining = config.execution.dispatchCandidateLimit - examined.getValue(status)
                if (remaining <= 0) continue
                val key = "WORK#$status#$indexShard"
                val request = QueryRequest
                    .builder()
                    .tableName(store.table)
                    .indexName("WorkIndex")
                    .keyConditionExpression("workPk = :pk AND workSk <= :now")
                    .expressionAttributeValues(mapOf(":pk" to text(key), ":now" to text("${DomainKeys.ordered(clock.millis())}#~")))
                    .limit(minOf(remaining, maxOf(1, config.execution.dispatchCandidateLimit / store.configuration.workIndexShards)))
                cursors[key]?.takeIf { it.isNotEmpty() }?.let(request::exclusiveStartKey)
                val response = store.client.query(request.build())
                cursors[key] = response.lastEvaluatedKey()
                examined[status] = examined.getValue(status) + response.count()
                response.items().forEach { item ->
                    val run =
                        store.get(DomainKeys.key(item.s("pk"), item.s("sk"))).takeIf { it.isNotEmpty() }?.let(RunCodec::decode) ?: return@forEach
                    if (run.status.terminal()) return@forEach
                    if (run.status == RunStatus.RUNNING && (run.recoveryAt ?: Long.MAX_VALUE) <= clock.millis()) {
                        active[run.runId]?.cancelled?.set(true)
                        execution.finish(
                            run.runId,
                            RunStatus.TIMED_OUT,
                            PublicationDecision.PRESERVED,
                            DomainError("RUN_EXPIRED", "The running attempt expired before completing."),
                        )
                    } else if (run.status == RunStatus.QUEUED && run.availableAt <= clock.millis()) {
                        candidates.add(run)
                    }
                }
            }
        }
        shard = (shard + 1) % store.configuration.workIndexShards
        idle = examined.values.sum() == 0 && active.isEmpty()
        val oldestByOrigin = candidates
            .groupBy { it.originHint }
            .values
            .map { group ->
                group.minWith(compareBy(CrawlRun::queuedAt, CrawlRun::runId))
            }.sortedWith(compareBy(CrawlRun::queuedAt, CrawlRun::runId))
        for (candidate in oldestByOrigin) {
            if (slots.availablePermits() == 0 || stopping) break
            val origin = origins.tryCrawl(candidate.originHint) ?: continue
            if (!slots.tryAcquire()) {
                origin.close()
                break
            }
            val registration = DispatchRegistration(origin)
            try {
                if (candidate.pipelineVersion != admission.pipelineVersion) {
                    execution.finish(
                        candidate.runId,
                        RunStatus.FAILED,
                        PublicationDecision.PRESERVED,
                        DomainError("PIPELINE_CHANGED", "This queued run requires a different pipeline."),
                    )
                    registration.close()
                    continue
                }
                val run = execution.claim(candidate)
                if (run == null) {
                    registration.close()
                    continue
                }
                val budget = CrawlBudget(run.budgets.maxRequests, maxOf(1, requireNotNull(run.deadlineAt) - clock.millis()))
                registration.register(run.runId, budget)
                workers.execute {
                    val startedNanos = System.nanoTime()
                    var latestCounters = run.counters
                    try {
                        val meta = execution.store.meta(run.identityId) ?: throw StorageUnavailable()
                        val result = runner.run(run, execution.baseline(meta), budget) { phase, counters ->
                            latestCounters = counters
                            execution.progress(run, phase, counters)
                        }
                        budget.remainingMillis()
                        execution.progress(
                            run,
                            RunPhase.PUBLISHING,
                            RunCounters(result.coverage.pagesAttempted, result.coverage.candidatesRetained, budget.requests.toLong()),
                        )
                        val terminal = execution.publish(run, result)
                        log.info(
                            "Crawl finished runId={} identityId={} sequence={} outcome={} pages={} requests={} queueWaitMillis={} elapsedMillis={}",
                            run.runId,
                            run.identityId,
                            run.observationSequence,
                            terminal.publicationDecision,
                            result.coverage.pagesAttempted,
                            budget.requests,
                            requireNotNull(run.startedAt) - run.queuedAt,
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos),
                        )
                    } catch (e: Exception) {
                        val error = if (e is CrawlFailure) {
                            DomainError(
                                e.code,
                                e.message!!,
                            )
                        } else {
                            DomainError(
                                "EXECUTION_FAILED",
                                "The crawl could not be completed; a previous result remains available when present.",
                            )
                        }
                        try {
                            val terminal = execution.finish(
                                run.runId,
                                if (clock.millis() >=
                                    run.deadlineAt
                                ) {
                                    RunStatus.TIMED_OUT
                                } else {
                                    RunStatus.FAILED
                                },
                                PublicationDecision.PRESERVED,
                                error,
                                counters = latestCounters.copy(requests = budget.requests.toLong()),
                            )
                            log.info(
                                "Crawl failed runId={} identityId={} status={} errorCode={} pages={} requests={} elapsedMillis={}",
                                run.runId,
                                run.identityId,
                                terminal.status,
                                terminal.error?.code,
                                terminal.counters.pages,
                                terminal.counters.requests,
                                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos),
                            )
                        } catch (_: Exception) {
                            log.warn("Run finalization remains recoverable runId={}", run.runId)
                        }
                    } finally {
                        registration.close()
                    }
                }
            } catch (_: Exception) {
                registration.close()
                log.warn("Claim interrupted; durable recovery will close expired work.")
            }
        }
    }

    @PreDestroy
    fun close() {
        stopping = true
        dispatcher.shutdownNow()
        active.values.forEach { it.cancelled.set(true) }
        workers.shutdownNow()
        // Best effort only. A process killed during storage I/O remains recoverable by WorkIndex.
        active.keys.forEach { id ->
            runCatching {
                execution.finish(
                    id,
                    RunStatus.FAILED,
                    PublicationDecision.PRESERVED,
                    DomainError("INTERRUPTED", "The application stopped during execution."),
                )
            }
        }
    }
}
