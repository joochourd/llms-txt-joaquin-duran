package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.ExecutionStore
import com.joaquinduran.llmstxt.persistence.dynamo.number
import com.joaquinduran.llmstxt.persistence.dynamo.text
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Tag("integration")
class SaturatedDispatchTest : MonitoringFixture() {
    private val queuedQueries = AtomicInteger()
    private val runningQueries = AtomicInteger()
    private val reads = ConcurrentHashMap<String, AtomicInteger>()

    @Test
    fun `the oldest waiting origin is claimed before a newer alphabetically earlier origin`() {
        withBusyWorker { dispatcher, busy, release ->
            val older = queued("https://z-older.fixture.example/")
            clock.now += 1_000
            val newer = queued("https://a-newer.fixture.example/")
            val claimed = mutableListOf<String>()
            release.countDown()
            await({ store.run(busy.runId)?.status?.terminal() == true })
            await({ permits(dispatcher) == 1 })

            // Each tick can claim just one run; terminal state records which origin won the free worker.
            await({ store.run(older.runId)?.status?.terminal() == true || store.run(newer.runId)?.status?.terminal() == true }) {
                dispatcher.tick()
                for (run in listOf(older, newer)) {
                    if (store.run(run.runId)?.status != RunStatus.QUEUED && run.runId !in claimed) claimed.add(run.runId)
                }
            }

            assertEquals(older.runId, claimed.first())
            assertEquals(RunStatus.QUEUED, requireNotNull(store.run(newer.runId)).status)
        }
    }

    @Test
    fun `occupied workers do not repeatedly discover or reread queued candidates`() {
        withBusyWorker { dispatcher, _, _ ->
            val pending = queued("https://pending.fixture.example/")
            queuedQueries.set(0)
            reads.clear()

            repeat(4) { dispatcher.tick() }

            assertEquals(0, queuedQueries.get(), "Pending discovery must wait while local execution is full.")
            assertEquals(0, reads["RUN#${pending.runId}"]?.get() ?: 0)
            assertEquals(RunStatus.QUEUED, requireNotNull(store.run(pending.runId)).status)
            assertTrue(runningQueries.get() > 0, "Running recovery must remain independent of available worker slots.")
        }
    }

    @Test
    fun `waiting work retains its reservation after a legacy queue deadline`() {
        withBusyWorker { dispatcher, _, _ ->
            val pending = queued("https://pending.fixture.example/")
            update(DomainKeys.run(pending.runId), "SET queueDeadlineAt = :deadline", mapOf(":deadline" to number(clock.now + 1)))
            clock.now += 10_001

            repeat(3) { dispatcher.tick() }

            assertEquals(RunStatus.QUEUED, requireNotNull(store.run(pending.runId)).status)
            assertEquals(2L, store.capacity().outstandingRuns)
            assertEquals(2L, store.capacity().monitoredIdentities)
            assertTrue(requireNotNull(store.run(pending.runId)).capacityReserved)
        }
    }

    @Test
    fun `running recovery continues between queued maintenance cycles`() {
        withBusyWorker { dispatcher, _, _ ->
            val orphan = requireNotNull(execution.claim(queued("https://orphan.fixture.example/")))
            val recoveryAt = clock.now + 1
            update(
                DomainKeys.run(orphan.runId),
                "SET recoveryAt = :recovery, workSk = :work",
                mapOf(":recovery" to number(recoveryAt), ":work" to text("${DomainKeys.ordered(recoveryAt)}#${orphan.runId}")),
            )
            queuedQueries.set(0)
            clock.now += 2

            await({ store.run(orphan.runId)?.status == RunStatus.TIMED_OUT }) { dispatcher.tick() }

            assertEquals(0, queuedQueries.get())
            assertEquals(1L, store.capacity().outstandingRuns)
            assertFalse(requireNotNull(store.run(orphan.runId)).capacityReserved)
        }
    }

    @Test
    fun `newly available workers discover pending work before the maintenance interval`() {
        withBusyWorker { dispatcher, busy, release ->
            val pending = queued("https://pending.fixture.example/")
            queuedQueries.set(0)
            release.countDown()
            await({ store.run(busy.runId)?.status?.terminal() == true })
            await({ permits(dispatcher) == 1 })

            await({ store.run(pending.runId)?.status == RunStatus.COMPLETED }) { dispatcher.tick() }

            assertTrue(queuedQueries.get() > 0)
            assertEquals(0L, store.capacity().outstandingRuns)
            assertEquals(2L, store.capacity().monitoredIdentities)
        }
    }

    private fun withBusyWorker(test: (WorkDispatcher, CrawlRun, CountDownLatch) -> Unit) {
        config.execution.maxConcurrentCrawls = 1
        val busy = queued("https://busy.fixture.example/")
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val html = "<main><h1>Useful fixture</h1><p>${"Useful fixture content. ".repeat(30)}</p></main>"
        val transport = PageTransport { url, budget, _ ->
            if (url.host == "busy.fixture.example" && url.encodedPath == "/") {
                started.countDown()
                check(release.await(10, TimeUnit.SECONDS)) { "The test must release its occupied worker." }
                budget.remainingMillis()
            }
            FetchResponse(
                url,
                if (url.encodedPath == "/") 200 else 404,
                mapOf("Content-Type" to listOf("text/html")),
                if (url.encodedPath == "/") html.toByteArray() else byteArrayOf(),
            )
        }
        val adapter = AdmissionStore(
            proxy { name, arguments, call ->
                if (name == "query") {
                    val key = (arguments[0] as QueryRequest).expressionAttributeValues()[":pk"]?.s().orEmpty()
                    if (key.startsWith("WORK#QUEUED#")) queuedQueries.incrementAndGet()
                    if (key.startsWith("WORK#RUNNING#")) runningQueries.incrementAndGet()
                }
                if (name == "getItem") {
                    val key = requireNotNull((arguments[0] as GetItemRequest).key()["pk"]).s()
                    reads.computeIfAbsent(key) { AtomicInteger() }.incrementAndGet()
                }
                call()
            },
            store.configuration,
        )
        val runner = CrawlRunner(SafeFetcher(transport, origins, config, clock, UrlIdentityResolver(settings)), config, clock)
        val dispatcher = WorkDispatcher(ExecutionStore(adapter, settings, config, clock), runner, origins, config, settings, clock)

        try {
            await({ started.count == 0L }) { dispatcher.tick() }
            test(dispatcher, busy, release)
        } finally {
            release.countDown()
            dispatcher.close()
        }
    }

    private fun permits(dispatcher: WorkDispatcher): Int = (
        WorkDispatcher::class.java
            .getDeclaredField("slots")
            .apply { isAccessible = true }
            .get(dispatcher) as Semaphore
    ).availablePermits()
}
