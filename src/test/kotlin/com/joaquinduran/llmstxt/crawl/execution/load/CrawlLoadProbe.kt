package com.joaquinduran.llmstxt.crawl.execution.load

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlRunner
import com.joaquinduran.llmstxt.crawl.execution.WorkDispatcher
import com.joaquinduran.llmstxt.crawl.fetch.OriginCoordinator
import com.joaquinduran.llmstxt.ingestion.Admission
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.ingestion.BulkBatchProcessor
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.sha256
import com.joaquinduran.llmstxt.ingestion.submitAndProcess
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.results.ArtifactRead
import io.micronaut.context.ApplicationContext
import io.micronaut.json.JsonMapper
import io.micronaut.runtime.server.EmbeddedServer
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import java.lang.management.ManagementFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.util.UUID

/** Explicit local benchmark, excluded from the normal test suite and production distribution. */
fun main(args: Array<String>) {
    require(args.size == 2) { "Provide the worker count (4, 8 or 12) and a temporary evidence directory." }
    val workers = args[0].toInt()
    require(workers in setOf(4, 8, 12))
    val output = Path.of(args[1])
    Files.createDirectories(output.resolve("artifacts"))
    val dynamo = DynamoConfiguration.fromEnvironment()
    dynamo.requireLocal()
    dynamo.table = "llms-txt-load-${UUID.randomUUID()}"
    DynamoClients.create(dynamo).use { client ->
        try {
            LocalTableBootstrap(client, dynamo).initialize()
            val properties = mapOf<String, Any>(
                "dynamodb.environment" to "local",
                "dynamodb.endpoint" to dynamo.endpoint,
                "dynamodb.table" to dynamo.table,
                "micronaut.server.port" to 0,
                "crawl.enabled" to false,
                "crawl.max-concurrent-crawls" to workers,
                "admission.max-outstanding-runs" to 100,
                "admission.max-open-bulk-batches" to 20,
                "admission.max-monitored-identities" to 100,
                "admission.max-queue-wait-seconds" to 1800,
                "test-load.enabled" to true,
            )
            ApplicationContext.run(EmbeddedServer::class.java, properties).use { server ->
                measure(server, workers, output)
            }
        } finally {
            client.deleteTable(DeleteTableRequest.builder().tableName(dynamo.table).build())
        }
    }
}

private fun measure(
    server: EmbeddedServer,
    workers: Int,
    output: Path,
) {
    val context = server.applicationContext
    val store = context.getBean(AdmissionStore::class.java)
    val admission = context.getBean(AdmissionService::class.java)
    val settings = context.getBean(AdmissionConfiguration::class.java)
    val configuration = context.getBean(CrawlConfiguration::class.java)
    val transport = context.getBean(LoadFixtureTransport::class.java)
    val mapper = context.getBean(JsonMapper::class.java)
    val measurements = LoadMeasurements()
    val urls = (0 until 100).map { "https://site-${it.toString().padStart(3, '0')}.load-fixture.example/docs" }
    // Preload through the real admission service, leaving public HTTP submission rates unchanged.
    val batches = urls.chunked(10).map { submitAndProcess(admission, store, context.getBean(Clock::class.java), it, false) }
    val rows = batches.flatMap { store.rows(it.batchId, true) }
    check(rows.size == 100 && rows.all { it.admission == Admission.QUEUED })
    val runIds = rows.map { requireNotNull(it.runId) }.toSet()
    check(runIds.size == 100 && store.capacity().outstandingRuns == 100L)
    val sharedBatches = mutableListOf<com.joaquinduran.llmstxt.ingestion.Batch>()
    val shared = urls
        .chunked(
            10,
        ).flatMap {
            store.rows(
                submitAndProcess(admission, store, context.getBean(Clock::class.java), it, false).also(sharedBatches::add).batchId,
                true,
            )
        }
    check(shared.all { it.admission == Admission.COALESCED } && shared.map { it.runId }.toSet() == runIds)
    val adapter = AdmissionStore(measurements.client(store.client), store.configuration)
    val dispatcher = WorkDispatcher(
        ExecutionStore(adapter, settings, configuration, context.getBean(Clock::class.java)),
        context.getBean(CrawlRunner::class.java),
        context.getBean(OriginCoordinator::class.java),
        configuration,
        settings,
        context.getBean(Clock::class.java),
    )
    val started = System.nanoTime()
    val cpuStarted = LoadMeasurements.cpuNanos()
    var nextDispatchAt = started
    var runs = emptyList<CrawlRun>()
    val samples = mutableListOf<Map<String, Any?>>()
    try {
        HttpClient.newHttpClient().use { http ->
            while (runs.size != 100 || runs.any { !it.status.terminal() }) {
                check(System.nanoTime() - started < 600_000_000_000) { "The isolated workload exceeded ten minutes." }
                if (System.nanoTime() >= nextDispatchAt) {
                    dispatcher.tick()
                    nextDispatchAt = System.nanoTime() + configuration.execution.dispatchPollSeconds * 1_000_000_000
                }
                runs = store.runs(runIds, true).values.toList()
                val capacity = store.capacity()
                val running = runs.count { it.status == RunStatus.RUNNING }
                check(running <= workers && capacity.outstandingRuns <= 100 && capacity.monitoredIdentities <= 100)
                val latency = request(http, "${server.url}/api/batches/${batches[samples.size % batches.size].batchId}")
                measurements.api.add(latency)
                if (samples.size % 5 == 0) measurements.readiness.add(request(http, "${server.url}/health/readiness"))
                samples.add(
                    mapOf(
                        "elapsedSeconds" to (System.nanoTime() - started) / 1_000_000_000.0,
                        "running" to running,
                        "outstanding" to capacity.outstandingRuns,
                        "completed" to runs.count { it.status == RunStatus.COMPLETED },
                        "heapBytes" to ManagementFactory.getMemoryMXBean().heapMemoryUsage.used,
                        "rssBytes" to LoadMeasurements.rssBytes(),
                    ),
                )
                Files.write(output.resolve("samples.json"), mapper.writeValueAsBytes(samples))
                if (runs.any { !it.status.terminal() }) Thread.sleep(1000)
            }
            val elapsedMillis = (System.nanoTime() - started) / 1_000_000.0
            val cpuMillis = (LoadMeasurements.cpuNanos() - cpuStarted) / 1_000_000.0
            check(runs.all { it.status == RunStatus.COMPLETED })
            check(store.capacity().outstandingRuns == 0L && store.capacity().monitoredIdentities == 100L)
            check(runs.sumOf { it.counters.requests } == transport.requests.get().toLong())
            check(transport.peakRequestsPerOrigin.get() == 1 && transport.peakRequests.get() <= workers)
            check(transport.minimumStartSpacingMillis.get() >= 995) { "Origin spacing must tolerate only millisecond clock rounding." }
            val hashes = rows
                .associate { row ->
                    val identityId = requireNotNull(row.identityId)
                    val meta = requireNotNull(store.meta(identityId))
                    val bytes = (CrawlResultReader(store).latestArtifact(identityId) { false } as ArtifactRead.Downloaded).bytes
                    check(sha256(bytes.toString(Charsets.UTF_8)) == meta.currentArtifactHash)
                    Files.write(output.resolve("artifacts/$identityId.txt"), bytes)
                    identityId to meta.currentArtifactHash
                }.toSortedMap()
            BulkBatchProcessor(store, BulkBatchStore(store, context.getBean(Clock::class.java)), admission, context.getBean(Clock::class.java))
                .use { processor -> (batches + sharedBatches).forEach { processor.process(it.batchId) } }
            val cached = urls
                .chunked(
                    10,
                ).flatMap { store.rows(submitAndProcess(admission, store, context.getBean(Clock::class.java), it, false).batchId, true) }
            check(cached.all { it.admission == Admission.CACHED })
            val report = mapOf(
                "workers" to workers,
                "admitted" to rows.size,
                "completed" to runs.size,
                "coalesced" to shared.size,
                "cached" to cached.size,
                "elapsedSeconds" to elapsedMillis / 1000,
                "jobsPerMinute" to runs.size * 60_000 / elapsedMillis,
                "queueWaitMillis" to LoadMeasurements.distribution(runs.map { requireNotNull(it.startedAt).minus(it.queuedAt).toDouble() }),
                "crawlDurationMillis" to
                    LoadMeasurements.distribution(runs.map { requireNotNull(it.finishedAt) - requireNotNull(it.startedAt) }.map(Long::toDouble)),
                "apiBatchMillis" to LoadMeasurements.distribution(measurements.api),
                "readinessMillis" to LoadMeasurements.distribution(measurements.readiness),
                "workerDispatcherSdkMillis" to measurements.sdkSummary(),
                "peakHeapBytes" to samples.maxOf { it.getValue("heapBytes") as Long },
                "peakRssBytes" to samples.mapNotNull { it["rssBytes"] as Long? }.maxOrNull(),
                "meanProcessCpuPercent" to cpuMillis / elapsedMillis * 100,
                "sourceRequestAttempts" to transport.requests.get(),
                "peakSourceRequests" to transport.peakRequests.get(),
                "peakSourceRequestsPerOrigin" to transport.peakRequestsPerOrigin.get(),
                "minimumOriginSpacingMillis" to transport.minimumStartSpacingMillis.get(),
                "artifactHashes" to hashes,
            )
            Files.write(output.resolve("summary.json"), mapper.writeValueAsBytes(report))
            println("Controlled workload passed: workers=$workers admitted=100 completed=100 elapsedSeconds=${elapsedMillis / 1000}.")
        }
    } finally {
        dispatcher.close()
    }
}

private fun request(
    http: HttpClient,
    url: String,
): Double {
    val started = System.nanoTime()
    val response = http.send(HttpRequest.newBuilder(URI(url)).GET().build(), HttpResponse.BodyHandlers.discarding())
    check(response.statusCode() == 200) { "Observation failed with HTTP ${response.statusCode()}." }
    return (System.nanoTime() - started) / 1_000_000.0
}
