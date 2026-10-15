package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.execution.CrawlRunner
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.OriginBackoff
import com.joaquinduran.llmstxt.crawl.fetch.OriginCoordinator
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.ingestion.submitAndProcess
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoClients
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoConfiguration
import com.joaquinduran.llmstxt.persistence.dynamo.ExecutionStore
import com.joaquinduran.llmstxt.persistence.dynamo.LocalTableBootstrap
import com.joaquinduran.llmstxt.persistence.dynamo.MonitoringStore
import com.joaquinduran.llmstxt.results.ArtifactRead
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Clock

/** Recognized isolated table; verifies durable scheduling, pending quotas and last-known-good after normal restart. */
fun main(args: Array<String>) {
    require(args.size == 1 && args[0] in setOf("write", "verify", "delete"))
    val configuration = DynamoConfiguration.fromEnvironment(System.getenv())
    configuration.requireLocal()
    configuration.table = "llms-txt-m3-restart-${sha256(configuration.table).take(16)}"
    val markerKey = DomainKeys.key("TEST#M3#RESTART", "META")
    DynamoClients.create(configuration).use { client ->
        val store = AdmissionStore(client, configuration)
        if (args[0] == "delete") {
            store.get(markerKey).check("RestartProbe")
            client.deleteTable(DeleteTableRequest.builder().tableName(configuration.table).build())
            println("Removed recognized M3 restart fixture table.")
            return
        }
        val settings = AdmissionConfiguration()
        val config = CrawlConfiguration().apply { fetch.minRequestIntervalMillis = 0 }
        val clock = MutableClock(Clock.systemUTC().millis())
        if (args[0] == "write") {
            val exists = try {
                client.describeTable(DescribeTableRequest.builder().tableName(configuration.table).build())
                true
            } catch (_: ResourceNotFoundException) {
                false
            }
            check(!exists) { "The restart fixture already exists; inspect it before deleting or rerunning." }
            LocalTableBootstrap(client, configuration).initialize()
            val monitoring =
                MonitoringService(
                    store,
                    MonitoringStore(store, settings),
                    settings,
                    config,
                    clock,
                )
            val service = AdmissionService(store, settings, UrlIdentityResolver(settings), clock)
            val execution = ExecutionStore(store, settings, config, clock)
            val batch = submitAndProcess(service, store, clock, listOf("https://restart-fixture.example/"), false)
            val first = requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(store.rows(batch.batchId, true).single().runId)))))
            val origins = OriginCoordinator(config, OriginBackoff(store, config), clock)
            val transport = PageTransport { url, _, _ ->
                if (url.encodedPath == "/") {
                    FetchResponse(
                        url,
                        200,
                        mapOf("Content-Type" to listOf("text/html")),
                        (
                            "<h1>Monitoring restart fixture</h1><main>" + "Durable monitoring source information. ".repeat(
                                20,
                            ) + "</main>"
                        ).toByteArray(),
                    )
                } else {
                    FetchResponse(url, 404, emptyMap(), byteArrayOf())
                }
            }
            val runner = CrawlRunner(SafeFetcher(transport, origins, config, clock, UrlIdentityResolver(settings)), config, clock)
            val value = runner.run(first, emptyList(), CrawlBudget(60, 10000)) { _, _ -> }
            val published = execution.publish(first, value)
            val hash = requireNotNull(published.outputHash)
            check(published.publicationDecision == PublicationDecision.UPDATED)
            clock.now = requireNotNull(store.meta(first.identityId)?.nextCheckAt)
            val failed = requireNotNull(
                execution.claim(requireNotNull(store.run(requireNotNull(monitoring.admit(requireNotNull(store.meta(first.identityId))).runId)))),
            )
            execution.finish(failed.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)
            val acceptedAfterFailure = requireNotNull(store.meta(first.identityId))
            check(acceptedAfterFailure.currentArtifactHash == hash)
            clock.now = requireNotNull(acceptedAfterFailure.nextCheckAt)
            val pending = requireNotNull(monitoring.admit(requireNotNull(store.meta(first.identityId))).runId)
            check(store.capacity() == Capacity(1, 1))
            check(
                store.putAbsent(
                    base("TEST#M3#RESTART", "META", "RestartProbe") + mapOf(
                        "identityId" to text(first.identityId),
                        "runId" to text(pending),
                        "hash" to text(hash),
                        "clockAt" to number(clock.now),
                    ),
                ),
            )
            println("M3 current result, automatic queued work and monitoring quota persisted before restart.")
        } else {
            val marker = store.get(markerKey)
            marker.check("RestartProbe")
            clock.now = marker.n("clockAt") + 1000
            val id = marker.s("identityId")
            val pending = requireNotNull(store.run(marker.s("runId")))
            val meta = requireNotNull(store.meta(id))
            check(pending.status == RunStatus.QUEUED && pending.trigger == "MONITORING")
            check(meta.monitoringState == MonitoringState.ACTIVE && meta.monitoringSlotReserved)
            check(meta.outstandingRunCount == 1L && meta.currentArtifactHash == marker.s("hash"))
            check(meta.lastMonitoringRunId == pending.runId && meta.nextCheckAt != null && meta.duePk != null)
            check(store.capacity() == Capacity(1, 1))
            val execution = ExecutionStore(store, settings, config, clock)
            check(execution.baseline(meta).isNotEmpty())
            val artifact = CrawlResultReader(store).latestArtifact(id) { false } as ArtifactRead.Downloaded
            check(sha256(artifact.bytes.toString(Charsets.UTF_8)) == marker.s("hash"))
            execution.finish(pending.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)
            check(store.capacity() == Capacity(0, 1))
            val monitoring =
                MonitoringService(
                    store,
                    MonitoringStore(store, settings),
                    settings,
                    config,
                    clock,
                )
            clock.now = requireNotNull(store.meta(id)?.monitoringExpiresAt)
            check(monitoring.expire(requireNotNull(store.meta(id))))
            check(store.capacity() == Capacity(0, 0))
            check(store.meta(id)?.currentArtifactHash == marker.s("hash"))
            println("M3 scheduling, quotas, queued work and last-good data survived restart; expiry released exactly once.")
        }
    }
}
