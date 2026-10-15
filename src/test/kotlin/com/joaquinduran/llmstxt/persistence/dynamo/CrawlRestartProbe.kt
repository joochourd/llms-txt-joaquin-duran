package com.joaquinduran.llmstxt.persistence.dynamo

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
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.ingestion.submitAndProcess
import com.joaquinduran.llmstxt.results.ArtifactRead
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Clock

/** An isolated, local-only fixture for actual published-data restart persistence. */
fun main(args: Array<String>) {
    require(args.size == 1 && args[0] in setOf("write", "verify", "delete"))
    val configuration = DynamoConfiguration.fromEnvironment(System.getenv())
    require(configuration.environment == "local")
    val originalTable = configuration.table
    configuration.table = "llms-txt-m2-restart-${sha256(originalTable).take(16)}"
    val markerKey = DomainKeys.key("TEST#M2#RESTART", "META")
    DynamoClients.create(configuration).use { client ->
        val store = AdmissionStore(client, configuration)
        when (args[0]) {
            "write" -> {
                val exists = try {
                    client.describeTable(DescribeTableRequest.builder().tableName(configuration.table).build())
                    true
                } catch (_: ResourceNotFoundException) {
                    false
                }
                check(!exists) { "Restart fixture already exists; inspect it before deleting or rerunning." }
                LocalTableBootstrap(client, configuration).initialize()
                val admission = AdmissionConfiguration()
                val config = CrawlConfiguration().apply { fetch.minRequestIntervalMillis = 0 }
                val clock = Clock.systemUTC()
                val urls = UrlIdentityResolver(admission)
                val service = AdmissionService(store, admission, urls, clock)
                val execution = ExecutionStore(store, admission, config, clock)
                val batch = submitAndProcess(service, store, clock, listOf("https://restart-fixture.example/"), false)
                val run = execution.claim(store.run(store.rows(batch.batchId, true).single().runId!!)!!)!!
                val origins = OriginCoordinator(config, OriginBackoff(store, config), clock)
                val transport = PageTransport { url, _, _ ->
                    if (url.encodedPath == "/") {
                        FetchResponse(
                            url,
                            200,
                            mapOf("Content-Type" to listOf("text/html")),
                            ("<h1>Restart fixture</h1><main>" + "Durable public-content fixture information. ".repeat(20) + "</main>").toByteArray(),
                        )
                    } else {
                        FetchResponse(url, 404, emptyMap(), byteArrayOf())
                    }
                }
                val result = CrawlRunner(
                    SafeFetcher(transport, origins, config, clock, urls),
                    config,
                    clock,
                ).run(run, emptyList(), CrawlBudget(60, 10000)) { _, _ -> }
                val published = execution.publish(run, result)
                val hash = requireNotNull(published.outputHash)
                check(published.status == RunStatus.COMPLETED)
                check(
                    store.putAbsent(
                        base("TEST#M2#RESTART", "META", "RestartProbe") + mapOf(
                            "runId" to text(run.runId),
                            "identityId" to text(run.identityId),
                            "batchId" to text(batch.batchId),
                            "sequence" to number(run.observationSequence!!),
                            "hash" to text(hash),
                        ),
                    ),
                )
                println("Published M2 fixture written to ${configuration.table}.")
            }

            "verify", "delete" -> {
                val marker = store.get(markerKey)
                marker.check("RestartProbe")
                if (args[0] == "delete") {
                    client.deleteTable(DeleteTableRequest.builder().tableName(configuration.table).build())
                    println("Removed recognized M2 restart fixture table.")
                } else {
                    val run = store.run(marker.s("runId"))!!
                    val meta = store.meta(marker.s("identityId"))!!
                    check(run.status == RunStatus.COMPLETED && !run.capacityReserved && run.workPk == null)
                    check(meta.acceptedObservationSequence == marker.n("sequence") && meta.currentArtifactHash == marker.s("hash"))
                    val reader = CrawlResultReader(store)
                    val artifact = reader.latestArtifact(meta.identityId) { false } as ArtifactRead.Downloaded
                    check(sha256(artifact.bytes.toString(Charsets.UTF_8)) == marker.s("hash"))
                    val accepted = reader.accepted(meta.identityId)
                    check(accepted.metadata.acceptedObservationSequence == marker.n("sequence"))
                    check(accepted.resources.size == 1)
                    check(ExecutionStore(store, AdmissionConfiguration(), CrawlConfiguration(), Clock.systemUTC()).baseline(meta).size == 1)
                    check(store.batch(marker.s("batchId"))!!.admissionComplete && store.rows(marker.s("batchId"), true).size == 1)
                    check(store.capacity() == Capacity(0, 1))
                    println("M2 current file, accepted source evidence, run, batch and counters survived restart.")
                }
            }
        }
    }
}
