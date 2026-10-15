package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.execution.CrawlRunner
import com.joaquinduran.llmstxt.crawl.fetch.OriginBackoff
import com.joaquinduran.llmstxt.crawl.fetch.OriginCoordinator
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.crawl.fetch.SafeTransport
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoClients
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoConfiguration
import com.joaquinduran.llmstxt.persistence.dynamo.LocalTableBootstrap
import io.micronaut.json.JsonMapper
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors

/** Explicit manual evaluation, never part of CI. Source downloads use the production safe boundary. */
fun main(args: Array<String>) {
    require(args.size in 2..11) { "Usage: qualityProbe <output-directory> <url> [url ...]" }
    val directory = Path.of(args[0])
    Files.createDirectories(directory)
    val dynamo = DynamoConfiguration.fromEnvironment(System.getenv()).apply { table = "llms-txt-quality-${UUID.randomUUID()}" }
    dynamo.requireLocal()
    val clock = Clock.systemUTC()
    val config = CrawlConfiguration()
    val settings = AdmissionConfiguration()
    if (System.getenv("QUALITY_SEED_ONLY") == "true") {
        settings.maxPages = 1
        settings.maxSelectedResources = 1
    }
    val urls = UrlIdentityResolver(settings)
    val mapper = JsonMapper.createDefault()
    val client = DynamoClients.create(dynamo)
    try {
        LocalTableBootstrap(client, dynamo).initialize()
        val store = AdmissionStore(client, dynamo)
        val origins = OriginCoordinator(config, OriginBackoff(store, config), clock)
        SafeTransport(config).use { transport ->
            val pool = Executors.newFixedThreadPool(4)
            try {
                pool
                    .invokeAll(
                        args.drop(1).mapIndexed { index, input ->
                            Callable {
                                val events = mutableListOf<Map<String, Any?>>()
                                var seedSaved = false
                                val inspectedTransport = PageTransport { url, budget, limit ->
                                    transport.get(url, budget, limit).also { response ->
                                        events.add(
                                            mapOf(
                                                "event" to "HTTP",
                                                "url" to url.toString(),
                                                "status" to response.status,
                                                "contentType" to response.header("Content-Type"),
                                                "bytes" to response.bytes.size,
                                            ),
                                        )
                                        if (!seedSaved && response.status in 200..299 &&
                                            response.header("Content-Type")?.startsWith("text/html") == true &&
                                            url.encodedPath != "/robots.txt"
                                        ) {
                                            Files.write(directory.resolve("$index.seed.html"), response.bytes)
                                            seedSaved = true
                                        }
                                    }
                                }
                                val fetcher = SafeFetcher(inspectedTransport, origins, config, clock, urls)
                                val observer = object : CrawlObserver {
                                    override fun candidate(candidate: Candidate) {
                                        events.add(
                                            mapOf(
                                                "event" to "CANDIDATE",
                                                "url" to candidate.url,
                                                "source" to candidate.source,
                                                "parent" to candidate.parent,
                                                "anchor" to candidate.anchor,
                                                "depth" to candidate.depth,
                                                "preScore" to candidate.score,
                                            ),
                                        )
                                    }

                                    override fun page(page: ExtractedPage) {
                                        events.add(
                                            mapOf(
                                                "event" to "PAGE",
                                                "url" to page.url.toString(),
                                                "sourceUrl" to page.candidate.url,
                                                "source" to page.candidate.source,
                                                "depth" to page.candidate.depth,
                                                "preScore" to page.candidate.score,
                                                "title" to page.title,
                                                "description" to page.description,
                                                "textLength" to page.textLength,
                                                "contentHash" to page.contentHash,
                                                "headings" to page.headings,
                                                "challenge" to page.challenge,
                                                "noindex" to page.noindex,
                                                "structured" to page.structured.take(8192),
                                                "links" to page.links.map { mapOf("url" to it.url, "anchor" to it.anchor, "source" to it.source) },
                                            ),
                                        )
                                    }

                                    override fun rejected(
                                        url: String,
                                        reason: String,
                                    ) {
                                        events.add(mapOf("event" to "REJECTED", "url" to url, "reason" to reason))
                                    }

                                    override fun selection(
                                        resources: List<Resource>,
                                        selected: List<Resource>,
                                    ) {
                                        val chosen = selected.map { it.resourceKey }.toSet()
                                        resources.forEach {
                                            events.add(
                                                mapOf(
                                                    "event" to "RANKED",
                                                    "url" to it.primaryHtmlUrl,
                                                    "title" to it.title,
                                                    "category" to it.category,
                                                    "section" to it.section,
                                                    "score" to it.score,
                                                    "reasons" to it.reasonCodes,
                                                    "selected" to (it.resourceKey in chosen),
                                                ),
                                            )
                                        }
                                    }

                                    override fun ranking(
                                        resource: Resource,
                                        components: Map<String, Double>,
                                        family: String,
                                        reason: String,
                                    ) {
                                        events.add(
                                            mapOf(
                                                "event" to "EXPLANATION",
                                                "url" to resource.primaryHtmlUrl,
                                                "components" to components,
                                                "score" to resource.score,
                                                "family" to family,
                                                "reason" to reason,
                                            ),
                                        )
                                    }
                                }
                                val identity = urls.normalize(input)
                                val now = clock.millis()
                                val run = CrawlRun(
                                    UUID.randomUUID().toString(),
                                    identity.identityId,
                                    identity.normalizedSeedUrl,
                                    settings.pipelineVersion,
                                    identity.origin,
                                    now,
                                    now + 600000,
                                    now,
                                    0,
                                    null,
                                    null,
                                    settings.budgets(),
                                )
                                val budget = CrawlBudget(run.budgets.maxRequests, run.budgets.maxCrawlDurationSeconds * 1000)
                                val started = System.nanoTime()
                                val report = linkedMapOf<String, Any?>(
                                    "inputUrl" to input,
                                    "pipelineVersion" to settings.pipelineVersion,
                                    "startedAt" to clock.instant().toString(),
                                )
                                try {
                                    val result = CrawlRunner(fetcher, config, clock).run(run, emptyList(), budget, observer) { phase, counts ->
                                        println("$input $phase pages=${counts.pages} requests=${counts.requests}")
                                    }
                                    report.putAll(
                                        mapOf(
                                            "status" to "COMPLETED",
                                            "context" to result.context.type,
                                            "confidence" to result.context.confidence,
                                            "contextEvidence" to result.context.evidence,
                                            "pagesAttempted" to result.coverage.pagesAttempted,
                                            "pagesUseful" to result.coverage.pagesUseful,
                                            "peakFrontier" to result.coverage.candidatesRetained,
                                            "omitted" to result.coverage.candidatesOmitted,
                                            "limitReasons" to result.coverage.limitReasons,
                                            "warnings" to result.warnings,
                                            "selectedCount" to result.resources.size,
                                            "outputBytes" to result.bytes.size,
                                            "selected" to result.resources.map {
                                                mapOf(
                                                    "url" to it.primaryHtmlUrl,
                                                    "title" to it.title,
                                                    "category" to it.category,
                                                    "section" to it.section,
                                                    "score" to it.score,
                                                    "reasons" to it.reasonCodes,
                                                )
                                            },
                                        ),
                                    )
                                    Files.write(directory.resolve("$index.llms.txt"), result.bytes)
                                } catch (error: CrawlFailure) {
                                    report.putAll(mapOf("status" to "FAILED", "errorCode" to error.code, "error" to error.message))
                                }
                                report["requests"] = budget.requests
                                report["durationMillis"] = (System.nanoTime() - started) / 1000000
                                report["events"] = events
                                Files.writeString(directory.resolve("$index.json"), mapper.writeValueAsString(report))
                                println("$input ${report["status"]} durationMillis=${report["durationMillis"]} requests=${budget.requests}")
                            }
                        },
                    ).forEach { it.get() }
            } finally {
                pool.shutdownNow()
            }
        }
    } finally {
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName(dynamo.table).build())
        } finally {
            client.close()
        }
    }
}
