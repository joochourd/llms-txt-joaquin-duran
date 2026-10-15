package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.ingestion.sha256
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.results.ArtifactRead
import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

/** Test-only source changes independently; the real scheduler must discover every accepted update. */
fun main(args: Array<String>) {
    require(args.size == 1) { "Provide a temporary evidence directory." }
    val output = Path.of(args.single())
    Files.createDirectories(output)
    val dynamo = DynamoConfiguration.fromEnvironment()
    dynamo.requireLocal()
    dynamo.table = "llms-txt-monitor-update-${UUID.randomUUID()}"
    DynamoClients.create(dynamo).use { client ->
        try {
            LocalTableBootstrap(client, dynamo).initialize()
            val properties = mapOf<String, Any>(
                "dynamodb.environment" to "local",
                "dynamodb.endpoint" to dynamo.endpoint,
                "dynamodb.table" to dynamo.table,
                "dynamodb.work-index-shards" to dynamo.workIndexShards,
                "dynamodb.due-index-shards" to dynamo.dueIndexShards,
                "micronaut.server.port" to 0,
                "crawl.monitor-interval-seconds" to 15,
                "crawl.monitor-window-seconds" to 600,
                "crawl.scheduler-tick-seconds" to 1,
                "test-fixture.enabled" to true,
            )
            ApplicationContext.run(EmbeddedServer::class.java, properties).use { server ->
                val context = server.applicationContext
                val store = context.getBean(AdmissionStore::class.java)
                val admission = context.getBean(AdmissionService::class.java)
                val reader = context.getBean(CrawlResultReader::class.java)
                val batch = admission.submit(listOf("https://monitoring-fixture.example/"), false)
                val identityId = context
                    .getBean(com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver::class.java)
                    .normalize("https://monitoring-fixture.example/")
                    .identityId
                val started = System.nanoTime()
                val deadline = started + 210_000_000_000
                var lastSequence = 0L
                var firstHash: String? = null
                var changedHash: String? = null
                var changedSources: String? = null
                var contentSequence: Long? = null
                var contentSources: String? = null
                var changedFileAt: Long? = null
                var complete = false
                val evidence = StringBuilder("elapsedSeconds\tobservation\tfileRevision\toutputHash\tsourceFingerprint\n")
                while (System.nanoTime() < deadline && !complete) {
                    val metadata = store.meta(identityId)
                    if (metadata != null && metadata.hasCanonical() && metadata.acceptedObservationSequence > lastSequence) {
                        val accepted = reader.accepted(identityId)
                        val artifact = reader.latestArtifact(identityId) { false } as ArtifactRead.Downloaded
                        if (artifact.outputHash != accepted.metadata.currentArtifactHash) continue
                        lastSequence = accepted.metadata.acceptedObservationSequence
                        val sources = sha256(
                            accepted.resources.sortedBy { it.resourceKey }.joinToString("\n") {
                                "${it.resourceKey}:${it.contentHash}:${it.metadataHash}:${it.relevantLinksHash}"
                            },
                        )
                        check(sha256(artifact.bytes.toString(Charsets.UTF_8)) == artifact.outputHash)
                        val elapsed = (System.nanoTime() - started) / 1_000_000_000.0
                        evidence.append("$elapsed\t$lastSequence\t${accepted.metadata.currentVersionSequence}\t${artifact.outputHash}\t$sources\n")
                        Files.writeString(output.resolve("automatic-update.tsv"), evidence.toString())
                        when {
                            firstHash == null -> {
                                firstHash = artifact.outputHash
                                Files.write(output.resolve("A.llms.txt"), artifact.bytes)
                            }

                            changedHash == null && artifact.outputHash != firstHash -> {
                                changedHash = artifact.outputHash
                                changedSources = sources
                                changedFileAt = accepted.metadata.lastOutputUpdatedAt
                                Files.write(output.resolve("B.llms.txt"), artifact.bytes)
                            }

                            changedHash != null && artifact.outputHash == changedHash && sources != changedSources -> {
                                check(accepted.metadata.lastOutputUpdatedAt == changedFileAt)
                                check(artifact.bytes.contentEquals(Files.readAllBytes(output.resolve("B.llms.txt"))))
                                if (contentSequence == null) {
                                    contentSequence = lastSequence
                                    contentSources = sources
                                    Files.write(output.resolve("C.llms.txt"), artifact.bytes)
                                } else if (lastSequence > contentSequence && sources == contentSources) {
                                    complete = true
                                }
                            }
                        }
                        println("Automatic observation=$lastSequence outputHash=${artifact.outputHash} elapsedSeconds=$elapsed")
                    }
                    Thread.sleep(250)
                }
                check(complete) { "Automatic initial/file-change/content-only/unchanged flow did not complete within 210 seconds." }
                check(store.meta(identityId)?.outstandingRunCount == 0L)
                println("Automatic updates passed: changed file, changed source with identical file, then unchanged validation.")
            }
        } finally {
            client.deleteTable(DeleteTableRequest.builder().tableName(dynamo.table).build())
        }
    }
}
