package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.api.query.AdmissionStatus
import com.joaquinduran.llmstxt.api.query.BatchPage
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.ingestion.BulkBatchProcessor
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.ingestion.sha256
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import java.time.Clock

/** Owns only its recognized Local table. Inputs are invalid FTP URLs and never cause external requests. */
fun main(args: Array<String>) {
    require(args.size == 1 && args[0] in setOf("write", "verify", "delete"))
    val configuration = DynamoConfiguration.fromEnvironment().apply {
        table = "llms-txt-bulk-restart-${sha256(table).take(12)}"
    }
    configuration.requireLocal()
    val markerKey = DomainKeys.key("TEST#BULK#RESTART", "META")
    val inputs = (0 until 1000).map { "ftp://invalid.example/input-$it" }
    val clock = Clock.systemUTC()
    DynamoClients.create(configuration).use { client ->
        val store = AdmissionStore(client, configuration)
        val settings = AdmissionConfiguration()
        val admission = AdmissionService(store, settings, UrlIdentityResolver(settings), clock)
        val batches = BulkBatchStore(store, clock)
        when (args[0]) {
            "write" -> {
                try {
                    client.describeTable(DescribeTableRequest.builder().tableName(configuration.table).build())
                    error("Bulk restart table exists. Inspect it before retrying; no data was removed.")
                } catch (_: ResourceNotFoundException) {
                    // Create only a fresh owned table.
                }
                LocalTableBootstrap(client, configuration).initialize()
                val batch = admission.submit(inputs, false)
                BulkBatchProcessor(store, batches, admission, clock).use { it.process(batch.batchId) }
                check(store.batch(batch.batchId)?.nextAdmissionIndex == 120)
                check(store.putAbsent(markerKey + mapOf("entityType" to text("BulkRestartProbe"), "batchId" to text(batch.batchId))))
                println("Persisted 1000 inputs with a partial admission cursor and an active bulk reservation.")
            }

            "verify", "delete" -> {
                val marker = store.get(markerKey)
                require(marker.ss("entityType") == "BulkRestartProbe") { "Refusing an unrecognized bulk restart table." }
                if (args[0] == "delete") {
                    client.deleteTable(DeleteTableRequest.builder().tableName(configuration.table).build())
                    println("Removed the owned bulk restart verification table.")
                    return
                }
                val id = marker.s("batchId")
                val restored = requireNotNull(store.batch(id))
                check(restored.received && restored.bulkSlotReserved && restored.nextAdmissionIndex == 120)
                check(batches.inputs(restored, 0, 1000) == inputs)
                check(store.get(DomainKeys.key("CAPACITY#BULK", "META")).n("activeBatches") == 1L)
                BulkBatchProcessor(store, batches, admission, clock).use { processor ->
                    repeat(30) { processor.process(id) }
                }
                val completed = requireNotNull(store.batch(id))
                check(completed.admissionComplete && completed.nextCompletionIndex == 1000 && !completed.bulkSlotReserved)
                check(store.capacity() == Capacity(0, 0))
                check(store.get(DomainKeys.key("CAPACITY#BULK", "META")).n("activeBatches") == 0L)
                val page = AdmissionStatus(store, admission, clock).batch(id, BatchPage(offset = 900, limit = 100))
                check(page.status == "COMPLETED" && page.totalItems == 1000 && page.items.size == 100)
                check(page.items.map { it.inputUrl } == inputs.takeLast(100))
                println("Verified all bulk inputs, cursor recovery, final page and exactly-once backlog release after restart.")
            }
        }
    }
}
