package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.api.query.AdmissionStatus
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.ingestion.submitAndProcess
import software.amazon.awssdk.services.dynamodb.model.*
import java.time.Clock

// Test-only CLI: its isolated table survives the normal container restart, then is removed.
fun main(args: Array<String>) {
    require(args.size == 1 && args[0] in setOf("write", "verify", "delete"))
    val configuration = DynamoConfiguration.fromEnvironment().apply {
        table = "llms-txt-m1-restart-${sha256(table).take(12)}"
    }
    configuration.requireLocal()
    val markerKey = DomainKeys.key("TEST#M1#RESTART", "META")
    DynamoClients.create(configuration).use { client ->
        val store = AdmissionStore(client, configuration)
        val settings = AdmissionConfiguration()
        val clock = Clock.systemUTC()
        val admission = AdmissionService(store, settings, UrlIdentityResolver(settings), clock)
        when (args[0]) {
            "write" -> {
                try {
                    client.describeTable(DescribeTableRequest.builder().tableName(configuration.table).build())
                    error("Restart verification table already exists. Inspect it before retrying; no data was removed.")
                } catch (_: ResourceNotFoundException) {
                    // Only create a new disposable test table.
                }
                LocalTableBootstrap(client, configuration).initialize()
                val batch = submitAndProcess(
                    admission,
                    store,
                    clock,
                    listOf("restart.example.com/docs", "https://restart.example.com/docs#section", "ftp://invalid.example"),
                    false,
                )
                store.putAbsent(
                    markerKey + mapOf(
                        "entityType" to text("M1RestartProbe"),
                        "itemSchemaVersion" to number(1),
                        "batchId" to text(batch.batchId),
                    ),
                )
                println("Wrote durable M1 admission in isolated restart verification table.")
            }

            "verify", "delete" -> {
                val marker = store.get(markerKey)
                require(
                    marker.ss("entityType") == "M1RestartProbe",
                ) { "Restart verification marker is missing; do not delete an unrecognized table." }
                if (args[0] == "verify") {
                    val result = AdmissionStatus(store, admission, clock).batch(marker.s("batchId"))
                    check(result.admissionComplete && result.status == "PROCESSING")
                    check(result.items.map { it.admission } == listOf("QUEUED", "COALESCED", "REJECTED"))
                    check(result.items[0].runId == result.items[1].runId)
                    val run = store.run(result.items[0].runId!!)!!
                    check(run.status == RunStatus.QUEUED && run.capacityReserved)
                    check(store.meta(run.identityId)!!.outstandingRunCount == 1L)
                    check(store.capacity() == Capacity(1, 1))
                    println("Verified batch, rows, run, identity, and quota persistence across restart.")
                } else {
                    client.deleteTable(DeleteTableRequest.builder().tableName(configuration.table).build())
                    println("Removed the isolated M1 restart verification table.")
                }
            }
        }
    }
}
