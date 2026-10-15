package com.joaquinduran.llmstxt.ingestion

import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.BulkBatchStore
import java.time.Clock

/** Advance asynchronous admission explicitly when a test needs a run, rather than a receipt. */
fun submitAndProcess(
    service: AdmissionService,
    store: AdmissionStore,
    clock: Clock,
    inputs: List<String>,
    force: Boolean,
): Batch {
    val receipt = service.submit(inputs, force)
    BulkBatchProcessor(store, BulkBatchStore(store, clock), service, clock).use { it.process(receipt.batchId) }
    return requireNotNull(store.batch(receipt.batchId))
}
