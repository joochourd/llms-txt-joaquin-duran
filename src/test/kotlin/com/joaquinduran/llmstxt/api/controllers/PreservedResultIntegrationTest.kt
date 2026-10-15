package com.joaquinduran.llmstxt.api.controllers

import com.joaquinduran.llmstxt.api.query.AdmissionStatus
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class PreservedResultIntegrationTest : MonitoringFixture() {
    @Test
    fun `failed refresh exposes the accepted file while retaining the failed outcome and cause`() {
        val previous = canonical()
        clock.now += 61_000
        val batch = submitAndProcess(service, store, clock, listOf(previous.normalizedSeedUrl), true)
        val run = requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(store.rows(batch.batchId, true).single().runId)))))
        execution.finish(run.runId, RunStatus.FAILED, PublicationDecision.PRESERVED, DomainError("FETCH_FAILED", "A prior source was unavailable."))

        val row = AdmissionStatus(store, service, clock).batch(batch.batchId).items.single()

        assertEquals("FAILED", row.status)
        assertEquals("FETCH_FAILED", row.error?.code)
        assertEquals("/artifacts/${previous.identityId}/llms.txt", row.artifactUrl)
        assertEquals(previous.currentArtifactHash, row.outputHash)
        assertEquals(previous.currentVersionSequence, row.versionSequence)
        assertEquals(previous.acceptedObservationSequence, store.meta(previous.identityId)?.acceptedObservationSequence)
    }
}
