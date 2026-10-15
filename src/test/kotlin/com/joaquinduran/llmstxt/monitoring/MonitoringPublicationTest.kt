package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.DomainError
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.submitAndProcess
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.CurrentResultCodec
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.ExecutionStore
import com.joaquinduran.llmstxt.persistence.dynamo.ObservationKeys
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest

@Tag("integration")
class MonitoringPublicationTest : MonitoringFixture() {
    @Test
    fun `unchanged scheduled success advances source validation but preserves file revision bytes and output date`() {
        val first = canonical()
        val run = requireNotNull(execution.claim(due(first.identityId)))
        assertEquals(PublicationDecision.UNCHANGED, publish(run).publicationDecision)
        val current = requireNotNull(store.meta(first.identityId))
        assertEquals(run.observationSequence, current.acceptedObservationSequence)
        assertEquals(first.currentVersionSequence, current.currentVersionSequence)
        assertEquals(first.currentArtifactHash, current.currentArtifactHash)
        assertEquals(first.lastOutputUpdatedAt, current.lastOutputUpdatedAt)
        assertTrue(requireNotNull(current.lastSuccessfulValidationAt) > requireNotNull(first.lastSuccessfulValidationAt))
        val acceptedResult = store.get(CurrentResultCodec.key(first.identityId))
        assertEquals(run.observationSequence, acceptedResult.n("observationSequence"))
        assertEquals(first.currentArtifactHash, acceptedResult.s("outputHash"))
        assertTrue(requireNotNull(current.nextCheckAt) >= clock.now + config.monitoring.monitorIntervalSeconds * 1000)
        assertEquals(Capacity(0, 1), store.capacity())
    }

    @Test
    fun `body changes update current source hashes separately from rendered file changes`() {
        val first = canonical()
        val originalContentHash = execution.baseline(first).single().contentHash
        val contentRun = requireNotNull(execution.claim(due(first.identityId)))
        val changedContent = result(contentRun, "first", "new body")
        assertEquals(PublicationDecision.UNCHANGED, publish(contentRun, changedContent).publicationDecision)
        val contentMeta = requireNotNull(store.meta(first.identityId))
        assertEquals(first.currentVersionSequence, contentMeta.currentVersionSequence)
        assertNotEquals(originalContentHash, execution.baseline(contentMeta).single().contentHash)
        assertEquals(sha256("new body"), execution.baseline(contentMeta).single().contentHash)
        assertEquals(true, CrawlResultReader(store).accepted(first.identityId).sourceChanged)

        val repeated = requireNotNull(execution.claim(due(first.identityId)))
        assertEquals(PublicationDecision.UNCHANGED, publish(repeated, result(repeated, "first", "new body")).publicationDecision)
        assertEquals(false, CrawlResultReader(store).accepted(first.identityId).sourceChanged)

        val next = requireNotNull(execution.claim(due(first.identityId)))
        assertEquals(PublicationDecision.UPDATED, publish(next, result(next, "new description", "new body")).publicationDecision)
        val current = requireNotNull(store.meta(first.identityId))
        assertEquals(next.observationSequence, current.currentVersionSequence)
        assertEquals(sha256("new body"), execution.baseline(current).single().contentHash)
        assertEquals(sha256("new description"), execution.baseline(current).single().metadataHash)
    }

    @Test
    fun `failed automatic validation preserves last good freshness and current evidence and backs off`() {
        val first = canonical()
        val run = requireNotNull(execution.claim(due(first.identityId)))
        execution.finish(
            run.runId,
            RunStatus.FAILED,
            PublicationDecision.PRESERVED,
            DomainError("INCOMPLETE_BASELINE", "A selected page could not be validated."),
        )
        val failed = requireNotNull(store.meta(first.identityId))
        assertEquals(first.acceptedObservationSequence, failed.acceptedObservationSequence)
        assertEquals(first.acceptedObservationStartedAt, failed.acceptedObservationStartedAt)
        assertEquals(first.lastSuccessfulValidationAt, failed.lastSuccessfulValidationAt)
        assertEquals(first.currentArtifactHash, failed.currentArtifactHash)
        assertEquals(1L, failed.failureCount)
        assertEquals(clock.now + 900_000, failed.nextCheckAt)
        assertEquals(first.acceptedObservationSequence, store.get(CurrentResultCodec.key(first.identityId)).n("observationSequence"))
        assertEquals(run.runId, failed.lastMonitoringRunId)
        val retry = requireNotNull(execution.claim(due(first.identityId)))
        execution.finish(retry.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)
        assertEquals(clock.now + 1_800_000, store.meta(first.identityId)?.nextCheckAt)
        val success = requireNotNull(execution.claim(due(first.identityId)))
        publish(success)
        assertEquals(0L, store.meta(first.identityId)?.failureCount)
    }

    private fun duplicate(id: String): CrawlRun {
        update(DomainKeys.crawl(id), "REMOVE activeRunIdHint, activeHintExpiresAt")
        val batch = submitAndProcess(service, store, clock, listOf(requireNotNull(store.meta(id)).normalizedSeedUrl), true)
        return requireNotNull(execution.claim(requireNotNull(store.run(requireNotNull(store.rows(batch.batchId, true).single().runId)))))
    }

    @Test
    fun `an older monitoring failure cannot replace newer unchanged acceptance or its schedule`() {
        val first = canonical()
        val old = requireNotNull(execution.claim(due(first.identityId)))
        val recent = duplicate(first.identityId)
        publish(recent)
        val before = requireNotNull(store.meta(first.identityId))
        execution.finish(old.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)
        val after = requireNotNull(store.meta(first.identityId))
        assertEquals(before.nextCheckAt, after.nextCheckAt)
        assertEquals(before.failureCount, after.failureCount)
        assertEquals(before.lastAttemptRunId, after.lastAttemptRunId)
        assertEquals(before.lastAttemptSummary, after.lastAttemptSummary)
        assertEquals(Capacity(0, 1), store.capacity())
    }

    @Test
    fun `newer failure permits an older sufficient automatic observation and expired monitoring stays expired`() {
        val first = canonical()
        val old = requireNotNull(execution.claim(due(first.identityId)))
        val recent = duplicate(first.identityId)
        execution.finish(recent.runId, RunStatus.FAILED, PublicationDecision.PRESERVED)
        assertEquals(PublicationDecision.UNCHANGED, publish(old).publicationDecision)
        assertEquals(recent.runId, store.meta(first.identityId)?.lastAttemptRunId)
        val queued = due(first.identityId)
        monitoring.change(first.identityId, false)
        val manual = requireNotNull(execution.claim(queued))
        publish(manual, result(manual, "changed"))
        assertEquals(MonitoringState.NONE, store.meta(first.identityId)?.monitoringState)
        assertNull(store.meta(first.identityId)?.nextCheckAt)
        assertEquals(Capacity(0, 0), store.capacity())
    }

    @Test
    fun `queued failure callback racing a newer start in the same millisecond preserves its schedule`() {
        val first = canonical()
        val old = due(first.identityId)
        update(DomainKeys.crawl(first.identityId), "REMOVE activeRunIdHint, activeHintExpiresAt")
        val batch = submitAndProcess(service, store, clock, listOf(first.normalizedSeedUrl), true)
        val queued = requireNotNull(store.run(requireNotNull(store.rows(batch.batchId, true).single().runId)))
        val before = requireNotNull(store.meta(first.identityId))
        var claimed = false
        val wrapped = proxy { name, args, delegate ->
            if (name == "transactWriteItems" && !claimed && (args[0] as TransactWriteItemsRequest).transactItems().any {
                    it.update()?.key() == DomainKeys.run(old.runId)
                }
            ) {
                assertNotNull(execution.claim(queued))
                claimed = true
            }
            delegate()
        }
        ExecutionStore(AdmissionStore(wrapped, store.configuration), settings, config, clock)
            .finish(old.runId, RunStatus.TIMED_OUT, PublicationDecision.PRESERVED)
        assertTrue(claimed)
        val after = requireNotNull(store.meta(first.identityId))
        assertEquals(before.nextCheckAt, after.nextCheckAt)
        assertEquals(before.failureCount, after.failureCount)
        assertEquals(queued.runId, after.lastAttemptRunId)
        assertEquals(Capacity(1, 1), store.capacity())
    }
}
