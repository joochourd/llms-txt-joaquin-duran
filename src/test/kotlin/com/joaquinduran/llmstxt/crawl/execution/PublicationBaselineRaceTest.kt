package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.monitoring.MonitoringFixture
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.CurrentResultCodec
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.persistence.dynamo.ExecutionStore
import com.joaquinduran.llmstxt.persistence.dynamo.ObservationKeys
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest

@Tag("integration")
class PublicationBaselineRaceTest : MonitoringFixture() {
    @Test
    fun `CAS loser revalidates newly accepted resources before publishing a higher sequence`() {
        val initial = canonical()
        val writerB = requireNotNull(execution.claim(due(initial.identityId)))
        // Hints may be stale or absent: concurrent durable runs remain valid publication contenders.
        update(DomainKeys.crawl(initial.identityId), "REMOVE activeRunIdHint, activeHintExpiresAt")
        val publisherA = requireNotNull(execution.claim(queued(initial.normalizedSeedUrl)))
        val sequenceA = requireNotNull(publisherA.observationSequence)
        val sequenceB = requireNotNull(writerB.observationSequence)
        assertTrue(sequenceA > sequenceB, "A must need baseline validation rather than simple supersession.")

        val additionalUrl = "${initial.normalizedSeedUrl}additional"
        val originalB = result(writerB, "Writer B baseline")
        val additional = originalB.resources.single().copy(
            resourceKey = sha256(additionalUrl),
            sourceUrl = additionalUrl,
            primaryHtmlUrl = additionalUrl,
            preferredLinkUrl = additionalUrl,
            title = "Additional resource",
            contentHash = sha256("Additional content"),
            metadataHash = sha256("Additional metadata"),
            section = "Resources",
            aliases = listOf(additionalUrl),
            depth = 1,
        )
        val resultB = originalB.copy(
            resources = originalB.resources + additional,
            checks = originalB.checks + (additionalUrl to "VALID"),
            bytes = (
                originalB.bytes.toString(Charsets.UTF_8) +
                    "\n## Resources\n\n- [Additional resource]($additionalUrl)\n"
            ).toByteArray(),
            coverage = originalB.coverage.copy(pagesAttempted = 2, pagesUseful = 2, candidatesRetained = 2),
        )
        val resultA = result(publisherA, "Publisher A output")
        assertFalse(additionalUrl in resultA.checks)
        val hashB = sha256(resultB.bytes.toString(Charsets.UTF_8))
        val currentKey = CurrentResultCodec.key(initial.identityId)
        val baselineReads = mutableListOf<Long>()
        var injected = false
        val wrapped = proxy { name, args, delegate ->
            if (name == "getItem" && (args[0] as GetItemRequest).key() == currentKey) {
                val response = delegate() as software.amazon.awssdk.services.dynamodb.model.GetItemResponse
                baselineReads.add(response.item().n("observationSequence"))
                response
            } else {
                if (name == "transactWriteItems" && !injected &&
                    (args[0] as TransactWriteItemsRequest).transactItems().any {
                        it.put()?.item()?.ss("sk") == "RESULT"
                    }
                ) {
                    assertEquals(listOf(initial.acceptedObservationSequence), baselineReads)
                    injected = true
                    // A already read the old accepted baseline; B commits immediately before A's CAS.
                    assertEquals(PublicationDecision.UPDATED, execution.publish(writerB, resultB).publicationDecision)
                }
                delegate()
            }
        }
        val racing = ExecutionStore(AdmissionStore(wrapped, store.configuration), settings, config, clock)
        val terminalA = racing.publish(publisherA, resultA)

        assertTrue(injected)
        assertEquals(RunStatus.FAILED, terminalA.status)
        assertEquals(PublicationDecision.PRESERVED, terminalA.publicationDecision)
        assertEquals("INCOMPLETE_BASELINE", terminalA.error?.code)
        assertEquals(listOf(initial.acceptedObservationSequence, sequenceB), baselineReads)
        val current = requireNotNull(store.meta(initial.identityId))
        assertEquals(sequenceB, current.acceptedObservationSequence)
        assertEquals(sequenceB, current.currentVersionSequence)
        assertEquals(hashB, current.currentArtifactHash)
        assertTrue(execution.baseline(current).any { it.sourceUrl == additionalUrl })
        val acceptedResult = store.get(currentKey)
        assertArrayEquals(resultB.bytes, CurrentResultCodec.bytes(acceptedResult))
        assertEquals(sequenceB, acceptedResult.n("observationSequence"))
        assertEquals(0L, current.outstandingRunCount)
        assertEquals(
            com.joaquinduran.llmstxt.ingestion
                .Capacity(0, 1),
            store.capacity(),
        )
    }
}
