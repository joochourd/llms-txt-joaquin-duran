package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.CrawlResultReader
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import com.joaquinduran.llmstxt.results.ArtifactRead
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest

@Tag("integration")
class CanonicalReadRaceTest : MonitoringFixture() {
    @Test
    fun `current file rereads metadata when publication replaces the result between reads`() {
        val (reader, identityId, expected) = racingReader()
        val response = reader.latestArtifact(identityId) { false } as ArtifactRead.Downloaded
        assertEquals(2L, response.versionSequence)
        assertArrayEquals(expected, response.bytes)
    }

    @Test
    fun `current evidence rereads metadata when publication replaces the accepted result`() {
        val (reader, identityId, _) = racingReader()
        val response = reader.accepted(identityId)
        assertEquals(2L, response.metadata.acceptedObservationSequence)
        assertEquals(2L, response.metadata.currentVersionSequence)
        assertEquals(1, response.resources.size)
        assertNotNull(response.context)
    }

    private fun racingReader(): Triple<CrawlResultReader, String, ByteArray> {
        val first = canonical()
        val running = requireNotNull(execution.claim(due(first.identityId)))
        val changed = result(running, "changed")
        var raced = false
        val wrapped = AdmissionStore(
            proxy { method, arguments, invoke ->
                val request = arguments.firstOrNull() as? GetItemRequest
                if (!raced && method == "getItem" && request?.key() == DomainKeys.crawl(first.identityId)) {
                    val previous = invoke()
                    raced = true
                    execution.publish(running, changed)
                    previous
                } else {
                    invoke()
                }
            },
            store.configuration,
        )
        return Triple(CrawlResultReader(wrapped), first.identityId, changed.bytes)
    }
}
