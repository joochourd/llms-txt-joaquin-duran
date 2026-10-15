package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.sha256
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.results.ArtifactRead
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

@Tag("integration")
class CurrentResultIntegrationTest : MonitoringFixture() {
    @Test
    fun `repeated publication keeps one current result without snapshot version or observation rows`() {
        config.monitoring.monitorIntervalSeconds = 15
        val first = canonical()
        repeat(12) { index ->
            val run = requireNotNull(execution.claim(due(first.identityId)))
            assertEquals(PublicationDecision.UPDATED, publish(run, result(run, "description $index")).publicationDecision)
        }

        val rows = client
            .query(
                QueryRequest
                    .builder()
                    .tableName(store.table)
                    .keyConditionExpression("pk = :pk")
                    .expressionAttributeValues(mapOf(":pk" to text("CRAWL#${first.identityId}")))
                    .consistentRead(true)
                    .build(),
            ).items()
        assertEquals(setOf("META", "RESULT"), rows.map { it.s("sk") }.toSet())
        val current = CrawlResultReader(store).accepted(first.identityId)
        assertEquals(13L, current.metadata.acceptedObservationSequence)
        assertEquals(13L, current.metadata.currentVersionSequence)
        assertEquals(1, current.resources.size)
        assertEquals(sha256("description 11"), current.resources.single().metadataHash)
    }

    @Test
    fun `previously accepted snapshot and artifact remain readable and upgrade only on new acceptance`() {
        val first = canonical()
        val currentKey = CurrentResultCodec.key(first.identityId)
        val current = store.get(currentKey)
        val legacySnapshot = base("CRAWL#${first.identityId}", "CHECK#${DomainKeys.ordered(1)}", "Snapshot") +
            current.filterKeys { it in setOf("observationSequence", "manifest", "context", "coverage", "warnings") } +
            mapOf("state" to text("ACCEPTED"), "stagingComplete" to bool(true))
        val hash = requireNotNull(first.currentArtifactHash)
        val legacyArtifact = base("ARTIFACT#${first.identityId}", "HASH#$hash", "Artifact") + mapOf("body" to current.getValue("body"))
        assertTrue(store.putAbsent(legacySnapshot))
        assertTrue(store.putAbsent(legacyArtifact))
        client.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(store.table)
                .key(currentKey)
                .build(),
        )

        val reader = CrawlResultReader(store)
        assertEquals(first.acceptedObservationSequence, reader.accepted(first.identityId).metadata.acceptedObservationSequence)
        val downloaded = reader.latestArtifact(first.identityId) { false } as ArtifactRead.Downloaded
        assertArrayEquals(CurrentResultCodec.bytes(current), downloaded.bytes)
        assertEquals(current.resources().single().contentHash, execution.baseline(first).single().contentHash)
        assertTrue(store.get(currentKey).isEmpty(), "Read compatibility must not rewrite stored data.")

        val run = requireNotNull(execution.claim(due(first.identityId)))
        assertEquals(PublicationDecision.UPDATED, publish(run, result(run, "new output")).publicationDecision)
        assertEquals(run.observationSequence, store.get(currentKey).n("observationSequence"))
        assertEquals(legacySnapshot, store.get(ObservationKeys.check(first.identityId, 1)))
        assertEquals(legacyArtifact, store.get(ObservationKeys.artifact(first.identityId, hash)))
    }

    @Test
    fun `batch downloads resolve the current file even when the batch observed an older revision`() {
        val first = canonical()
        val run = requireNotNull(execution.claim(due(first.identityId)))
        val updated = result(run, "new current file")
        publish(run, updated)

        val downloaded = CrawlResultReader(store).batchArtifact(first.identityId)
        assertEquals(run.observationSequence, downloaded.sequence)
        assertArrayEquals(updated.bytes, downloaded.bytes)
    }
}
