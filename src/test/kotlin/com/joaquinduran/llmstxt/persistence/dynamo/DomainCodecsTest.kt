package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.Admission
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.Batch
import com.joaquinduran.llmstxt.ingestion.BatchItem
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.DomainError
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.PublicationDecision
import com.joaquinduran.llmstxt.ingestion.RunStatus
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DomainCodecsTest {
    @Test
    fun `empty META has explicit absence of canonical and zero counters`() {
        val identity = UrlIdentityResolver(AdmissionConfiguration()).normalize("example.com/docs")
        val meta = CrawlMeta(identity.identityId, identity.normalizedSeedUrl, identity.intentProfile, "v1")
        val encoded = CrawlMetaCodec.encode(meta)
        assertEquals(meta, CrawlMetaCodec.decode(encoded))
        assertFalse(encoded.containsKey("currentVersionSequence"))
        assertFalse(meta.hasCanonical())
        assertEquals("0", encoded["acceptedObservationSequence"]?.n())
        assertEquals(
            meta.copy(
                metadataRevision = 3,
                acceptedObservationSequence = 2,
                acceptedPipelineVersion = "v0",
                currentVersionSequence = 1,
                currentArtifactHash = "abc",
                monitoringState = MonitoringState.EXPIRED,
                dueShard = 1,
                duePk = "DUE#1",
                dueSk = "00000000000000000100#id",
            ),
            CrawlMetaCodec.decode(
                CrawlMetaCodec.encode(
                    meta.copy(
                        metadataRevision = 3,
                        acceptedObservationSequence = 2,
                        acceptedPipelineVersion = "v0",
                        currentVersionSequence = 1,
                        currentArtifactHash = "abc",
                        monitoringState = MonitoringState.EXPIRED,
                        dueShard = 1,
                        duePk = "DUE#1",
                        dueSk = "00000000000000000100#id",
                    ),
                ),
            ),
        )
    }

    @Test
    fun `run and batch decisions round trip with lossless numbers and nullable outcomes`() {
        val run = CrawlRun(
            "r",
            "i",
            "https://example.com/",
            "v1",
            "https://example.com",
            100,
            700,
            100,
            1,
            "WORK#QUEUED#1",
            "00000000000000000100#r",
            AdmissionConfiguration().budgets(),
        )
        assertEquals(run, RunCodec.decode(RunCodec.encode(run)))
        val terminal = run.copy(
            status = RunStatus.COMPLETED,
            capacityReserved = false,
            workPk = null,
            workSk = null,
            observationSequence = 9_007_199_254_740_993,
            publicationDecision = PublicationDecision.UNCHANGED,
            warnings = listOf("LIMIT_REACHED"),
        )
        assertEquals(terminal, RunCodec.decode(RunCodec.encode(terminal)))
        val batch = Batch("b", listOf(" https://example.com/ ", "invalid"), true, 100, 200, 300)
        assertEquals(batch, BatchCodec.decode(BatchCodec.encode(batch)))
        val row = BatchItem("b", 1, "invalid", Admission.REJECTED, error = DomainError("INVALID_URL", "Invalid URL."), expiresAtEpochSeconds = 300)
        assertEquals(row, BatchItemCodec.decode(BatchItemCodec.encode(row)))
        assertTrue(DomainKeys.ordered(9) < DomainKeys.ordered(10))
    }

    @Test
    fun `codecs fail closed on incompatible schema and missing capacity`() {
        assertThrows(IllegalArgumentException::class.java) { CapacityCodec.decode(emptyMap()) }
        assertThrows(IllegalArgumentException::class.java) {
            BatchCodec.decode(BatchCodec.encode(Batch("b", listOf("url"), false, 1, 2, 3)) + ("itemSchemaVersion" to number(2)))
        }
        assertThrows(IllegalArgumentException::class.java) { AdmissionConfiguration().apply { maxBatchUrls = 1001 }.validate() }
    }
}
