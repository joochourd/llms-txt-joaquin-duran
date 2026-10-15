package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.crawl.model.SeedContext
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.DomainError
import com.joaquinduran.llmstxt.results.AcceptedCrawl
import com.joaquinduran.llmstxt.results.ArtifactRead
import com.joaquinduran.llmstxt.results.BatchArtifact
import com.joaquinduran.llmstxt.results.ResultReadException
import jakarta.inject.Singleton

/** Strong pointer rereads resolve concurrent publication; reads never start crawls or change durable state. */
@Singleton
class CrawlResultReader(
    private val store: AdmissionStore,
) {
    fun metadata(identityId: String): CrawlMeta = store.meta(identityId) ?: missing("NOT_FOUND", "Crawl not found.")

    fun accepted(identityId: String): AcceptedCrawl {
        repeat(MAX_POINTER_READS) {
            val metadata = metadata(identityId)
            if (!metadata.hasCanonical()) return AcceptedCrawl(metadata)
            try {
                val snapshot = CurrentResultCodec.snapshot(store, metadata)
                return AcceptedCrawl(
                    metadata = metadata,
                    context = snapshot["context"]?.m()?.let { SeedContext(it.s("type"), it.s("confidence"), it.list("evidence")) },
                    coverage = snapshot["coverage"]?.m()?.let(::coverage),
                    warnings = snapshot["warnings"]?.l()?.map { it.s() }.orEmpty(),
                    resources = snapshot.resources(),
                    sourceChanged = snapshot["sourceChanged"]?.bool(),
                )
            } catch (_: StorageUnavailable) {
                // Publication may replace RESULT between the metadata and result reads.
            }
        }
        throw StorageUnavailable()
    }

    fun latestArtifact(
        identityId: String,
        isCached: (String) -> Boolean,
    ): ArtifactRead {
        repeat(MAX_POINTER_READS) {
            val metadata = metadata(identityId)
            val hash = metadata.currentArtifactHash ?: missing("NO_RESULT", "No accepted result is available yet.")
            if (isCached(hash)) return ArtifactRead.Cached(hash, metadata.normalizedSeedUrl, metadata.currentVersionSequence)
            try {
                val snapshot = CurrentResultCodec.snapshot(store, metadata)
                return ArtifactRead.Downloaded(
                    outputHash = hash,
                    normalizedSeedUrl = metadata.normalizedSeedUrl,
                    versionSequence = metadata.currentVersionSequence,
                    bytes = CurrentResultCodec.body(store, metadata, snapshot),
                )
            } catch (_: StorageUnavailable) {
                // Retry the whole pointer read rather than returning mismatched bytes or metadata.
            }
        }
        throw StorageUnavailable()
    }

    /** Batch downloads deliberately use the current file; batches no longer pin historical versions. */
    fun batchArtifact(identityId: String): BatchArtifact {
        val current = latestArtifact(identityId) { false } as ArtifactRead.Downloaded
        return BatchArtifact(
            seedUrl = current.normalizedSeedUrl,
            sequence = requireNotNull(current.versionSequence),
            hash = current.outputHash,
            bytes = current.bytes,
        )
    }

    private fun missing(
        code: String,
        message: String,
    ): Nothing = throw ResultReadException(DomainError(code, message))

    companion object {
        private const val MAX_POINTER_READS = 3
    }
}
