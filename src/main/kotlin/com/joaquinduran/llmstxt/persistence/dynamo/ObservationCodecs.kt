package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.crawl.model.CrawlResult
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.RunCoverage
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

internal fun resourceItem(r: Resource): Item = linkedMapOf(
    "resourceKey" to text(r.resourceKey),
    "sourceUrl" to text(r.sourceUrl),
    "primaryHtmlUrl" to text(r.primaryHtmlUrl),
    "preferredLinkUrl" to text(r.preferredLinkUrl),
    "linkFormat" to text(r.linkFormat),
    "title" to text(r.title),
    "description" to text(r.description),
    "contentHash" to text(r.contentHash),
    "metadataHash" to text(r.metadataHash),
    "relevantLinksHash" to text(r.relevantLinksHash),
    "category" to text(r.category),
    "section" to text(r.section),
    "reasonCodes" to strings(r.reasonCodes),
)

internal fun resource(item: Item) = Resource(
    item.s("resourceKey"),
    item.s("sourceUrl"),
    item.s("primaryHtmlUrl"),
    item.s("preferredLinkUrl"),
    item.s("linkFormat"),
    item.s("title"),
    item.s("description"),
    item.s("contentHash"),
    item.s("metadataHash"),
    item.s("relevantLinksHash"),
    item.s("category"),
    item.s("section"),
    item.list("reasonCodes"),
    item["aliases"]?.l()?.map { it.s() }.orEmpty(),
    item.ns("depth")?.toInt(),
    item["score"]?.n()?.toDouble() ?: 0.0,
)

internal fun resourceList(resources: List<Resource>) = AttributeValue
    .builder()
    .l(
        resources.map {
            AttributeValue.builder().m(resourceItem(it)).build()
        },
    ).build()

internal fun Item.resources() = this["manifest"]?.l().orEmpty().map { resource(it.m()) }

internal fun coverageItem(c: RunCoverage) = mapOf(
    "pagesAttempted" to number(c.pagesAttempted),
    "pagesUseful" to number(c.pagesUseful),
    "candidatesRetained" to number(c.candidatesRetained),
    "candidatesOmitted" to number(c.candidatesOmitted),
    "limitReasons" to strings(c.limitReasons),
    "discoverySources" to strings(c.discoverySources),
)

internal fun coverage(item: Item) = RunCoverage(
    item.n("pagesAttempted"),
    item.n("pagesUseful"),
    item.n("candidatesRetained"),
    item.n("candidatesOmitted"),
    item.list("limitReasons"),
    item.list("discoverySources"),
)

object ObservationKeys {
    fun check(
        id: String,
        sequence: Long,
    ) = DomainKeys.key("CRAWL#$id", "CHECK#${DomainKeys.ordered(sequence)}")

    fun artifact(
        id: String,
        hash: String,
    ) = DomainKeys.key("ARTIFACT#$id", "HASH#$hash")
}

/** Read-only compatibility for results accepted before current-result storage. */
internal object ArtifactCodec {
    fun bytes(item: Item): ByteArray {
        item.check("Artifact")
        return item.getValue("body").b().asByteArray()
    }
}

/** The accepted manifest and file change atomically with metadata and the terminal run. */
object CurrentResultCodec {
    fun key(identityId: String) = DomainKeys.key("CRAWL#$identityId", "RESULT")

    fun encode(
        run: CrawlRun,
        result: CrawlResult,
        outputHash: String,
        acceptedAt: Long,
        sourceChanged: Boolean,
    ): Item = base("CRAWL#${run.identityId}", "RESULT", "CurrentResult") + mapOf(
        "observationSequence" to number(requireNotNull(run.observationSequence)),
        "pipelineVersion" to text(run.pipelineVersion),
        "outputHash" to text(outputHash),
        "body" to AttributeValue.builder().b(SdkBytes.fromByteArray(result.bytes)).build(),
        "manifest" to resourceList(result.resources),
        "observedFrom" to number(result.observedFrom),
        "observedUntil" to number(result.observedUntil),
        "acceptedAt" to number(acceptedAt),
        "sourceChanged" to bool(sourceChanged),
        "context" to AttributeValue
            .builder()
            .m(
                mapOf(
                    "type" to text(result.context.type),
                    "confidence" to text(result.context.confidence),
                    "evidence" to strings(result.context.evidence),
                ),
            ).build(),
        "coverage" to AttributeValue.builder().m(coverageItem(result.coverage)).build(),
        "warnings" to strings(result.warnings),
    )

    fun bytes(item: Item): ByteArray {
        item.check("CurrentResult")
        return item.getValue("body").b().asByteArray()
    }

    internal fun snapshot(
        store: AdmissionStore,
        metadata: CrawlMeta,
    ): Item {
        val current = store.get(key(metadata.identityId))
        if (current.isNotEmpty()) {
            current.check("CurrentResult")
            if (current.n("observationSequence") != metadata.acceptedObservationSequence ||
                current.s("outputHash") != metadata.currentArtifactHash
            ) {
                throw StorageUnavailable()
            }
            return current
        }
        // Existing tables remain readable without migration or deleting old observations.
        val previous = store.get(ObservationKeys.check(metadata.identityId, metadata.acceptedObservationSequence))
        if (previous.ss("state") != "ACCEPTED" || previous["stagingComplete"]?.bool() != true) throw StorageUnavailable()
        previous.check("Snapshot")
        return previous
    }

    internal fun body(
        store: AdmissionStore,
        metadata: CrawlMeta,
        snapshot: Item,
    ): ByteArray = if (snapshot.ss("entityType") == "CurrentResult") {
        bytes(snapshot)
    } else {
        val hash = requireNotNull(metadata.currentArtifactHash)
        val artifact = store.get(ObservationKeys.artifact(metadata.identityId, hash))
        if (artifact.isEmpty()) throw StorageUnavailable()
        ArtifactCodec.bytes(artifact)
    }
}
