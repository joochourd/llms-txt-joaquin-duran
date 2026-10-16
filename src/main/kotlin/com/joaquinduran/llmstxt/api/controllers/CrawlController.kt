package com.joaquinduran.llmstxt.api.controllers

import com.joaquinduran.llmstxt.api.downloads.DownloadNames
import com.joaquinduran.llmstxt.api.dto.BatchResponse
import com.joaquinduran.llmstxt.api.dto.CrawlResponse
import com.joaquinduran.llmstxt.api.dto.ResourceResponse
import com.joaquinduran.llmstxt.api.mapping.MonitoringView
import com.joaquinduran.llmstxt.api.mapping.dto
import com.joaquinduran.llmstxt.api.query.AdmissionStatus
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import com.joaquinduran.llmstxt.persistence.dynamo.CrawlResultReader
import com.joaquinduran.llmstxt.results.ArtifactRead
import io.micronaut.http.*
import io.micronaut.http.annotation.*
import io.micronaut.scheduling.annotation.ExecuteOn
import java.net.URI

@Controller
@ExecuteOn("api-io")
class CrawlController(
    private val reader: CrawlResultReader,
    private val admission: AdmissionService,
    private val status: AdmissionStatus,
    private val monitoring: MonitoringView,
) {
    @Get("/api/crawls/{identityId}")
    fun crawl(identityId: String): CrawlResponse {
        val accepted = reader.accepted(identityId)
        return accepted.dto(monitoring.response(accepted.metadata))
    }

    @Get("/api/crawls/{identityId}/resources")
    fun resources(identityId: String): List<ResourceResponse> = reader.accepted(identityId).resources.map { it.dto() }

    @Post("/api/crawls/{identityId}/refresh")
    fun refresh(identityId: String): HttpResponse<BatchResponse> {
        val metadata = reader.metadata(identityId)
        val batch = admission.submit(listOf(metadata.normalizedSeedUrl), true)
        val response = status.batch(batch.batchId)
        return HttpResponse
            .status<BatchResponse>(HttpStatus.ACCEPTED)
            .body(response)
            .headers { it.location(URI("/api/batches/${batch.batchId}")) }
    }

    @Get("/artifacts/{identityId}/llms.txt", produces = ["text/plain;charset=UTF-8"])
    fun latest(
        identityId: String,
        request: HttpRequest<*>,
    ): HttpResponse<ByteArray> = artifactResponse(reader.latestArtifact(identityId) { hash -> isCached(request, hash) })

    private fun isCached(
        request: HttpRequest<*>,
        hash: String,
    ): Boolean {
        val etag = "\"$hash\""
        return request.headers
            .get("If-None-Match")
            ?.split(',')
            ?.map(String::trim)
            ?.any { it == "*" || it.removePrefix("W/") == etag } == true
    }

    private fun artifactResponse(artifact: ArtifactRead): HttpResponse<ByteArray> {
        val etag = "\"${artifact.outputHash}\""
        return when (artifact) {
            is ArtifactRead.Cached -> {
                HttpResponse
                    .status<ByteArray>(HttpStatus.NOT_MODIFIED)
                    .header("ETag", etag)
                    .header("Cache-Control", "public, max-age=30")
            }

            is ArtifactRead.Downloaded -> {
                val fileName = DownloadNames.text(artifact.normalizedSeedUrl)
                HttpResponse
                    .ok(artifact.bytes)
                    .contentType("text/plain;charset=UTF-8")
                    .header("ETag", etag)
                    .header("Cache-Control", "public, max-age=30")
                    .header(
                        "Content-Disposition",
                        "attachment; filename=\"$fileName\"",
                    ).header("X-Content-Type-Options", "nosniff")
            }
        }
    }
}
