package com.joaquinduran.llmstxt.api.controllers

import com.joaquinduran.llmstxt.api.downloads.DownloadNames
import com.joaquinduran.llmstxt.api.dto.ApiError
import com.joaquinduran.llmstxt.api.dto.BatchDownloadItem
import com.joaquinduran.llmstxt.api.dto.BatchDownloadSummary
import com.joaquinduran.llmstxt.api.dto.BatchRowResponse
import com.joaquinduran.llmstxt.api.errors.ApiProblem
import com.joaquinduran.llmstxt.api.query.AdmissionStatus
import com.joaquinduran.llmstxt.api.query.BatchPage
import com.joaquinduran.llmstxt.persistence.dynamo.CrawlResultReader
import com.joaquinduran.llmstxt.results.BatchArtifact
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.QueryValue
import io.micronaut.json.JsonMapper
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.serde.annotation.Serdeable
import java.io.ByteArrayOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

@Controller("/api/batches")
@ExecuteOn("api-io")
class BatchDownloadController(
    private val reader: CrawlResultReader,
    private val status: AdmissionStatus,
    private val mapper: JsonMapper,
) {
    @Get("/{batchId}/download", produces = ["application/zip"])
    fun download(
        batchId: String,
        @QueryValue(defaultValue = "0") offset: Int = 0,
        @QueryValue(defaultValue = "100") limit: Int = BatchPage.MAX_PAGE_SIZE,
    ): HttpResponse<ByteArray> {
        val page = BatchPage(offset = offset, limit = limit)
        val batch = status.batch(batchId, page)
        if (batch.items.any { it.status !in setOf("COMPLETED", "FAILED", "TIMED_OUT", "EXPIRED") }) {
            throw ApiProblem(409, "BATCH_NOT_READY", "This result page is still processing. Please retry its download later.")
        }
        if (batch.items.none { it.artifactUrl != null }) throw ApiProblem(409, "NO_BATCH_RESULTS", "This batch has no downloadable results.")
        val output = ByteArrayOutputStream()
        val used = mutableSetOf<String>()
        ZipOutputStream(output).use { zip ->
            val items = batch.items.map { row ->
                val artifact = if (row.artifactUrl != null) artifact(row) else null
                val fileName = artifact?.let {
                    val baseName = DownloadNames.text(it.seedUrl)
                    val positionedName = if (batch.totalItems > page.limit) {
                        "${baseName.removeSuffix(".txt")}-row-${row.index + 1}.txt"
                    } else {
                        baseName
                    }
                    DownloadNames.unique(positionedName, used)
                }
                if (artifact != null) entry(zip, requireNotNull(fileName), artifact.bytes)
                BatchDownloadItem(
                    index = row.index,
                    inputUrl = row.inputUrl,
                    status = row.status,
                    fileName = fileName,
                    fileRevision = artifact?.sequence,
                    outputHash = artifact?.hash,
                    error = row.error,
                )
            }
            val summary = BatchDownloadSummary(
                batchId = batch.batchId,
                items = items,
                totalItems = batch.totalItems,
                offset = page.offset,
                nextOffset = batch.nextOffset,
            )
            entry(zip, "batch-summary.json", mapper.writeValueAsBytes(summary))
        }
        val prefix = "llms-batch-${batch.batchId.take(8)}"
        val archiveName = if (page.offset == 0 && batch.nextOffset == null) {
            "$prefix.zip"
        } else {
            "$prefix-${page.offset + 1}-${page.end(batch.totalItems)}.zip"
        }
        return HttpResponse
            .ok(output.toByteArray())
            .contentType("application/zip")
            .header("Content-Disposition", "attachment; filename=\"$archiveName\"")
            .header("Cache-Control", "no-store")
            .header("X-Content-Type-Options", "nosniff")
    }

    private fun artifact(row: BatchRowResponse): BatchArtifact = reader.batchArtifact(
        identityId = requireNotNull(row.identityId),
    )

    private fun entry(
        zip: ZipOutputStream,
        name: String,
        bytes: ByteArray,
    ) {
        zip.putNextEntry(ZipEntry(name))
        zip.write(bytes)
        zip.closeEntry()
    }
}
