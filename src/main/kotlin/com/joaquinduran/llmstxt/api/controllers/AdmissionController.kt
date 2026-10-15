package com.joaquinduran.llmstxt.api.controllers

import com.joaquinduran.llmstxt.api.dto.BatchRequest
import com.joaquinduran.llmstxt.api.dto.BatchResponse
import com.joaquinduran.llmstxt.api.dto.RunResponse
import com.joaquinduran.llmstxt.api.limits.ApiRateLimiter
import com.joaquinduran.llmstxt.api.limits.ClientIpResolver
import com.joaquinduran.llmstxt.api.mapping.dto
import com.joaquinduran.llmstxt.api.query.AdmissionStatus
import com.joaquinduran.llmstxt.api.query.BatchPage
import com.joaquinduran.llmstxt.ingestion.AdmissionService
import io.micronaut.http.*
import io.micronaut.http.annotation.*
import io.micronaut.json.tree.JsonNode
import io.micronaut.scheduling.annotation.ExecuteOn
import java.net.URI

@Controller("/api")
@ExecuteOn("api-io")
class AdmissionController(
    private val admission: AdmissionService,
    private val status: AdmissionStatus,
    private val rates: ApiRateLimiter,
    private val clients: ClientIpResolver,
) {
    @Post("/batches")
    fun submit(
        @Body body: JsonNode,
        request: HttpRequest<*>,
    ): HttpResponse<BatchResponse> {
        // Validate JSON types explicitly; string coercion and null list elements are not valid URLs.
        val submission = BatchRequest.parse(body)
        admission.validateInputs(submission.urls)
        rates.check(
            clients.client(request),
            submission = true,
            force = submission.force,
            submittedUrls = submission.urls.size,
        )
        val batch = admission.submit(submission.urls, submission.force)
        val result = status.batch(batch.batchId)
        return HttpResponse.status<BatchResponse>(HttpStatus.ACCEPTED).body(result).headers { it.location(URI("/api/batches/${batch.batchId}")) }
    }

    @Get("/batches/{batchId}")
    fun batch(
        batchId: String,
        request: HttpRequest<*>,
        @QueryValue(defaultValue = "0") offset: Int = 0,
        @QueryValue(defaultValue = "100") limit: Int = BatchPage.MAX_PAGE_SIZE,
    ): BatchResponse {
        rates.check(clients.client(request), submission = false)
        return status.batch(batchId, BatchPage(offset = offset, limit = limit))
    }

    @Get("/runs/{runId}")
    fun run(
        runId: String,
        request: HttpRequest<*>,
    ): RunResponse {
        rates.check(clients.client(request), submission = false)
        return status.run(runId).dto()
    }
}
