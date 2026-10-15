package com.joaquinduran.llmstxt.api.errors

import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.server.exceptions.ExceptionHandler
import io.micronaut.serde.annotation.Serdeable
import jakarta.inject.Singleton
import java.util.concurrent.RejectedExecutionException

@Serdeable
data class BusyResponse(
    val code: String = "SERVER_BUSY",
    val message: String = "Please retry shortly.",
)

@Singleton
class ExecutorSaturationHandler : ExceptionHandler<RejectedExecutionException, HttpResponse<BusyResponse>> {
    override fun handle(
        request: HttpRequest<*>,
        exception: RejectedExecutionException,
    ): HttpResponse<BusyResponse> = HttpResponse.status<BusyResponse>(HttpStatus.SERVICE_UNAVAILABLE).body(BusyResponse())
}
