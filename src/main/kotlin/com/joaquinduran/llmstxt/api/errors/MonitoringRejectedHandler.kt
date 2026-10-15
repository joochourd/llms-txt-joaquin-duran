package com.joaquinduran.llmstxt.api.errors

import com.joaquinduran.llmstxt.api.dto.ApiError
import com.joaquinduran.llmstxt.monitoring.MonitoringRejected
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

@Singleton
class MonitoringRejectedHandler : ExceptionHandler<MonitoringRejected, HttpResponse<ApiError>> {
    override fun handle(
        request: HttpRequest<*>,
        exception: MonitoringRejected,
    ): HttpResponse<ApiError> {
        val status = when (exception.code) {
            "NOT_FOUND" -> HttpStatus.NOT_FOUND
            "MONITORING_EXPIRED" -> HttpStatus.GONE
            else -> HttpStatus.CONFLICT
        }
        return HttpResponse.status<ApiError>(status).body(ApiError(exception.code, exception.message))
    }
}
