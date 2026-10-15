package com.joaquinduran.llmstxt.api.errors

import com.joaquinduran.llmstxt.api.dto.ApiError
import com.joaquinduran.llmstxt.results.ResultReadException
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton

@Singleton
class ResultReadExceptionHandler : ExceptionHandler<ResultReadException, HttpResponse<ApiError>> {
    override fun handle(
        request: HttpRequest<*>,
        exception: ResultReadException,
    ): HttpResponse<ApiError> = HttpResponse.notFound(ApiError(exception.error.code, exception.error.message))
}
