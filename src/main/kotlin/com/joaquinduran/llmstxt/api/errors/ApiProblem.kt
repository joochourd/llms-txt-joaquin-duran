package com.joaquinduran.llmstxt.api.errors

import com.joaquinduran.llmstxt.api.dto.ApiError
import com.joaquinduran.llmstxt.persistence.dynamo.StorageUnavailable
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.server.exceptions.ExceptionHandler
import io.micronaut.serde.annotation.Serdeable
import jakarta.inject.Singleton
import software.amazon.awssdk.core.exception.SdkException

class ApiProblem(
    val status: Int,
    val code: String,
    message: String,
    val retryAfter: Long? = null,
) : RuntimeException(message)

@Singleton
class ApiProblemHandler : ExceptionHandler<ApiProblem, HttpResponse<ApiError>> {
    override fun handle(
        request: HttpRequest<*>,
        exception: ApiProblem,
    ): HttpResponse<ApiError> {
        val response = HttpResponse.status<ApiError>(HttpStatus.valueOf(exception.status)).body(ApiError(exception.code, exception.message!!))
        exception.retryAfter?.let { response.header("Retry-After", it.toString()) }
        return response
    }
}

@Singleton
class StorageUnavailableHandler : ExceptionHandler<StorageUnavailable, HttpResponse<ApiError>> {
    override fun handle(
        request: HttpRequest<*>,
        exception: StorageUnavailable,
    ) = storageUnavailable()
}

@Singleton
class SdkFailureHandler : ExceptionHandler<SdkException, HttpResponse<ApiError>> {
    override fun handle(
        request: HttpRequest<*>,
        exception: SdkException,
    ) = storageUnavailable()
}

private fun storageUnavailable() = HttpResponse
    .status<ApiError>(HttpStatus.SERVICE_UNAVAILABLE)
    .body(
        ApiError(
            "STORAGE_UNAVAILABLE",
            "Storage is temporarily unavailable. Some admission decisions may already be durable. Please retry status reads.",
        ),
    )
