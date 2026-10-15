package com.joaquinduran.llmstxt.api.controllers

import com.joaquinduran.llmstxt.persistence.dynamo.InfrastructureReadiness
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.serde.annotation.Serdeable

@Serdeable
data class HealthResponse(
    val status: String,
)

@Controller("/health")
class HealthController(
    private val infrastructure: InfrastructureReadiness,
) {
    private val readiness = ReadinessCache(infrastructure::isReady)

    @Get("/liveness")
    fun liveness(): HealthResponse = HealthResponse("UP")

    @Get("/readiness")
    @ExecuteOn("api-io")
    fun readiness(): HttpResponse<HealthResponse> = if (readiness.ready()) {
        HttpResponse.ok(HealthResponse("UP"))
    } else {
        HttpResponse.status<HealthResponse>(HttpStatus.SERVICE_UNAVAILABLE).body(HealthResponse("DOWN"))
    }
}
