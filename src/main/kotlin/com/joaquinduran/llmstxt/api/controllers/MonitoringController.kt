package com.joaquinduran.llmstxt.api.controllers

import com.joaquinduran.llmstxt.api.dto.MonitoringResponse
import com.joaquinduran.llmstxt.api.errors.ApiProblem
import com.joaquinduran.llmstxt.api.mapping.MonitoringView
import com.joaquinduran.llmstxt.monitoring.MonitoringService
import io.micronaut.http.annotation.*
import io.micronaut.json.tree.JsonNode
import io.micronaut.scheduling.annotation.ExecuteOn

@Controller("/api/crawls")
@ExecuteOn("api-io")
class MonitoringController(
    private val monitoring: MonitoringService,
    private val view: MonitoringView,
) {
    @Post("/{identityId}/monitoring")
    fun change(
        identityId: String,
        @Body body: JsonNode,
    ): MonitoringResponse {
        val enabled = body.takeIf { it.isObject }?.get("enabled")
        if (enabled == null || !enabled.isBoolean) throw ApiProblem(400, "INVALID_REQUEST", "Expected a boolean enabled field.")
        return view.response(monitoring.change(identityId, enabled.booleanValue))
    }
}
