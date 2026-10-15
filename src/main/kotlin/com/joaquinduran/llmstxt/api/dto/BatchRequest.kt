package com.joaquinduran.llmstxt.api.dto

import com.joaquinduran.llmstxt.api.errors.ApiProblem
import io.micronaut.json.tree.JsonNode
import io.micronaut.serde.annotation.Serdeable
import java.time.Instant
import java.time.format.DateTimeFormatterBuilder

@Serdeable
data class BatchRequest(
    val urls: List<String>,
    val force: Boolean = false,
) {
    companion object {
        fun parse(body: JsonNode): BatchRequest {
            fun invalid(): Nothing = throw ApiProblem(400, "INVALID_REQUEST", "Expected a urls array of strings and an optional boolean force.")
            if (!body.isObject) invalid()
            val urls = body.get("urls") ?: invalid()
            if (!urls.isArray || urls.values().any { !it.isString }) invalid()
            val force = body.get("force")
            if (force != null && !force.isBoolean) invalid()
            return BatchRequest(urls.values().map { it.stringValue }, force?.booleanValue ?: false)
        }
    }
}
