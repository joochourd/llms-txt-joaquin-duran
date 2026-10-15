package com.joaquinduran.llmstxt.bootstrap

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/** A short-lived Docker health-check client; this mode never starts another application instance. */
object ReadinessProbe {
    fun exitCode(): Int {
        val port = System.getenv("PORT") ?: "8080"
        val request = HttpRequest
            .newBuilder(URI("http://127.0.0.1:$port/health/readiness"))
            .timeout(Duration.ofSeconds(15))
            .GET()
            .build()
        return HttpClient.newHttpClient().use { client ->
            val response = client.send(request, HttpResponse.BodyHandlers.discarding())
            if (response.statusCode() == 200) 0 else 1
        }
    }
}
