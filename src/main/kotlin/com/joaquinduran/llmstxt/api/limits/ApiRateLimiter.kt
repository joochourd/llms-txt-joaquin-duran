package com.joaquinduran.llmstxt.api.limits

import com.joaquinduran.llmstxt.api.configuration.ApiLimitConfiguration
import com.joaquinduran.llmstxt.api.errors.ApiProblem
import io.micronaut.context.annotation.ConfigurationProperties
import jakarta.inject.Singleton
import java.time.Clock
import kotlin.math.ceil

// One process, bounded ephemeral state. Client identity comes from the explicit proxy policy.
@Singleton
class ApiRateLimiter(
    private val clock: Clock,
    private val configuration: ApiLimitConfiguration,
) {
    private companion object {
        const val READ_BURST = 120
        const val SUBMISSION_BURST = 2
        const val FORCE_BURST = 1
        const val GLOBAL_SUBMISSION_BURST = 10
        const val URL_BURST = 1000
        const val GLOBAL_URL_BURST = 2000
        const val MAX_CLIENTS = 10_000
        const val CLIENT_IDLE_TTL_MILLIS = 600_000
        const val RATE_WINDOW_MILLIS = 60_000.0
        const val FORCE_RATE_WINDOW_MILLIS = 300_000.0
    }

    private data class Bucket(
        var tokens: Double,
        var lastAt: Long,
        val burst: Int,
        val ratePerMillis: Double,
    )

    private data class Client(
        val read: Bucket,
        val submit: Bucket,
        val force: Bucket,
        val urls: Bucket,
        var lastSeen: Long,
    )

    private val clients = mutableMapOf<String, Client>()
    private val global = Bucket(
        GLOBAL_SUBMISSION_BURST.toDouble(),
        clock.millis(),
        GLOBAL_SUBMISSION_BURST,
        configuration.submissionsPerMinuteGlobal / RATE_WINDOW_MILLIS,
    )
    private val globalUrls = Bucket(
        GLOBAL_URL_BURST.toDouble(),
        clock.millis(),
        GLOBAL_URL_BURST,
        configuration.urlsPerMinuteGlobal / RATE_WINDOW_MILLIS,
    )

    @Synchronized
    fun check(
        peer: String,
        submission: Boolean,
        force: Boolean = false,
        submittedUrls: Int = 1,
    ) {
        require(submittedUrls in 1..URL_BURST)
        val now = clock.millis()
        clients.entries.removeIf { now - it.value.lastSeen > CLIENT_IDLE_TTL_MILLIS }
        val client = clients[peer] ?: run {
            if (clients.size >= MAX_CLIENTS) throw ApiProblem(503, "SERVER_BUSY", "Please retry shortly.")
            Client(
                Bucket(READ_BURST.toDouble(), now, READ_BURST, configuration.readsPerMinutePerClient / RATE_WINDOW_MILLIS),
                Bucket(SUBMISSION_BURST.toDouble(), now, SUBMISSION_BURST, configuration.submissionsPerMinutePerClient / RATE_WINDOW_MILLIS),
                Bucket(FORCE_BURST.toDouble(), now, FORCE_BURST, configuration.forceRequestsPerFiveMinutesPerClient / FORCE_RATE_WINDOW_MILLIS),
                Bucket(URL_BURST.toDouble(), now, URL_BURST, configuration.urlsPerMinutePerClient / RATE_WINDOW_MILLIS),
                now,
            ).also { clients[peer] = it }
        }
        client.lastSeen = now
        val charges = if (submission) {
            listOf(client.submit to 1, global to 1, client.urls to submittedUrls, globalUrls to submittedUrls) +
                if (force) listOf(client.force to 1) else emptyList()
        } else {
            listOf(client.read to 1)
        }
        charges.forEach { (bucket, _) ->
            bucket.tokens = minOf(bucket.burst.toDouble(), bucket.tokens + maxOf(0L, now - bucket.lastAt) * bucket.ratePerMillis)
            bucket.lastAt = now
        }
        val wait = charges.maxOf { (bucket, cost) ->
            if (bucket.tokens >= cost) 0L else ceil((cost - bucket.tokens) / bucket.ratePerMillis / 1000).toLong()
        }
        if (wait > 0) throw ApiProblem(429, "RATE_LIMITED", "Request rate limit reached. Please retry later.", wait)
        charges.forEach { (bucket, cost) -> bucket.tokens -= cost }
    }
}
