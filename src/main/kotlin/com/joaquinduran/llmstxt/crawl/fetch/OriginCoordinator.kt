package com.joaquinduran.llmstxt.crawl.fetch

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import jakarta.inject.Singleton
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest
import java.time.Clock
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

@Singleton
class OriginBackoff(
    private val store: AdmissionStore,
    private val config: CrawlConfiguration,
) {
    fun until(origin: String) = store.get(DomainKeys.origin(origin)).ns("blockedUntil") ?: 0L

    fun record(
        origin: String,
        until: Long,
    ) {
        try {
            store.client.updateItem(
                UpdateItemRequest
                    .builder()
                    .tableName(store.table)
                    .key(DomainKeys.origin(origin))
                    .updateExpression("SET blockedUntil = :until, expiresAtEpochSeconds = :ttl, entityType = :type, itemSchemaVersion = :schema")
                    .conditionExpression("attribute_not_exists(blockedUntil) OR blockedUntil < :until")
                    .expressionAttributeValues(
                        mapOf(
                            ":until" to number(until),
                            ":ttl" to number(until / 1000 + config.fetch.originStateIdleTtlSeconds),
                            ":type" to text("OriginBackoff"),
                            ":schema" to number(1),
                        ),
                    ).build(),
            )
        } catch (_: ConditionalCheckFailedException) {
            // A newer, longer block already won.
        }
    }
}

fun httpOrigin(url: okhttp3.HttpUrl) = "${url.scheme}://${url.host}" + if (url.port == (if (url.isHttps) 443 else 80)) "" else ":${url.port}"

@Singleton
class OriginCoordinator(
    private val config: CrawlConfiguration,
    private val backoff: OriginBackoff,
    private val clock: Clock,
) {
    private class State(
        var touched: Long,
        var blocked: Long,
        var next: Long = 0,
        var requests: Int = 0,
        var crawls: Int = 0,
        var waiters: Int = 0,
        var lastStart: Long = 0,
    )

    private val states = linkedMapOf<String, State>()

    @Synchronized
    private fun state(origin: String): State {
        val now = clock.millis()
        states[origin]?.let { return it }
        states.entries.removeIf { (_, originState) ->
            originState.requests == 0 && originState.crawls == 0 && originState.waiters == 0 && originState.blocked <= now &&
                now - originState.touched > config.fetch.originStateIdleTtlSeconds * 1000
        }
        if (states.size >= config.fetch.originCacheMaxEntries) throw CrawlFailure("ORIGIN_CAPACITY", "Origin coordination capacity is exhausted.")
        return State(now, backoff.until(origin)).also { states[origin] = it }
    }

    @Synchronized
    fun tryCrawl(origin: String): AutoCloseable? {
        val originState = state(origin)
        if (originState.blocked > clock.millis() || originState.crawls >= config.fetch.maxActiveCrawlsPerOrigin) return null
        originState.crawls++
        originState.touched = clock.millis()
        return AutoCloseable {
            synchronized(this) {
                originState.crawls--
                originState.touched = clock.millis()
            }
        }
    }

    @Synchronized
    fun canCrawl(origin: String): Boolean {
        val originState = state(origin)
        return originState.blocked <= clock.millis() && originState.crawls < config.fetch.maxActiveCrawlsPerOrigin
    }

    fun request(
        origin: String,
        budget: CrawlBudget,
        crawlDelay: Long,
    ): AutoCloseable {
        val originState = synchronized(this) { state(origin).also { it.waiters++ } }
        try {
            while (true) {
                val wait = synchronized(this) {
                    val now = clock.millis()
                    originState.touched = now
                    if (originState.blocked > now) throw CrawlFailure("ORIGIN_BLOCKED", "The source requested a later retry.")
                    val earliest = maxOf(originState.next, originState.lastStart + crawlDelay)
                    if (originState.requests < config.fetch.maxRequestsPerOrigin && now >= earliest) {
                        originState.requests++
                        originState.lastStart = now
                        originState.next = now + maxOf(config.fetch.minRequestIntervalMillis, crawlDelay)
                        return AutoCloseable {
                            synchronized(this) {
                                originState.requests--
                                originState.touched = clock.millis()
                            }
                        }
                    }
                    maxOf(1, earliest - now).coerceAtMost(50)
                }
                Thread.sleep(minOf(wait, budget.remainingMillis()))
            }
        } finally {
            synchronized(this) { originState.waiters-- }
        }
    }

    fun block(
        origin: String,
        retryAfter: String?,
    ) {
        val now = clock.millis()
        val delay = retryAfter?.toLongOrNull()?.takeIf { it >= 0 }?.let { seconds ->
            if (seconds > (Long.MAX_VALUE - now) / 1000) Long.MAX_VALUE else now + seconds * 1000
        } ?: runCatching { ZonedDateTime.parse(retryAfter, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli() }.getOrNull()
        val until = maxOf(now, delay ?: (now + config.fetch.originDefaultBackoffSeconds * 1000))
        synchronized(this) {
            state(origin).apply {
                blocked = maxOf(blocked, until)
                touched = now
            }
        }
        backoff.record(origin, until)
    }
}
