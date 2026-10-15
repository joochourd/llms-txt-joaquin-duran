package com.joaquinduran.llmstxt.crawl.execution.load

import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeTransport
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import okhttp3.HttpUrl
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/** Test-classpath only: finite synthetic sources, never public network access. */
class LoadFixtureTransport : PageTransport {
    val requests = AtomicInteger()
    val activeRequests = AtomicInteger()
    val peakRequests = AtomicInteger()
    val peakRequestsPerOrigin = AtomicInteger()
    val minimumStartSpacingMillis = AtomicLong(Long.MAX_VALUE)
    private val origins = ConcurrentHashMap<String, AtomicInteger>()
    private val lastStarts = ConcurrentHashMap<String, Long>()

    override fun get(
        url: HttpUrl,
        budget: CrawlBudget,
        maxBytes: Int,
    ): FetchResponse {
        require(url.host.matches(Regex("site-[0-9]{3}\\.load-fixture\\.example")))
        requests.incrementAndGet()
        peakRequests.accumulateAndGet(activeRequests.incrementAndGet(), ::maxOf)
        val originRequests = origins.computeIfAbsent(url.host) { AtomicInteger() }
        peakRequestsPerOrigin.accumulateAndGet(originRequests.incrementAndGet(), ::maxOf)
        val now = System.nanoTime()
        lastStarts.put(url.host, now)?.let { previous ->
            minimumStartSpacingMillis.accumulateAndGet((now - previous) / 1_000_000, ::minOf)
        }
        try {
            val siteNumber = url.host
                .substringAfter("site-")
                .substringBefore('.')
                .toInt()
            Thread.sleep(if (siteNumber % 10 == 0) 350 else 50)
            budget.remainingMillis()
            val html = when (url.encodedPath) {
                "/docs" -> page(url.host, "Documentation", "<nav><a href='/docs/guide'>Getting started guide</a></nav>")
                "/docs/guide" -> page(url.host, "Getting started guide", "")
                else -> null
            }
            val bytes = html?.toByteArray() ?: byteArrayOf()
            check(bytes.size <= maxBytes)
            return FetchResponse(url, if (html == null) 404 else 200, mapOf("Content-Type" to listOf("text/html")), bytes)
        } finally {
            originRequests.decrementAndGet()
            activeRequests.decrementAndGet()
        }
    }

    private fun page(
        site: String,
        title: String,
        links: String,
    ): String = "<html><head><meta name='description' content='Practical setup and integration documentation for $site.'></head>" +
        "<body>$links<main><h1>$title for $site</h1><p>" +
        "Useful setup instructions and integration examples for $site. ".repeat(20) + "</p></main></body></html>"
}

@Factory
@Requires(property = "test-load.enabled", value = "true")
class LoadFixtureFactory {
    @Singleton
    @Replaces(SafeTransport::class)
    fun transport(): LoadFixtureTransport = LoadFixtureTransport()
}
