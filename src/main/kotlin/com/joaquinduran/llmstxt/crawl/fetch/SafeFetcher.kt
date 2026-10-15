package com.joaquinduran.llmstxt.crawl.fetch

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import crawlercommons.robots.BaseRobotRules
import crawlercommons.robots.SimpleRobotRules
import crawlercommons.robots.SimpleRobotRulesParser
import jakarta.inject.Singleton
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import java.time.Clock

@Singleton
class SafeFetcher(
    private val transport: PageTransport,
    private val origins: OriginCoordinator,
    private val config: CrawlConfiguration,
    private val clock: Clock,
    private val urls: UrlIdentityResolver,
) {
    private data class Robots(
        val rules: BaseRobotRules,
        val expires: Long,
    )

    private val cache = linkedMapOf<String, Robots>()
    private val parser = SimpleRobotRulesParser()

    fun normalize(value: String): HttpUrl = try {
        urls.normalize(value).normalizedSeedUrl.toHttpUrl()
    } catch (_: Exception) {
        throw CrawlFailure("UNSAFE_TARGET", "Only public HTTP and HTTPS targets are allowed.")
    }

    private fun raw(
        url: HttpUrl,
        budget: CrawlBudget,
        limit: Int,
        delay: Long,
    ): FetchResponse {
        origins.request(httpOrigin(url), budget, delay).use {
            budget.request()
            val response = transport.get(url, budget, limit)
            if (response.status in setOf(429, 503)) {
                origins.block(httpOrigin(url), response.header("Retry-After"))
                throw CrawlFailure("ORIGIN_BLOCKED", "The source requested a later retry.")
            }
            return response
        }
    }

    private fun redirect(
        url: HttpUrl,
        response: FetchResponse,
    ): HttpUrl {
        val next = response.header("Location")?.let(url::resolve)?.let { normalize(it.toString()) }
            ?: throw CrawlFailure("INVALID_REDIRECT", "The source returned an invalid redirect.")
        if (url.isHttps && !next.isHttps) throw CrawlFailure("UNSAFE_REDIRECT", "HTTPS to HTTP redirects are rejected.")
        return next
    }

    private fun robots(
        url: HttpUrl,
        budget: CrawlBudget,
    ): BaseRobotRules {
        val origin = httpOrigin(url)
        synchronized(cache) {
            cache[origin]?.takeIf { it.expires > clock.millis() }?.let { return it.rules }
            cache.entries.removeIf { it.value.expires <= clock.millis() }
        }
        var target = normalize("$origin/robots.txt")
        var response: FetchResponse? = null
        for (hop in 0..config.fetch.maxRedirects) {
            val fetched = raw(target, budget, config.fetch.maxRobotsBytes, 0)
            if (fetched.status !in setOf(301, 302, 303, 307, 308)) {
                response = fetched
                break
            }
            if (hop == config.fetch.maxRedirects) throw CrawlFailure("REDIRECT_LIMIT", "The redirect limit was exceeded.")
            target = redirect(target, fetched)
        }
        val fetched = requireNotNull(response)
        val rules = when {
            // RFC 9309 allows normal resource access when the policy endpoint returns 4xx.
            // raw() handles 429 separately and preserves its origin backoff.
            fetched.status in 400..499 -> SimpleRobotRules(SimpleRobotRules.RobotRulesMode.ALLOW_ALL)

            fetched.status in 200..299 -> synchronized(parser) {
                parser.parseContent(
                    target.toString(),
                    fetched.bytes,
                    fetched.header("Content-Type") ?: "text/plain",
                    listOf("llmstxtgenerator"),
                )
            }

            else -> throw CrawlFailure("ROBOTS_UNAVAILABLE", "The source robots policy returned HTTP ${fetched.status}; crawling is deferred.")
        }
        synchronized(cache) {
            if (cache.size <
                config.fetch.originCacheMaxEntries
            ) {
                cache[origin] = Robots(rules, clock.millis() + config.fetch.robotsCacheTtlSeconds * 1000)
            }
        }
        return rules
    }

    fun sitemapHints(
        url: HttpUrl,
        budget: CrawlBudget,
    ): List<String> = robots(url, budget).sitemaps.sorted()

    fun get(
        value: String,
        budget: CrawlBudget,
        scope: String? = null,
        maxBytes: Int = config.fetch.maxResponseBytes,
    ): FetchResponse {
        var url = normalize(value)
        for (hop in 0..config.fetch.maxRedirects) {
            budget.remainingMillis()
            if (scope != null && httpOrigin(url) != scope) throw CrawlFailure("OUT_OF_SCOPE", "The redirect is outside the seed's effective origin.")
            val rules = robots(url, budget)
            if (!rules.isAllowed(url.toString())) throw CrawlFailure("ROBOTS_DISALLOWED", "The source disallows this URL for our crawler.")
            val response = raw(url, budget, maxBytes, rules.crawlDelay.coerceAtLeast(0))
            if (response.status !in setOf(301, 302, 303, 307, 308)) return response
            if (hop == config.fetch.maxRedirects) break
            url = redirect(url, response)
        }
        throw CrawlFailure("REDIRECT_LIMIT", "The redirect limit was exceeded.")
    }
}
