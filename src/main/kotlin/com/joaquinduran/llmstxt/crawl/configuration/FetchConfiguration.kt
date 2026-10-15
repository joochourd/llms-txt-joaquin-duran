package com.joaquinduran.llmstxt.crawl.configuration

import io.micronaut.context.annotation.ConfigurationProperties
import jakarta.annotation.PostConstruct

/** Network budgets and per-origin coordination. Byte limits apply to complete decompressed responses. */
@ConfigurationProperties("crawl")
class FetchConfiguration {
    var maxActiveCrawlsPerOrigin = 1
    var maxRequestsPerOrigin = 1
    var minRequestIntervalMillis = 1000L
    var requestTimeoutSeconds = 10L
    var maxResponseBytes = 2097152
    var maxRobotsBytes = 524288
    var maxRedirects = 5
    var robotsCacheTtlSeconds = 900L
    var originStateIdleTtlSeconds = 900L
    var originCacheMaxEntries = 1000
    var originDefaultBackoffSeconds = 60L

    @PostConstruct
    fun validate() {
        require(maxActiveCrawlsPerOrigin in 1..8 && maxRequestsPerOrigin in 1..8)
        require(minRequestIntervalMillis >= 0 && requestTimeoutSeconds > 0)
        require(maxResponseBytes in 1024..2097152 && maxRobotsBytes in 128..524288)
        require(maxRedirects in 0..5)
        require(robotsCacheTtlSeconds > 0 && originStateIdleTtlSeconds > 0 && originCacheMaxEntries > 0)
        require(originDefaultBackoffSeconds > 0)
    }
}
