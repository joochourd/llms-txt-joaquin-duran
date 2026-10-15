package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.ingestion.sha256

data class MonitoringProfile(
    val intervalSeconds: Long,
    val windowSeconds: Long,
)

/** Scheduling changes eligibility, never the accepted observation or artifact. */
class MonitoringPolicy(
    private val configuration: CrawlConfiguration,
) {
    fun success(
        identityId: String,
        now: Long,
        profile: MonitoringProfile,
        blockedUntil: Long,
    ): Long {
        val maximumJitter = profile.intervalSeconds / 10
        val jitter = sha256(identityId).take(8).toLong(16) % (maximumJitter + 1)
        return maxOf(now + (profile.intervalSeconds + jitter) * 1000, blockedUntil)
    }

    fun failure(
        now: Long,
        failures: Long,
        profile: MonitoringProfile,
        blockedUntil: Long,
    ): Long {
        var seconds = minOf(configuration.monitoring.monitorFailureBackoffSeconds, profile.intervalSeconds)
        repeat(minOf(maxOf(0, failures - 1), 32).toInt()) {
            seconds = minOf(profile.intervalSeconds, seconds * 2)
        }
        return maxOf(now + seconds * 1000, blockedUntil)
    }

    fun pending(
        now: Long,
        blockedUntil: Long = 0,
    ): Long = maxOf(now + configuration.monitoring.monitorPendingRecheckSeconds * 1000, blockedUntil)

    fun initialWindow(
        now: Long,
        profile: MonitoringProfile,
    ): Long = now + profile.windowSeconds * 1000
}
