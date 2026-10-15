package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MonitoringPolicyTest {
    @Test
    fun `success jitter is stable bounded and never shortens an origin block`() {
        val policy = MonitoringPolicy(CrawlConfiguration())
        val profile = MonitoringProfile(86400, 604800)
        val start = 1_800_000_000_000L
        val planned = policy.success("identity", start, profile, 0)
        assertEquals(planned, policy.success("identity", start, profile, 0))
        assertTrue(planned in (start + 86_400_000)..(start + 95_040_000))
        val blocked = start + 200_000_000
        assertEquals(blocked, policy.success("identity", start, profile, blocked))
    }

    @Test
    fun `retry delay grows to the configured interval`() {
        val policy = MonitoringPolicy(CrawlConfiguration())
        val normal = MonitoringProfile(86400, 604800)
        val retries = (1L..20L).map { policy.failure(0, it, normal, 0) }
        assertTrue(retries.zipWithNext().all { (before, after) -> before <= after })
        assertTrue(retries.all { it in 900_000..86_400_000 })
        assertEquals(86_400_000L, retries.last())
        assertEquals(15_000L, policy.failure(0, 100, MonitoringProfile(15, 600), 0))
        assertEquals(900_000L, policy.failure(0, 1, MonitoringProfile(15, 600), 900_000))
    }

    @Test
    fun `initial monitoring window has one fixed expiry`() {
        val policy = MonitoringPolicy(CrawlConfiguration())
        val profile = MonitoringProfile(86400, 604800)
        val startedAt = 1_800_000_000_000L

        assertEquals(startedAt + 604_800_000, policy.initialWindow(startedAt, profile))
    }
}
