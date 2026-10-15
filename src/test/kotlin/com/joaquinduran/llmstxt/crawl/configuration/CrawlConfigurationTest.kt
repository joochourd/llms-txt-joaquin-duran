package com.joaquinduran.llmstxt.crawl.configuration

import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import io.micronaut.context.ApplicationContext
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class CrawlConfigurationTest {
    @Test
    fun `default crawl profile supports twelve workers and ten page selection without changing origin courtesy`() {
        val configuration = CrawlConfiguration()
        val admission = AdmissionConfiguration()

        configuration.validate()
        admission.validate()

        assertEquals(12, configuration.execution.maxConcurrentCrawls)
        assertEquals(10, admission.budgets().maxPages)
        assertEquals(10, admission.budgets().maxSelectedResources)
        assertEquals(1, configuration.fetch.maxRequestsPerOrigin)
        assertEquals(1000L, configuration.fetch.minRequestIntervalMillis)
    }

    @Test
    fun `selection exceeding the ten page crawl budget is rejected at startup`() {
        val admission = AdmissionConfiguration().apply { maxSelectedResources = 11 }

        assertThrows(IllegalArgumentException::class.java, admission::validate)
    }

    @Test
    fun `existing flat properties configure the responsible groups through dependency injection`() {
        val properties = mapOf(
            "crawl.enabled" to false,
            "crawl.max-response-bytes" to 2048,
            "crawl.max-sitemap-entries" to 100,
            "crawl.max-llms-bytes" to 1024,
            "crawl.max-concurrent-crawls" to 2,
            "crawl.monitor-interval-seconds" to 120,
            "crawl.monitor-window-seconds" to 3600,
            "crawl.run-retention-days" to 3,
        )

        ApplicationContext.run(properties).use { context ->
            val configuration = context.getBean(CrawlConfiguration::class.java)

            assertSame(context.getBean(FetchConfiguration::class.java), configuration.fetch)
            assertEquals(2048, configuration.fetch.maxResponseBytes)
            assertEquals(100, configuration.discovery.maxSitemapEntries)
            assertEquals(1024, configuration.content.maxLlmsBytes)
            assertFalse(configuration.execution.enabled)
            assertEquals(2, configuration.execution.maxConcurrentCrawls)
            assertEquals(120L, configuration.monitoring.monitorIntervalSeconds)
            assertEquals(3600L, configuration.monitoring.monitorWindowSeconds)
            assertEquals(3L, configuration.execution.runRetentionDays)
        }
    }

    @Test
    fun `grouping preserves startup rejection of unsafe or unbounded operational settings`() {
        val invalidConfigurations = listOf(
            CrawlConfiguration().apply { fetch.maxResponseBytes = 2_097_153 },
            CrawlConfiguration().apply { discovery.maxSitemapDocuments = 4 },
            CrawlConfiguration().apply { content.maxLlmsBytes = 65_537 },
            CrawlConfiguration().apply { execution.maxConcurrentCrawls = 13 },
            CrawlConfiguration().apply { execution.maxConcurrentCrawls = 0 },
            CrawlConfiguration().apply { monitoring.monitorIntervalSeconds = monitoring.monitorWindowSeconds + 1 },
            CrawlConfiguration().apply { execution.runRetentionDays = 0 },
        )

        invalidConfigurations.forEach { configuration ->
            assertThrows(IllegalArgumentException::class.java, configuration::validate)
        }
    }
}
