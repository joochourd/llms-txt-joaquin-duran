package com.joaquinduran.llmstxt.api.limits

import com.joaquinduran.llmstxt.api.configuration.ApiLimitConfiguration
import com.joaquinduran.llmstxt.api.errors.ApiProblem
import com.joaquinduran.llmstxt.api.limits.ApiRateLimiter
import com.joaquinduran.llmstxt.ingestion.MutableClock
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ApiRateLimiterTest {
    @Test
    fun `submissions have bounded bursts and retry after replenishment`() {
        val clock = MutableClock()
        val limiter = ApiRateLimiter(clock, ApiLimitConfiguration())
        repeat(2) { limiter.check("peer", true) }
        val denied = assertThrows(ApiProblem::class.java) { limiter.check("peer", true) }
        assertEquals(429, denied.status)
        assertEquals(10L, denied.retryAfter)
        clock.now += 10_000
        limiter.check("peer", true)
        limiter.check("other-peer", true)
    }

    @Test
    fun `force has its own quota and global rate spans clients`() {
        val clock = MutableClock()
        val limiter = ApiRateLimiter(clock, ApiLimitConfiguration())
        limiter.check("peer", true, true)
        assertEquals(100L, assertThrows(ApiProblem::class.java) { limiter.check("peer", true, true) }.retryAfter)
        clock.now += 100_000
        limiter.check("peer", true, true)
        val global = ApiRateLimiter(clock, ApiLimitConfiguration())
        repeat(10) { global.check("peer-$it", true) }
        assertEquals(429, assertThrows(ApiProblem::class.java) { global.check("new-peer", true) }.status)
        // Read protection is independent of submission tokens.
        global.check("new-peer", false)
    }

    @Test
    fun `bulk submissions consume URL budgets independently of request counts`() {
        val clock = MutableClock()
        val limiter = ApiRateLimiter(clock, ApiLimitConfiguration())

        limiter.check("first-client", submission = true, submittedUrls = 1000)
        val clientLimited = assertThrows(ApiProblem::class.java) {
            limiter.check("first-client", submission = true, submittedUrls = 1)
        }
        limiter.check("second-client", submission = true, submittedUrls = 1000)
        val globallyLimited = assertThrows(ApiProblem::class.java) {
            limiter.check("third-client", submission = true, submittedUrls = 1)
        }

        assertEquals(429, clientLimited.status)
        assertEquals(429, globallyLimited.status)
        limiter.check("third-client", submission = false)
        clock.now += 60_000
        limiter.check("first-client", submission = true, submittedUrls = 1000)
    }
}
