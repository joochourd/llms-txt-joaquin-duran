package com.joaquinduran.llmstxt.api.controllers

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ReadinessCacheTest {
    @Test
    fun `success and failure are shared only within the short freshness window`() {
        var now = 0L
        var available = true
        var probes = 0
        val cache = ReadinessCache({
            probes++
            available
        }, { now })

        repeat(100) { assertTrue(cache.ready()) }
        available = false
        now = 1_999_999_999L
        assertTrue(cache.ready())
        assertEquals(1, probes)
        now++
        assertFalse(cache.ready())
        repeat(100) { assertFalse(cache.ready()) }
        available = true
        now += 2_000_000_000L
        assertTrue(cache.ready())
        assertEquals(3, probes)
    }

    @Test
    fun `concurrent health requests share one storage probe`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val probes = AtomicInteger()
        val cache = ReadinessCache({
            probes.incrementAndGet()
            entered.countDown()
            assertTrue(release.await(5, TimeUnit.SECONDS))
            true
        })
        Executors.newFixedThreadPool(4).use { executor ->
            val requests = (1..4).map { executor.submit<Boolean> { cache.ready() } }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            release.countDown()

            requests.forEach { assertTrue(it.get(5, TimeUnit.SECONDS)) }
            assertEquals(1, probes.get())
        }
    }
}
