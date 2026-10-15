package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/** Wall time is persisted; elapsed execution uses a monotonic clock. */
class CrawlBudget(
    val maxRequests: Int,
    durationMillis: Long,
) {
    private val until = System.nanoTime() + TimeUnit.MILLISECONDS
        .toNanos(durationMillis)
    val cancelled = AtomicBoolean(false)
    var requests = 0
        private set

    fun remainingMillis(): Long {
        if (cancelled.get() || Thread.currentThread().isInterrupted) throw CrawlFailure("CANCELLED", "The crawl was interrupted.")
        val remaining = TimeUnit.NANOSECONDS
            .toMillis(until - System.nanoTime())
        if (remaining <= 0) throw CrawlFailure("TIME_BUDGET", "The crawl exceeded its time budget.")
        return remaining
    }

    fun request() {
        remainingMillis()
        if (requests >=
            maxRequests
        ) {
            throw CrawlFailure("REQUEST_BUDGET", "The request budget was exhausted.")
        }
        requests++
    }
}
