package com.joaquinduran.llmstxt.api.controllers

/** Shares the expensive storage probe for two seconds, including failures, without limiting health requests. */
internal class ReadinessCache(
    private val probe: () -> Boolean,
    private val nanoTime: () -> Long = System::nanoTime,
) {
    private companion object {
        const val FRESHNESS_NANOS = 2_000_000_000L
    }

    private var checkedAt: Long? = null
    private var ready = false

    @Synchronized
    fun ready(): Boolean {
        val previous = checkedAt
        if (previous == null || nanoTime() - previous >= FRESHNESS_NANOS) {
            ready = probe()
            checkedAt = nanoTime()
        }
        return ready
    }
}
