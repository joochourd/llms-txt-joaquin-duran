package com.joaquinduran.llmstxt.ingestion

import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

class MutableClock(
    @Volatile var now: Long = 1_800_000_000_000,
) : Clock() {
    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = Instant.ofEpochMilli(now)

    override fun millis(): Long = now
}
