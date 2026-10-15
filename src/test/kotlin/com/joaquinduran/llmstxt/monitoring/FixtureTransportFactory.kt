package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeTransport
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.time.Clock

@Factory
@Requires(property = "test-fixture.enabled", value = "true")
class FixtureTransportFactory {
    @Singleton
    @Replaces(SafeTransport::class)
    fun transport(clock: Clock): PageTransport = RegisteredFixtureTransport(clock)
}
