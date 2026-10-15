package com.joaquinduran.llmstxt.api.configuration

import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import java.time.Clock

@Factory
class ClockFactory {
    @Singleton
    fun clock(): Clock = Clock.systemUTC()
}
