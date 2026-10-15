package com.joaquinduran.llmstxt.api.configuration

import io.micronaut.context.annotation.ConfigurationProperties

@ConfigurationProperties("api-limits")
class ApiLimitConfiguration {
    var readsPerMinutePerClient: Int = 120
    var submissionsPerMinutePerClient: Int = 6
    var submissionsPerMinuteGlobal: Int = 30
    var urlsPerMinutePerClient: Int = 1000
    var urlsPerMinuteGlobal: Int = 2000
    var forceRequestsPerFiveMinutesPerClient: Int = 3

    @jakarta.annotation.PostConstruct
    fun validate() {
        require(
            listOf(
                readsPerMinutePerClient,
                submissionsPerMinutePerClient,
                submissionsPerMinuteGlobal,
                urlsPerMinutePerClient,
                urlsPerMinuteGlobal,
                forceRequestsPerFiveMinutesPerClient,
            ).all { it > 0 },
        ) {
            "API rates must be positive."
        }
    }
}
