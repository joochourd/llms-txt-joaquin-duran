package com.joaquinduran.llmstxt.crawl.configuration

import io.micronaut.context.annotation.ConfigurationProperties
import jakarta.annotation.PostConstruct

/** Automatic checks and retry timing, in seconds. A scheduler tick only discovers due work. */
@ConfigurationProperties("crawl")
class MonitoringConfiguration {
    var monitorIntervalSeconds = 86400L
    var monitorWindowSeconds = 604800L
    var schedulerTickSeconds = 5L
    var schedulerCandidateLimit = 40
    var monitorPendingRecheckSeconds = 60L
    var monitorFailureBackoffSeconds = 900L

    @PostConstruct
    fun validate() {
        require(monitorIntervalSeconds > 0 && monitorWindowSeconds > 0)
        require(schedulerTickSeconds > 0 && schedulerCandidateLimit in 1..100)
        require(monitorPendingRecheckSeconds > 0 && monitorFailureBackoffSeconds > 0)
        require(monitorIntervalSeconds <= monitorWindowSeconds && monitorWindowSeconds <= 31_536_000)
        require(schedulerTickSeconds <= 3600 && monitorPendingRecheckSeconds <= 86400 && monitorFailureBackoffSeconds <= 86400)
    }
}
