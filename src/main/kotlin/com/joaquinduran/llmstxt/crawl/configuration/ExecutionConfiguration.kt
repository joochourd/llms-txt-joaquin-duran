package com.joaquinduran.llmstxt.crawl.configuration

import io.micronaut.context.annotation.ConfigurationProperties
import jakarta.annotation.PostConstruct

/** Local worker and discovery limits. This application runs one coordinating JVM. */
@ConfigurationProperties("crawl")
class ExecutionConfiguration {
    var enabled = true
    var maxConcurrentCrawls = 12
    var dispatchPollSeconds = 2L
    var dispatchCandidateLimit = 40
    var runRetentionDays = 7L

    @PostConstruct
    fun validate() {
        require(maxConcurrentCrawls in 1..12)
        require(dispatchPollSeconds > 0 && dispatchCandidateLimit in 1..100)
        require(runRetentionDays > 0)
    }
}
