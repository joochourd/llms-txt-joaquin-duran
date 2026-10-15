package com.joaquinduran.llmstxt.crawl.configuration

import jakarta.inject.Singleton

/** Groups share the existing crawl property prefix so environment and property overrides remain compatible. */
@Singleton
class CrawlConfiguration(
    val fetch: FetchConfiguration = FetchConfiguration(),
    val discovery: DiscoveryConfiguration = DiscoveryConfiguration(),
    val content: ContentConfiguration = ContentConfiguration(),
    val execution: ExecutionConfiguration = ExecutionConfiguration(),
    val monitoring: MonitoringConfiguration = MonitoringConfiguration(),
) {
    fun validate() {
        fetch.validate()
        discovery.validate()
        content.validate()
        execution.validate()
        monitoring.validate()
    }
}
