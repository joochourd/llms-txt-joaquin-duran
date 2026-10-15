package com.joaquinduran.llmstxt.crawl.configuration

import io.micronaut.context.annotation.ConfigurationProperties
import jakarta.annotation.PostConstruct

/** Optional sitemap discovery remains bounded independently of the HTML traversal budget. */
@ConfigurationProperties("crawl")
class DiscoveryConfiguration {
    var maxSitemapBytes = 1048576
    var maxSitemapDocuments = 3
    var maxSitemapEntries = 1000

    @PostConstruct
    fun validate() {
        require(maxSitemapBytes in 128..1048576)
        require(maxSitemapDocuments in 1..3 && maxSitemapEntries in 1..1000)
    }
}
