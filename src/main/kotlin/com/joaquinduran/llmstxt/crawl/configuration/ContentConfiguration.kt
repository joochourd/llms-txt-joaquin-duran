package com.joaquinduran.llmstxt.crawl.configuration

import io.micronaut.context.annotation.ConfigurationProperties
import jakarta.annotation.PostConstruct

/** Output and retained metadata limits; presentation lengths count characters, not bytes. */
@ConfigurationProperties("crawl")
class ContentConfiguration {
    var maxLlmsBytes = 65536
    var maxResourceMetadataBytes = 8192
    var titleMaxChars = 120
    var descriptionMaxChars = 240

    @PostConstruct
    fun validate() {
        require(maxLlmsBytes in 256..65536 && maxResourceMetadataBytes in 1024..8192)
        require(titleMaxChars in 1..120 && descriptionMaxChars in 1..240)
    }
}
