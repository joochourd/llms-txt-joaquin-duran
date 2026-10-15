package com.joaquinduran.llmstxt.api.configuration

import io.micronaut.context.annotation.ConfigurationProperties

@ConfigurationProperties("api-client")
class ClientIpConfiguration {
    var trustedProxyCidrs: String = ""
}
