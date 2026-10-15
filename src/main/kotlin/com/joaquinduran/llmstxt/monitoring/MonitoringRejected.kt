package com.joaquinduran.llmstxt.monitoring

class MonitoringRejected(
    val code: String,
    override val message: String,
) : RuntimeException(message)
