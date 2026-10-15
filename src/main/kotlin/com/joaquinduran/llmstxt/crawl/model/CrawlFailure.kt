package com.joaquinduran.llmstxt.crawl.model

class CrawlFailure(
    val code: String,
    message: String,
) : RuntimeException(message)
