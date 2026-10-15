package com.joaquinduran.llmstxt.results

import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.crawl.model.SeedContext
import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.RunCoverage

data class AcceptedCrawl(
    val metadata: CrawlMeta,
    val context: SeedContext? = null,
    val coverage: RunCoverage? = null,
    val warnings: List<String> = emptyList(),
    val resources: List<Resource> = emptyList(),
    val sourceChanged: Boolean? = null,
)
