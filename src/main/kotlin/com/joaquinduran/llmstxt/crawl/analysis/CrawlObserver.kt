package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import com.joaquinduran.llmstxt.crawl.model.Resource

/** Opt-in development diagnostics. The production runner neither retains nor persists this stream. */
interface CrawlObserver {
    fun candidate(candidate: Candidate) {}

    fun page(page: ExtractedPage) {}

    fun rejected(
        url: String,
        reason: String,
    ) {}

    fun selection(
        resources: List<Resource>,
        selected: List<Resource>,
    ) {}

    fun ranking(
        resource: Resource,
        components: Map<String, Double>,
        family: String,
        reason: String,
    ) {}
}
