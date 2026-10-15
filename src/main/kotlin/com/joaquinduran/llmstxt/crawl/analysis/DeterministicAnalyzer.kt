package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.model.*
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import com.joaquinduran.llmstxt.crawl.model.ObservedLink
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.crawl.model.SeedContext
import okhttp3.HttpUrl

/** Coordinates pure, fixed pipeline stages; discovery and persistence stay outside content analysis. */
class DeterministicAnalyzer {
    private val contextInference = ContextInference()
    private val ranking = ResourceRanking()
    private val selection = ResourceSelection()
    private val classifier = ResourceClassifier(ranking, selection)

    fun context(seed: ExtractedPage): SeedContext = contextInference.context(seed)

    fun preComponents(
        url: HttpUrl,
        anchor: String,
        source: String,
        seed: ExtractedPage,
        context: SeedContext,
    ): Map<String, Double> = ranking.preComponents(url, anchor, source, seed, context)

    fun preScore(
        url: HttpUrl,
        anchor: String,
        source: String,
        seed: ExtractedPage,
        context: SeedContext,
    ): Double = ranking.preScore(url, anchor, source, seed, context)

    fun scoreComponents(
        page: ExtractedPage,
        seed: ExtractedPage,
        context: SeedContext,
    ): Map<String, Double> = ranking.scoreComponents(page, seed, context)

    fun useful(page: ExtractedPage): Boolean = ranking.useful(page)

    fun resource(
        page: ExtractedPage,
        seed: ExtractedPage,
        context: SeedContext,
    ): Resource = classifier.resource(page, seed, context)

    fun isHub(
        url: String,
        links: List<ObservedLink>,
    ): Boolean = selection.isHub(url, links)

    fun family(
        candidate: Candidate,
        category: String? = null,
    ): String = selection.family(candidate, category)

    fun select(
        resources: List<Resource>,
        limit: Int,
        families: Map<String, String> = emptyMap(),
    ): List<Resource> = selection.select(resources, limit, families)
}
