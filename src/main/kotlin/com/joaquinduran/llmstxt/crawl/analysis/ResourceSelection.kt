package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import com.joaquinduran.llmstxt.crawl.model.ObservedLink
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.crawl.model.SeedContext
import com.joaquinduran.llmstxt.ingestion.sha256
import okhttp3.HttpUrl
import org.jsoup.Jsoup
import java.io.ByteArrayInputStream
import java.text.Normalizer

class ResourceSelection {
    fun isHub(
        url: String,
        links: List<ObservedLink>,
    ): Boolean {
        val prefix = url.substringBefore('?').substringBefore('#').trimEnd('/') + "/"
        return links
            .asSequence()
            .map {
                it.url
                    .substringBefore('?')
                    .substringBefore('#')
                    .trimEnd('/')
            }.filter { it.length > prefix.length && it.startsWith(prefix) }
            .distinct()
            .take(ContentThresholds.MIN_HUB_CHILDREN)
            .count() >=
            ContentThresholds.MIN_HUB_CHILDREN
    }

    fun family(
        candidate: Candidate,
        category: String? = null,
    ): String {
        val pathFamily = "PATH:${candidate.url.substringBefore('?').trimEnd('/').substringBeforeLast('/')}"
        if (candidate.structuralHub) return "ENTRY:${candidate.url}"
        if (category == "ARTICLE") return pathFamily
        if (candidate.sectionOverview || (candidate.source == "NAVIGATION" && candidate.group == null)) return "ENTRY:${candidate.url}"
        return candidate.group?.let { "GROUP:$it" }
            ?: pathFamily
    }

    fun select(
        resources: List<Resource>,
        limit: Int,
        families: Map<String, String> = emptyMap(),
    ): List<Resource> {
        val comparator = compareBy<Resource> { if (it.section == "Overview") 0 else 1 }
            .thenByDescending { it.score }
            .thenBy { it.depth ?: Int.MAX_VALUE }
            .thenBy { it.primaryHtmlUrl }
            .thenBy { it.resourceKey }
        val groups = resources.sortedWith(comparator).groupBy { it.contentHash }
        val unique = groups.values
            .map { group ->
                group.first().copy(aliases = group.flatMap { it.aliases }.distinct().sorted())
            }.sortedWith(comparator)
        val selected = linkedSetOf<Resource>()
        unique.firstOrNull { it.section == "Overview" }?.let(selected::add)
        unique
            .groupBy { it.category }
            .values
            .map { it.first() }
            .sortedWith(comparator)
            .forEach { if (selected.size < limit) selected.add(it) }

        fun familyOf(resource: Resource) = families[resource.resourceKey] ?: if ("STRUCTURAL_HUB" in resource.reasonCodes ||
            (resource.category != "ARTICLE" && ("SECTION_OVERVIEW" in resource.reasonCodes || "PRIMARY_NAVIGATION" in resource.reasonCodes))
        ) {
            "ENTRY:${resource.primaryHtmlUrl}"
        } else {
            "PATH:${resource.primaryHtmlUrl.substringBefore('?').trimEnd('/').substringBeforeLast('/')}"
        }
        while (selected.size < limit) {
            val counts = selected.groupingBy(::familyOf).eachCount()
            val next = unique.filter { it !in selected }.minWithOrNull(
                compareByDescending<Resource> {
                    it.score - RankingWeights.FAMILY_REPETITION_PENALTY * (counts[familyOf(it)] ?: 0)
                }.then(comparator),
            ) ?: break
            selected.add(next)
        }
        return selected.sortedWith(comparator)
    }
}
