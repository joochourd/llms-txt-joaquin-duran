package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.analysis.ContentThresholds
import com.joaquinduran.llmstxt.crawl.analysis.CrawlObserver
import com.joaquinduran.llmstxt.crawl.analysis.DeterministicAnalyzer
import com.joaquinduran.llmstxt.crawl.analysis.RankingWeights
import com.joaquinduran.llmstxt.crawl.analysis.SourceExtractor
import com.joaquinduran.llmstxt.crawl.analysis.clean
import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.discovery.SitemapDiscovery
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.crawl.fetch.httpOrigin
import com.joaquinduran.llmstxt.crawl.generation.LlmsRenderer
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.CrawlResult
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import com.joaquinduran.llmstxt.crawl.model.ObservedLink
import com.joaquinduran.llmstxt.crawl.model.Resource
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.RunCounters
import com.joaquinduran.llmstxt.ingestion.RunCoverage
import com.joaquinduran.llmstxt.ingestion.RunPhase
import crawlercommons.sitemaps.UnknownFormatException
import jakarta.inject.Singleton
import java.io.IOException
import java.net.URI
import java.time.Clock
import java.util.ArrayDeque
import javax.xml.stream.XMLStreamException

@Singleton
class CrawlRunner(
    private val fetcher: SafeFetcher,
    private val config: CrawlConfiguration,
    private val clock: Clock,
) {
    fun run(
        run: CrawlRun,
        baseline: List<Resource>,
        budget: CrawlBudget,
        observer: CrawlObserver? = null,
        progress: (RunPhase, RunCounters) -> Unit,
    ): CrawlResult {
        val started = clock.millis()
        val budgets = run.budgets
        val extractor = SourceExtractor(config)
        val analyzer = DeterministicAnalyzer()
        val checks = linkedMapOf<String, String>()
        val pages = linkedMapOf<String, ExtractedPage>()
        val frontier = linkedMapOf<String, Candidate>()
        val examined = hashSetOf<String>()
        val discoveredPaths = ArrayDeque<ExtractedPage>()
        val reasons = linkedSetOf<String>()
        val warnings = linkedSetOf<String>()
        val sources = linkedSetOf("SEED")
        var attempted = 0L
        var omitted = 0L
        var links = 0
        var peakFrontier = 0
        val comparator = compareBy<Candidate> {
            when {
                it.seed -> -2.0
                it.baseline -> -1.0
                it.depth == null -> 1.5
                else -> it.depth.toDouble()
            }
        }.thenByDescending { it.score }
            .thenBy { it.url }
        val seedCandidate = Candidate(run.normalizedSeedUrl, "SEED", null, "", 0, Double.MAX_VALUE, seed = true)
        attempted++
        progress(RunPhase.DISCOVERING, RunCounters(attempted, 0, 0))
        val seedResponse = fetcher.get(run.normalizedSeedUrl, budget)
        if (seedResponse.status !in 200..299) throw CrawlFailure("SEED_UNAVAILABLE", "The seed returned HTTP ${seedResponse.status}.")
        // Hash complete main text before discarding it; retain no page bodies across the crawl.
        val seed = extractor.extract(seedResponse, seedCandidate, budget, budgets.maxLinksInspected).copy(text = "")
        warnings.addAll(seed.warnings)
        observer?.page(seed)
        if (seed.challenge) throw CrawlFailure("SOURCE_BLOCKED", "The seed returned a challenge or access block.")
        if (seed.javascriptShell) {
            throw CrawlFailure("INSUFFICIENT_SEED", "The seed HTML contains no readable content or links. JavaScript rendering is not supported.")
        }
        pages[seedCandidate.url] = seed
        checks[seedCandidate.url] = "VALID"
        val scope = httpOrigin(seed.url)
        val context = analyzer.context(seed)

        fun merge(
            old: Candidate,
            incoming: Candidate,
        ): Candidate {
            val sourcePriority = mapOf(
                "SEED" to 0,
                "NAVIGATION" to 1,
                "MAIN_CONTENT" to 2,
                "SECONDARY_NAVIGATION" to 3,
                "SITEMAP" to 4,
                "FOOTER" to 5,
                "BASELINE" to 6,
            )
            val best = listOf(old, incoming)
                .sortedWith(
                    compareBy<Candidate> { sourcePriority[it.source] ?: 7 }
                        .thenByDescending { it.score }
                        .thenBy { it.parent.orEmpty() }
                        .thenBy { it.anchor },
                ).first()
            return best.copy(
                depth = listOfNotNull(old.depth, incoming.depth).filter { it <= budgets.maxDepth }.minOrNull(),
                baseline = old.baseline || incoming.baseline,
                seed = old.seed || incoming.seed,
                group = listOfNotNull(old.group, incoming.group).sorted().firstOrNull(),
                sectionOverview = old.sectionOverview || incoming.sectionOverview,
                structuralHub = old.structuralHub || incoming.structuralHub,
                score = maxOf(old.score, incoming.score),
            )
        }

        fun add(
            value: String,
            source: String,
            parent: String?,
            anchor: String,
            depth: Int?,
            previous: Boolean = false,
            group: String? = null,
        ) {
            budget.remainingMillis()
            val url = runCatching { fetcher.normalize(value) }.getOrNull() ?: return
            if (httpOrigin(url) != scope) return
            if (Regex("/(login|logout|cart|checkout|search)(/|$)", RegexOption.IGNORE_CASE).containsMatchIn(url.encodedPath) ||
                Regex(
                    "\\.(pdf|zip|png|jpg|jpeg|gif|svg|mp4|mp3|exe|css|js|ico|woff2?)(?:$)",
                    RegexOption.IGNORE_CASE,
                ).containsMatchIn(url.encodedPath)
            ) {
                return
            }
            val normalized = url.toString()
            // A page's own breadcrumbs/TOC cannot create inbound navigation authority for itself.
            if (normalized == parent) return
            val sectionOverview = group != null && clean(anchor).equals(group, ignoreCase = true)
            val candidate = Candidate(
                normalized,
                source,
                parent,
                anchor,
                depth,
                analyzer.preScore(url, anchor, source, seed, context) + if (sectionOverview) RankingWeights.SECTION_OVERVIEW else 0,
                previous,
                group = group,
                sectionOverview = sectionOverview,
                structuralHub = analyzer.isHub(normalized, seed.links),
            )
            (pages[normalized] ?: pages.values.firstOrNull { it.url.toString() == normalized })?.let { page ->
                // Improve discovery evidence without changing the original, already verified fetch key.
                val updated = page.copy(candidate = merge(page.candidate, candidate).copy(url = page.candidate.url))
                pages[page.candidate.url] = updated
                if (depth != null && depth <= budgets.maxDepth && (page.candidate.depth == null || depth < page.candidate.depth)) {
                    discoveredPaths.addLast(updated)
                }
                return
            }
            if (checks.containsKey(normalized)) return
            val existing = frontier[normalized]
            if (existing != null) {
                frontier[normalized] = merge(existing, candidate)
                return
            }
            if (!examined.add(sha256(normalized))) return
            if (examined.size > budgets.maxUrlEvaluations) {
                examined.remove(sha256(normalized))
                reasons.add("URL_EVALUATION_LIMIT")
                omitted++
                return
            }
            if (depth != null && depth > budgets.maxDepth) {
                reasons.add("DEPTH_LIMIT")
                omitted++
                return
            }
            observer?.candidate(candidate)
            sources.add(source)
            if (frontier.size >= budgets.maxCandidates) {
                val worst = frontier.values.maxWithOrNull(comparator)!!
                if (comparator.compare(candidate, worst) < 0) {
                    frontier.remove(worst.url)
                    frontier[normalized] = candidate
                }
                reasons.add("FRONTIER_LIMIT")
                omitted++
            } else {
                frontier[normalized] = candidate
            }
            peakFrontier = maxOf(peakFrontier, frontier.size)
        }

        fun expand(page: ExtractedPage) {
            val depth = page.candidate.depth ?: return
            page.links.sortedWith(compareBy<ObservedLink> { it.url }.thenBy { it.anchor }).forEach { link ->
                if (links >= budgets.maxLinksInspected) {
                    reasons.add("LINK_LIMIT")
                    return@forEach
                }
                links++
                add(link.url, link.source, page.url.toString(), link.anchor, depth + 1, group = link.group)
            }
        }
        baseline.forEach { resource ->
            if (httpOrigin(fetcher.normalize(resource.sourceUrl)) != scope) {
                checks[resource.sourceUrl] = "SOURCE_RELOCATED"
            } else {
                add(resource.sourceUrl, "BASELINE", null, resource.title, null, true)
            }
        }
        expand(seed)
        // Optional discovery cannot consume the HTML baseline's reserved request budget.
        var sitemapDone = false

        fun sitemaps() {
            sitemapDone = true
            val queue = ArrayDeque<String>()
            try {
                queue.addAll(
                    fetcher.sitemapHints(seed.url, budget).ifEmpty { listOf("$scope/sitemap.xml") }.take(config.discovery.maxSitemapDocuments),
                )
            } catch (failure: CrawlFailure) {
                propagateCancellation(failure)
                warnings.add("SITEMAP_UNAVAILABLE")
                return
            }
            val seen = hashSetOf<String>()
            var documents = 0
            var entries = 0
            while (queue.isNotEmpty() && documents < config.discovery.maxSitemapDocuments &&
                entries < config.discovery.maxSitemapEntries &&
                budget.requests + ContentThresholds.HTML_REQUEST_RESERVE < budget.maxRequests
            ) {
                val url = queue.removeFirst()
                if (!seen.add(url)) continue
                documents++
                try {
                    val response = fetcher.get(url, budget, scope, config.discovery.maxSitemapBytes)
                    if (response.status !in 200..299) {
                        warnings.add("SITEMAP_UNAVAILABLE")
                        continue
                    }
                    val parsed = parseOptionalSitemap(response, budget, config.discovery.maxSitemapEntries - entries)
                    entries += parsed.urls.size
                    if (parsed.index) {
                        parsed.urls.take(config.discovery.maxSitemapDocuments - documents).forEach(queue::addLast)
                    } else {
                        parsed.urls.forEach { add(it, "SITEMAP", null, "", null) }
                    }
                } catch (failure: CrawlFailure) {
                    propagateCancellation(failure)
                    warnings.add(failure.code)
                }
            }
            if (queue.isNotEmpty()) reasons.add("SITEMAP_DOCUMENT_LIMIT")
        }
        val fetchedFamilies = mutableMapOf<String, Int>()

        fun layer(candidate: Candidate) = when {
            candidate.baseline -> -1.0
            candidate.depth == null -> 1.5
            else -> candidate.depth.toDouble()
        }
        while (frontier.isNotEmpty() || discoveredPaths.isNotEmpty() || !sitemapDone) {
            budget.remainingMillis()
            while (discoveredPaths.isNotEmpty()) expand(discoveredPaths.removeFirst())
            if (attempted >= budgets.maxPages && frontier.values.none { it.baseline }) {
                reasons.add("PAGE_LIMIT")
                break
            }
            if (attempted >= budgets.maxPages) warnings.add("BASELINE_REVALIDATION")
            if (budget.requests >= budget.maxRequests) {
                reasons.add("REQUEST_BUDGET")
                break
            }
            val first = frontier.values.minWithOrNull(comparator)
            // Diversity affects ranking within a BFS layer, never baseline priority or HTML distance.
            val next = first?.let {
                frontier.values.filter { candidate -> layer(candidate) == layer(it) }.minWithOrNull(
                    compareByDescending<Candidate> { candidate ->
                        candidate.score -
                            RankingWeights.FAMILY_REPETITION_PENALTY * (fetchedFamilies[analyzer.family(candidate)] ?: 0)
                    }.then(comparator),
                )
            }
            if (!sitemapDone && (next == null || (!next.baseline && (next.depth ?: 2) > 1))) {
                sitemaps()
                continue
            }
            val candidate = next ?: break
            frontier.remove(candidate.url)
            attempted++
            fetchedFamilies.merge(analyzer.family(candidate), 1, Int::plus)
            progress(RunPhase.DISCOVERING, RunCounters(attempted, peakFrontier.toLong(), budget.requests.toLong()))
            try {
                val response = fetcher.get(candidate.url, budget, scope)
                if (response.status in setOf(404, 410)) {
                    checks[candidate.url] = "CONFIRMED_GONE"
                    observer?.rejected(candidate.url, "CONFIRMED_GONE")
                    continue
                }
                if (response.status !in 200..299) {
                    checks[candidate.url] = "HTTP_${response.status}"
                    observer?.rejected(candidate.url, "HTTP_${response.status}")
                    continue
                }
                val page = extractor.extract(response, candidate, budget, budgets.maxLinksInspected - links).copy(text = "")
                warnings.addAll(page.warnings)
                observer?.page(page)
                if (!analyzer.useful(page)) {
                    checks[candidate.url] = "INSUFFICIENT"
                    continue
                }
                checks[candidate.url] = "VALID"
                pages[candidate.url] = page
                if (candidate.depth != null) expand(page)
            } catch (e: CrawlFailure) {
                observer?.rejected(candidate.url, e.code)
                checks[candidate.url] = e.code
                warnings.add(e.code)
                if (e.code in setOf("ORIGIN_BLOCKED", "TIME_BUDGET", "CANCELLED")) throw e
            }
        }
        val children = pages.values.count { !it.candidate.seed && analyzer.useful(it) }
        if (seed.textLength < ContentThresholds.SUBSTANTIAL_TEXT_LENGTH &&
            !(
                seed.textLength >= ContentThresholds.MIN_SHORT_TEXT_LENGTH &&
                    seed.titleObserved && seed.title.length >= ContentThresholds.MIN_USEFUL_TITLE_LENGTH
            ) &&
            !(
                seed.title.length >= ContentThresholds.MIN_USEFUL_TITLE_LENGTH &&
                    seed.description.length >= ContentThresholds.MIN_USEFUL_DESCRIPTION_LENGTH &&
                    children >= ContentThresholds.MIN_SEED_CHILDREN
            )
        ) {
            throw CrawlFailure("INSUFFICIENT_SEED", "The seed lacks 50 readable characters with a descriptive title or enough useful child pages.")
        }
        if (seed.textLength < ContentThresholds.SUBSTANTIAL_TEXT_LENGTH) warnings.add("LIMITED_SEED_CONTENT")
        validateBaseline(baseline, checks, scope)
        progress(RunPhase.ANALYZING, RunCounters(attempted, peakFrontier.toLong(), budget.requests.toLong()))
        val resources = pages.values
            .filter(analyzer::useful)
            .map { analyzer.resource(it, seed, context) }
            .groupBy { it.primaryHtmlUrl }
            .values
            .map { group ->
                group
                    .sortedWith(
                        compareBy<Resource> {
                            if (it.section ==
                                "Overview"
                            ) {
                                0
                            } else {
                                1
                            }
                        }.thenBy { it.sourceUrl },
                    ).first()
                    .copy(aliases = group.flatMap { it.aliases }.distinct().sorted())
            }
        val families = resources.associate { resource ->
            resource.resourceKey to analyzer.family(
                pages.values.first { it.url.toString() == resource.primaryHtmlUrl }.candidate.copy(
                    structuralHub =
                        "STRUCTURAL_HUB" in resource.reasonCodes,
                ),
                resource.category,
            )
        }
        val selected = analyzer.select(resources, budgets.maxSelectedResources, families)
        observer?.selection(resources, selected)
        if (observer != null) {
            resources.forEach { resource ->
                val page = pages.values.first { it.url.toString() == resource.primaryHtmlUrl }
                observer.ranking(
                    resource,
                    analyzer.scoreComponents(page, seed, context),
                    families.getValue(resource.resourceKey),
                    if (selected.any { it.resourceKey == resource.resourceKey }) {
                        "SELECTED"
                    } else if (selected.any { it.contentHash == resource.contentHash }) {
                        "EXACT_DUPLICATE"
                    } else {
                        "LOWER_MARGINAL_COVERAGE"
                    },
                )
            }
        }
        if (selected.isEmpty()) throw CrawlFailure("NO_USEFUL_RESOURCES", "No useful resources were found.")
        val (bytes, rendered) = LlmsRenderer(config).render(seed, context, selected)
        if (rendered.size < selected.size) reasons.add("OUTPUT_LIMIT")
        return CrawlResult(
            seed.url.toString(),
            scope,
            context,
            rendered,
            checks,
            bytes,
            RunCoverage(attempted, pages.values.count(analyzer::useful).toLong(), peakFrontier.toLong(), omitted, reasons.sorted(), sources.sorted()),
            warnings.sorted(),
            started,
            clock.millis(),
        )
    }

    private fun propagateCancellation(failure: CrawlFailure) {
        if (failure.code in setOf("CANCELLED", "TIME_BUDGET")) throw failure
    }

    private fun parseOptionalSitemap(
        response: FetchResponse,
        budget: CrawlBudget,
        remainingEntries: Int,
    ): SitemapDiscovery.Entries = try {
        SitemapDiscovery(config).parse(response, budget, remainingEntries)
    } catch (_: XMLStreamException) {
        throw CrawlFailure("SITEMAP_INVALID", "The optional sitemap contains invalid XML.")
    } catch (_: UnknownFormatException) {
        throw CrawlFailure("SITEMAP_INVALID", "The optional sitemap has an unsupported format.")
    } catch (_: IOException) {
        // Parsing uses complete in-memory bytes; this includes malformed compressed content.
        throw CrawlFailure("SITEMAP_INVALID", "The optional sitemap could not be decoded.")
    }

    companion object {
        fun validateBaseline(
            baseline: List<Resource>,
            checks: Map<String, String>,
            scope: String,
        ) {
            val missing = baseline.firstOrNull { resource ->
                val evidence = checks[resource.sourceUrl]
                if (evidence == "SOURCE_RELOCATED") {
                    val oldOrigin = runCatching { URI(resource.sourceUrl).let { "${it.scheme}://${it.authority}" } }.getOrNull()
                    oldOrigin == scope
                } else {
                    evidence !in setOf("VALID", "CONFIRMED_GONE", "ROBOTS_DISALLOWED")
                }
            }
            if (missing != null) {
                val source = missing.sourceUrl
                    .substringBefore('?')
                    .substringBefore('#')
                    .take(200)
                val reason = checks[missing.sourceUrl] ?: "NOT_VISITED"
                throw CrawlFailure(
                    "INCOMPLETE_BASELINE",
                    "Could not revalidate $source ($reason). The previous accepted file is still available.",
                )
            }
        }
    }
}
