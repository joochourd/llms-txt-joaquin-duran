package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import org.commonmark.node.Node
import org.commonmark.parser.Parser
import org.commonmark.renderer.html.HtmlRenderer
import java.util.ArrayDeque

/** CommonMark rendering is in memory only; Jsoup extracts text without executing or loading HTML. */
internal class MarkdownExtractor(private val configuration: CrawlConfiguration) {
    private companion object {
        const val MAX_NODES = 20_000
        const val MAX_DEPTH = 64
    }

    fun extract(
        response: FetchResponse,
        candidate: Candidate,
        budget: CrawlBudget,
        linksRemaining: Int,
        text: String,
    ): ExtractedPage {
        budget.remainingMillis()
        val document = Parser.builder().build().parse(text)
        validateStructure(document, budget)
        val rendered = HtmlRenderer
            .builder()
            .build()
            .render(document)
            .toByteArray()
        if (rendered.size > configuration.fetch.maxResponseBytes) {
            throw CrawlFailure("CONTENT_STRUCTURE_LIMIT", "The complete Markdown representation exceeds its processing limit.")
        }
        val html = response.copy(headers = mapOf("Content-Type" to listOf("text/html; charset=UTF-8")), bytes = rendered)
        return HtmlExtractor(configuration).extract(html, candidate, budget, linksRemaining).copy(sourceFormat = "MARKDOWN")
    }

    private fun validateStructure(
        document: Node,
        budget: CrawlBudget,
    ) {
        val pending = ArrayDeque<Pair<Node, Int>>()
        pending.add(document to 0)
        var nodes = 1
        while (pending.isNotEmpty()) {
            budget.remainingMillis()
            val (node, depth) = pending.removeLast()
            var child = node.firstChild
            while (child != null) {
                if (++nodes > MAX_NODES || depth + 1 > MAX_DEPTH) {
                    throw CrawlFailure("CONTENT_STRUCTURE_LIMIT", "The Markdown document is too deeply nested or complex.")
                }
                pending.add(child to depth + 1)
                child = child.next
            }
        }
    }
}
