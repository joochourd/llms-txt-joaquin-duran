package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction

/** Routes complete source bodies; format detection never performs additional network requests. */
class SourceExtractor(private val configuration: CrawlConfiguration) {
    private companion object {
        const val SNIFF_PREFIX_CHARACTERS = 4096
        const val MEDIA_TYPE_LABEL_CHARACTERS = 100
    }

    private val html = HtmlExtractor(configuration)
    private val markdown = MarkdownExtractor(configuration)
    private val json = StructuredJsonExtractor(configuration)

    fun extract(
        response: FetchResponse,
        candidate: Candidate,
        budget: CrawlBudget,
        linksRemaining: Int,
    ): ExtractedPage {
        budget.remainingMillis()
        if (response.bytes.size > configuration.fetch.maxResponseBytes) {
            throw CrawlFailure("BODY_LIMIT", "The complete source exceeds its byte limit.")
        }
        val mime = response
            .header("Content-Type")
            ?.substringBefore(';')
            ?.trim()
            ?.lowercase()
            .orEmpty()
        if (mime in setOf("text/html", "application/xhtml+xml")) return html.extract(response, candidate, budget, linksRemaining)
        if (mime == "application/json" || mime.endsWith("+json")) return json.extract(response, candidate, budget, linksRemaining)
        if (mime in setOf("text/markdown", "text/x-markdown")) {
            return markdown.extract(response, candidate, budget, linksRemaining, decodeText(response))
        }
        if (mime in setOf("", "text/plain", "application/octet-stream")) {
            val text = decodeText(response)
            val beginning = text.trimStart().take(SNIFF_PREFIX_CHARACTERS)
            val extracted = when {
                Regex("^<(?:!doctype\\s+html|html|head|body|main|article|h1)(?:\\s|>)", RegexOption.IGNORE_CASE).containsMatchIn(beginning) -> {
                    html.extract(response.copy(headers = mapOf("Content-Type" to listOf("text/html"))), candidate, budget, linksRemaining)
                }

                beginning.startsWith('{') || beginning.startsWith('[') -> {
                    json.extract(response, candidate, budget, linksRemaining)
                }

                response.url.encodedPath
                    .lowercase()
                    .let { it.endsWith(".md") || it.endsWith(".markdown") } ||
                    Regex("^#{1,6}\\s+\\S").containsMatchIn(beginning) -> {
                    markdown.extract(response, candidate, budget, linksRemaining, text)
                }

                else -> {
                    throw unsupported(mime)
                }
            }
            return extracted.copy(warnings = extracted.warnings + "CONTENT_TYPE_FALLBACK")
        }
        throw unsupported(mime)
    }

    private fun decodeText(response: FetchResponse): String = try {
        val text = Charsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(response.bytes))
            .toString()
            .removePrefix("\uFEFF")
        if (text.any { it == '\u0000' || (it.code < 32 && it !in "\n\r\t") }) throw unsupported("binary data")
        text
    } catch (_: CharacterCodingException) {
        throw unsupported("non-UTF-8 text")
    }

    private fun unsupported(mime: String): CrawlFailure {
        val label = mime.ifEmpty { "missing Content-Type" }.filter { it.code in 32..126 }.take(MEDIA_TYPE_LABEL_CHARACTERS)
        return CrawlFailure("UNSUPPORTED_CONTENT", "Received $label; supported sources are HTML, Markdown and recognized JSON structures.")
    }
}
