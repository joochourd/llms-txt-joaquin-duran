package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class SourceExtractorTest {
    private val extractor = SourceExtractor(CrawlConfiguration())
    private val usefulText = "Practical instructions for installing and configuring this application safely."

    private fun extract(
        body: String,
        mime: String? = "text/html",
        path: String = "/docs/",
        linksRemaining: Int = 100,
    ) = extractor.extract(
        FetchResponse(
            "https://example.com$path".toHttpUrl(),
            200,
            mime?.let { mapOf("Content-Type" to listOf(it)) }.orEmpty(),
            body.toByteArray(),
        ),
        Candidate("https://example.com$path", "SEED", null, "", 0, 0.0, seed = true),
        CrawlBudget(60, 5000),
        linksRemaining,
    )

    @ParameterizedTest
    @ValueSource(strings = ["text/markdown", "text/x-markdown", "text/plain"])
    fun `Markdown headings links and complete content participate in extraction`(mime: String) {
        val original = extract("# Installation guide\n\n$usefulText\n\n[Next step](next.md)\n\nVersion 2026", mime, "/docs/start.md")
        val changed = extract("# Installation guide\n\n$usefulText\n\n[Next step](next.md)\n\nVersion 2027", mime, "/docs/start.md")

        assertEquals("Installation guide", original.title)
        assertEquals("https://example.com/docs/next.md", original.links.single().url)
        assertTrue(DeterministicAnalyzer().useful(original.copy(text = "")))
        assertNotEquals(original.contentHash, changed.contentHash)
    }

    @ParameterizedTest
    @ValueSource(strings = ["text/plain", "application/octet-stream", "missing", " Text/HTML ; charset=UTF-8"])
    fun `recognizable complete HTML survives missing generic or irregular media type`(mime: String) {
        val page = extract(
            "<!doctype html><html><body><h1>Installation guide</h1><main>$usefulText</main></body></html>",
            mime.takeUnless {
                it ==
                    "missing"
            },
        )

        assertEquals("Installation guide", page.title)
        assertTrue(page.text.contains(usefulText))
    }

    @Test
    fun `OpenAPI extracts operation descriptions and documentation without inventing endpoint fetches`() {
        val page = extract(
            """
            {"openapi":"3.1.0","info":{"title":"Payments API reference","description":"$usefulText"},
            "paths":{"/charges":{"post":{"summary":"Create a charge","description":"Observed payment operation"}}},
            "externalDocs":{"url":"guide.md"},"components":{"schemas":{"Charge":{"${'$'}ref":"http://127.0.0.1/private"}}}}
            """.trimIndent(),
            "application/json",
        )

        assertEquals("Payments API reference", page.title)
        assertEquals("DOCUMENTATION", DeterministicAnalyzer().context(page).type)
        assertTrue(page.text.contains("POST /charges"))
        assertEquals(listOf("https://example.com/docs/guide.md"), page.links.map { it.url })
    }

    @Test
    fun `catalog metadata and explicit product links are extracted in deterministic order`() {
        val original = extract(
            """
            {"title":"Equipment catalog","description":"$usefulText","products":[
            {"name":"Starter kit","description":"A useful starter kit for new installations.","url":"/products/starter"}]}
            """.trimIndent(),
            "application/json",
        )
        val reordered = extract(
            """
            {"products":[{"url":"/products/starter","description":"A useful starter kit for new installations.","name":"Starter kit"}],
            "description":"$usefulText","title":"Equipment catalog"}
            """.trimIndent(),
            "application/json",
        )

        assertEquals("Equipment catalog", original.title)
        assertEquals("https://example.com/products/starter", original.links.single().url)
        assertEquals(original.contentHash, reordered.contentHash)
    }

    @Test
    fun `JSON source changes outside the displayed description still change its observation hash`() {
        val original = extract("""{"name":"Equipment catalog","description":"$usefulText","revision":1}""", "application/ld+json")
        val changed = extract("""{"name":"Equipment catalog","description":"$usefulText","revision":2}""", "application/ld+json")

        assertNotEquals(original.contentHash, changed.contentHash)
    }

    @Test
    fun `invalid JSON and unrecognized JSON data have explicit distinct failures`() {
        val invalid = assertThrows(CrawlFailure::class.java) { extract("{broken", "application/json") }
        val unrecognized = assertThrows(CrawlFailure::class.java) { extract("""{"status":"ok","count":5}""", "application/json") }

        assertEquals("INVALID_SOURCE_JSON", invalid.code)
        assertEquals("UNSUPPORTED_JSON_STRUCTURE", unrecognized.code)
    }

    @Test
    fun `excessive JSON nesting fails within a bounded parser instead of overflowing the stack`() {
        val body = "[".repeat(100) + "0" + "]".repeat(100)

        assertEquals("INVALID_SOURCE_JSON", assertThrows(CrawlFailure::class.java) { extract(body, "application/json") }.code)
    }

    @ParameterizedTest
    @ValueSource(strings = ["application/pdf", "image/png", "application/zip"])
    fun `unsupported binary formats are never interpreted as HTML despite embedded markup`(mime: String) {
        val failure = assertThrows(CrawlFailure::class.java) { extract("<html><h1>Misleading embedded document</h1></html>", mime) }

        assertEquals("UNSUPPORTED_CONTENT", failure.code)
    }

    @Test
    fun `short titled content is useful while challenges and empty JavaScript shells remain excluded`() {
        val short = extract("<h1>Installation guide</h1><main>${"Useful setup guidance. ".repeat(3)}</main>")
        val challenge = extract("<title>Access denied</title><main>$usefulText</main>")
        val shell = extract("<title>Installation guide</title><meta name='description' content='$usefulText'><script src='/app.js'></script>")
        val analyzer = DeterministicAnalyzer()

        assertTrue(analyzer.useful(short))
        assertFalse(analyzer.useful(challenge))
        assertFalse(analyzer.useful(shell))
    }

    @Test
    fun `generic content type recognizes structured JSON and Markdown without accepting arbitrary plain text`() {
        val metadata = extract("""{"name":"Installation guide","description":"$usefulText"}""", null)
        val markdown = extract("# Installation guide\n\n$usefulText", "text/plain")
        val arbitrary = assertThrows(CrawlFailure::class.java) { extract(usefulText, "text/plain") }

        assertEquals("Installation guide", metadata.title)
        assertEquals("Installation guide", markdown.title)
        assertEquals("UNSUPPORTED_CONTENT", arbitrary.code)
    }

    @Test
    fun `Markdown rejects excessive structural nesting and respects the source byte limit`() {
        val nested = "> ".repeat(100) + "Installation guidance"
        val oversized = "# Installation guide\n" + "x".repeat(2_097_152)

        assertEquals("CONTENT_STRUCTURE_LIMIT", assertThrows(CrawlFailure::class.java) { extract(nested, "text/markdown") }.code)
        assertEquals("BODY_LIMIT", assertThrows(CrawlFailure::class.java) { extract(oversized, "text/markdown") }.code)
    }

    @Test
    fun `malformed trailing duplicate and overly complex JSON is never accepted as a partial observation`() {
        val invalidDocuments = listOf(
            """{"name":"First","name":"Second"}""",
            """{"name":"Installation guide","description":"$usefulText","value":1e999999999}""",
            """{"name":"Installation guide","description":"$usefulText"} {}""",
            """{"name":"Installation guide","description":"$usefulText","values":[${List(50_001) { "0" }.joinToString(",")}]}""",
        )

        invalidDocuments.forEach { body ->
            assertEquals("INVALID_SOURCE_JSON", assertThrows(CrawlFailure::class.java) { extract(body, "application/json") }.code)
        }
    }

    @Test
    fun `JSON fingerprinting preserves scalar types and never expands compact numeric exponents`() {
        val number = extract("""{"name":"Installation guide","description":"$usefulText","value":1}""", "application/json")
        val string = extract("""{"name":"Installation guide","description":"$usefulText","value":"1"}""", "application/json")
        val compactNumbers = List(1000) { "1e1000" }.joinToString(",")
        val compact = extract("""{"name":"Installation guide","description":"$usefulText","values":[$compactNumbers]}""", "application/json")

        assertNotEquals(number.contentHash, string.contentHash)
        assertEquals(64, compact.contentHash.length)
    }

    @Test
    fun `JSON keys resembling structural markers cannot collide across object boundaries`() {
        val nested = extract(
            """{"name":"Installation guide","description":"$usefulText","value":{"A":{"END_OBJECT":"STRING"}}}""",
            "application/json",
        )
        val adjacent = extract(
            """{"name":"Installation guide","description":"$usefulText","value":{"A":{},"STRING":"END_OBJECT"}}""",
            "application/json",
        )

        assertNotEquals(nested.contentHash, adjacent.contentHash)
    }

    @Test
    fun `recognized JSON still observes the remaining link inspection budget`() {
        val page = extract(
            """
            {"title":"Installation guide","description":"$usefulText","products":[
            {"name":"First guide","url":"/first"},{"name":"Second guide","url":"/second"}]}
            """.trimIndent(),
            "application/json",
            linksRemaining = 1,
        )

        assertEquals(listOf("https://example.com/first"), page.links.map { it.url })
    }

    @Test
    fun `error pages are excluded without rejecting documentation discussing server errors`() {
        val error = extract("<h1>Service unavailable</h1><main>$usefulText</main>")
        val documentation = extract("<h1>Troubleshooting guide</h1><main>Handling service unavailable responses. $usefulText</main>")

        assertFalse(DeterministicAnalyzer().useful(error))
        assertTrue(DeterministicAnalyzer().useful(documentation))
    }

    @Test
    fun `short client redirect placeholders are not useful source documents`() {
        val placeholder = "<title>Redirecting…</title><main>You are being redirected to the documentation website. Please wait.</main>"

        val failure = assertThrows(CrawlFailure::class.java) { extract(placeholder) }
        val guide = extract("<h1>Redirecting requests guide</h1><main>$usefulText</main>")

        assertEquals("SOURCE_REDIRECT_PLACEHOLDER", failure.code)
        assertTrue(DeterministicAnalyzer().useful(guide))
    }

    @Test
    fun `source parsing preserves cancellation before any format-specific work`() {
        val budget = CrawlBudget(60, 5000).apply { cancelled.set(true) }
        val response = FetchResponse("https://example.com/".toHttpUrl(), 200, mapOf("Content-Type" to listOf("application/json")), "{}".toByteArray())

        val failure = assertThrows(CrawlFailure::class.java) {
            extractor.extract(response, Candidate(response.url.toString(), "SEED", null, "", 0, 0.0), budget, 10)
        }

        assertEquals("CANCELLED", failure.code)
    }
}
