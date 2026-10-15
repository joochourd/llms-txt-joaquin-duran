package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.model.Candidate
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.ExtractedPage
import io.micronaut.json.JsonMapper
import io.micronaut.json.tree.JsonNode
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import tools.jackson.core.JacksonException
import tools.jackson.core.JsonParser
import tools.jackson.core.JsonToken
import tools.jackson.core.ObjectReadContext
import tools.jackson.core.StreamReadConstraints
import tools.jackson.core.json.JsonFactory
import java.security.MessageDigest
import java.util.HexFormat

/** Reads metadata, catalogs and OpenAPI descriptions; never resolves $ref or executes described operations. */
internal class StructuredJsonExtractor(private val configuration: CrawlConfiguration) {
    private companion object {
        const val MAX_DEPTH = 64
        const val MAX_TOKENS = 50_000L
        const val MAX_NUMBER_CHARACTERS = 1000
        const val MAX_NUMBER_SCALE = 1000L
        val COLLECTION_FIELDS = listOf("items", "products", "categories", "itemListElement", "@graph")
        val HTTP_METHODS = setOf("get", "put", "post", "delete", "options", "head", "patch", "trace", "query")
    }

    fun extract(
        response: FetchResponse,
        candidate: Candidate,
        budget: CrawlBudget,
        linksRemaining: Int,
    ): ExtractedPage {
        val root = parse(response.bytes, budget)
        if (!root.isObject) throw CrawlFailure("UNSUPPORTED_JSON_STRUCTURE", "JSON must contain a recognized document or catalog object.")
        val apiVersion = root.text("openapi")
        val isOpenApi = apiVersion.startsWith("3.") || root.text("swagger") == "2.0"
        val metadata = if (isOpenApi) root.get("info") else root
        val title = metadata?.text("title").orEmpty().ifEmpty { metadata?.text("name").orEmpty() }
        if (title.isBlank()) throw CrawlFailure("UNSUPPORTED_JSON_STRUCTURE", "JSON lacks recognized OpenAPI or titled document/catalog metadata.")

        val document = Jsoup.parse("", response.url.toString())
        document
            .head()
            .appendElement("meta")
            .attr("name", "description")
            .attr("content", metadata?.text("description").orEmpty())
        val main = document.body().appendElement("main")
        main.appendElement("h1").text(title)
        addMetadata(metadata, main, budget)
        addDocumentation(root, main)
        if (isOpenApi) addOperations(root, main, budget)
        for (field in COLLECTION_FIELDS) {
            val items = root.get(field)?.takeIf { it.isArray } ?: continue
            for (entry in items.values()) {
                budget.remainingMillis()
                if (!entry.isObject) continue
                val item = entry.get("item")?.takeIf { it.isObject } ?: entry
                val heading = item.text("title").ifEmpty { item.text("name") }
                if (heading.isBlank()) continue
                main.appendElement("h2").text(heading)
                addMetadata(item, main, budget)
            }
        }
        val rendered = document.outerHtml().toByteArray()
        if (rendered.size > configuration.fetch.maxResponseBytes) {
            throw CrawlFailure("CONTENT_STRUCTURE_LIMIT", "The complete JSON description exceeds its processing limit.")
        }
        val html = response.copy(headers = mapOf("Content-Type" to listOf("text/html; charset=UTF-8")), bytes = rendered)
        return HtmlExtractor(configuration).extract(html, candidate, budget, linksRemaining).copy(
            sourceFormat = "JSON",
            contentHash = fingerprint(root, budget),
            structured = structuredMetadata(root),
        )
    }

    /** Length framing preserves value types and boundaries without expanding numbers or serializing a second full document. */
    private fun fingerprint(
        root: JsonNode,
        budget: CrawlBudget,
    ): String {
        val digest = MessageDigest.getInstance("SHA-256")

        fun token(value: String) {
            val bytes = value.toByteArray()
            digest.update("${bytes.size}:".toByteArray())
            digest.update(bytes)
        }

        fun visit(node: JsonNode) {
            budget.remainingMillis()
            when {
                node.isObject -> {
                    token("OBJECT")
                    token(node.size().toString())
                    for ((name, value) in node.entries().sortedBy { it.key }) {
                        token("KEY")
                        token(name)
                        visit(value)
                    }
                    token("END_OBJECT")
                }

                node.isArray -> {
                    token("ARRAY")
                    token(node.size().toString())
                    node.values().forEach(::visit)
                    token("END_ARRAY")
                }

                node.isString -> {
                    token("STRING")
                    token(node.stringValue)
                }

                node.isNumber -> {
                    token("NUMBER")
                    token(node.numberValue.toString())
                }

                node.isBoolean -> {
                    token(if (node.booleanValue) "TRUE" else "FALSE")
                }

                else -> {
                    token("NULL")
                }
            }
        }

        visit(root)
        return HexFormat.of().formatHex(digest.digest())
    }

    private fun structuredMetadata(root: JsonNode): String {
        val fields = mutableMapOf<String, JsonNode>()
        for (field in listOf("@type", "openapi", "swagger")) {
            val value = root.get(field) ?: continue
            if (value.isString && value.stringValue.length <= ContentThresholds.MAX_ANCHOR_LENGTH) fields[field] = value
        }
        return JsonMapper.createDefault().writeValueAsBytes(JsonNode.createObjectNode(fields)).toString(Charsets.UTF_8)
    }

    private fun addMetadata(
        node: JsonNode?,
        target: Element,
        budget: CrawlBudget,
    ) {
        budget.remainingMillis()
        if (node == null || !node.isObject) return
        for (field in listOf("description", "text", "articleBody")) {
            node.text(field).takeIf { it.isNotBlank() }?.let { target.appendElement("p").text(it) }
        }
        val url = node.text("url").ifEmpty { node.text("@id") }
        if (url.isNotBlank()) target.appendElement("a").attr("href", url).text(node.text("name").ifEmpty { node.text("title") })
    }

    private fun addDocumentation(
        node: JsonNode,
        target: Element,
    ) {
        val docs = node.get("externalDocs")?.takeIf { it.isObject } ?: return
        val url = docs.text("url")
        if (url.isNotBlank()) target.appendElement("a").attr("href", url).text(docs.text("description").ifEmpty { "Documentation" })
    }

    private fun addOperations(
        root: JsonNode,
        target: Element,
        budget: CrawlBudget,
    ) {
        val paths = root.get("paths")?.takeIf { it.isObject } ?: return
        for ((path, item) in paths.entries()) {
            budget.remainingMillis()
            if (!path.startsWith('/') || !item.isObject) continue
            for ((method, operation) in item.entries()) {
                budget.remainingMillis()
                if (method !in HTTP_METHODS || !operation.isObject) continue
                target.appendElement("h2").text("${method.uppercase()} $path")
                operation.text("summary").takeIf { it.isNotBlank() }?.let { target.appendElement("p").text(it) }
                operation.text("description").takeIf { it.isNotBlank() }?.let { target.appendElement("p").text(it) }
                addDocumentation(operation, target)
            }
        }
    }

    private fun parse(
        bytes: ByteArray,
        budget: CrawlBudget,
    ): JsonNode = try {
        val constraints = StreamReadConstraints
            .builder()
            .maxNestingDepth(MAX_DEPTH)
            .maxTokenCount(MAX_TOKENS)
            .maxStringLength(configuration.fetch.maxResponseBytes)
            .maxNumberLength(MAX_NUMBER_CHARACTERS)
            .build()
        JsonFactory.builder().streamReadConstraints(constraints).build().createParser(ObjectReadContext.empty(), bytes).use { parser ->
            if (parser.nextToken() == null) throw invalid()
            val root = readValue(parser, budget)
            if (parser.nextToken() != null) throw invalid()
            root
        }
    } catch (_: JacksonException) {
        throw invalid()
    }

    private fun readValue(
        parser: JsonParser,
        budget: CrawlBudget,
    ): JsonNode {
        budget.remainingMillis()
        return when (parser.currentToken()) {
            JsonToken.START_OBJECT -> {
                val fields = sortedMapOf<String, JsonNode>()
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    if (parser.currentToken() != JsonToken.PROPERTY_NAME) throw invalid()
                    val name = parser.currentName()
                    if (fields.containsKey(name) || parser.nextToken() == null) throw invalid()
                    fields[name] = readValue(parser, budget)
                }
                JsonNode.createObjectNode(fields)
            }

            JsonToken.START_ARRAY -> {
                val values = mutableListOf<JsonNode>()
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    if (parser.currentToken() == null) throw invalid()
                    values.add(readValue(parser, budget))
                }
                JsonNode.createArrayNode(values)
            }

            JsonToken.VALUE_STRING -> {
                JsonNode.createStringNode(parser.string)
            }

            JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_NUMBER_FLOAT -> {
                val number = parser.decimalValue
                if (number.scale().toLong() !in -MAX_NUMBER_SCALE..MAX_NUMBER_SCALE) throw invalid()
                JsonNode.createNumberNode(number)
            }

            JsonToken.VALUE_TRUE -> {
                JsonNode.createBooleanNode(true)
            }

            JsonToken.VALUE_FALSE -> {
                JsonNode.createBooleanNode(false)
            }

            JsonToken.VALUE_NULL -> {
                JsonNode.nullNode()
            }

            else -> {
                throw invalid()
            }
        }
    }

    private fun JsonNode.text(field: String): String = if (isObject) get(field)?.takeIf { it.isString }?.stringValue.orEmpty() else ""

    private fun invalid() = CrawlFailure("INVALID_SOURCE_JSON", "The JSON source is malformed or exceeds its bounded parsing limits.")
}
