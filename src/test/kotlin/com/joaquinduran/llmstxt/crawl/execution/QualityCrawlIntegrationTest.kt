package com.joaquinduran.llmstxt.crawl.execution

import com.joaquinduran.llmstxt.crawl.analysis.QualityFixtures
import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.OriginBackoff
import com.joaquinduran.llmstxt.crawl.fetch.OriginCoordinator
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import com.joaquinduran.llmstxt.crawl.fetch.SafeFetcher
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import com.joaquinduran.llmstxt.crawl.model.CrawlResult
import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.AdmissionConfiguration
import com.joaquinduran.llmstxt.ingestion.CrawlRun
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.AdmissionStore
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoClients
import com.joaquinduran.llmstxt.persistence.dynamo.DynamoConfiguration
import com.joaquinduran.llmstxt.persistence.dynamo.LocalTableBootstrap
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import java.time.Clock
import java.util.UUID

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QualityCrawlIntegrationTest {
    private lateinit var client: DynamoDbClient
    private lateinit var store: AdmissionStore
    private val clock = Clock.systemUTC()
    private val config = CrawlConfiguration().apply { fetch.minRequestIntervalMillis = 0 }
    private val settings = AdmissionConfiguration().apply {
        maxPages = 18
        maxSelectedResources = 8
        maxCandidates = 30
    }

    @BeforeAll fun setup() {
        val dynamo = DynamoConfiguration.fromEnvironment(System.getenv()).apply { table = "llms-txt-quality-tests-${UUID.randomUUID()}" }
        dynamo.requireLocal()
        client = DynamoClients.create(dynamo)
        LocalTableBootstrap(client, dynamo).initialize()
        store = AdmissionStore(client, dynamo)
    }

    @AfterAll fun cleanup() {
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName(store.table).build())
        } finally {
            client.close()
        }
    }

    private fun execute(
        pages: Map<String, String>,
        seen: MutableList<String>,
        redirects: Map<String, String> = emptyMap(),
    ): CrawlResult {
        val transport = PageTransport { url, _, _ ->
            seen.add(url.encodedPath)
            when {
                url.encodedPath == "/robots.txt" -> FetchResponse(
                    url,
                    200,
                    mapOf("Content-Type" to listOf("text/plain")),
                    "User-agent: *\nAllow: /\n".toByteArray(),
                )

                redirects.containsKey(
                    url.encodedPath,
                ) -> FetchResponse(url, 302, mapOf("Location" to listOf(redirects.getValue(url.encodedPath))), byteArrayOf())

                pages.containsKey(
                    url.encodedPath,
                ) -> FetchResponse(url, 200, mapOf("Content-Type" to listOf("text/html")), pages.getValue(url.encodedPath).toByteArray())

                else -> FetchResponse(url, 404, emptyMap(), byteArrayOf())
            }
        }
        val origins = OriginCoordinator(config, OriginBackoff(store, config), clock)
        val identity = UrlIdentityResolver(settings).normalize("https://quality.example/")
        val now = clock.millis()
        val run = CrawlRun(
            UUID.randomUUID().toString(),
            identity.identityId,
            identity.normalizedSeedUrl,
            settings.pipelineVersion,
            identity.origin,
            now,
            now + 600000,
            now,
            0,
            null,
            null,
            settings.budgets(),
        )
        return CrawlRunner(SafeFetcher(transport, origins, config, clock, UrlIdentityResolver(settings)), config, clock)
            .run(run, emptyList(), CrawlBudget(60, 10000)) { _, _ -> }
    }

    @ParameterizedTest
    @ValueSource(strings = ["documentation", "news", "marketing", "developer", "insufficient"])
    fun `bounded production traversal supports the generic quality corpus`(name: String) {
        val seen = mutableListOf<String>()
        if (name == "insufficient") {
            val error = assertThrows(CrawlFailure::class.java) { execute(QualityFixtures.pages(name), seen) }
            assertEquals("INSUFFICIENT_SEED", error.code)
            assertTrue(error.message!!.contains("JavaScript"))
            assertEquals(listOf("/robots.txt", "/"), seen)
            return
        }
        val result = execute(QualityFixtures.pages(name), seen)
        assertTrue(result.coverage.pagesAttempted <= settings.maxPages)
        assertTrue(result.coverage.candidatesRetained <= settings.maxCandidates)
        assertTrue(seen.size <= settings.maxRequests)
        assertEquals(1, seen.count { it == "/" }, "The effective seed cannot be fetched again through a self-link.")
        val paths = result.resources.map { it.primaryHtmlUrl.toHttpUrl().encodedPath }
        assertEquals(8, result.resources.size)
        when (name) {
            "documentation" -> {
                assertTrue(paths.containsAll(listOf("/manual/start", "/manual/runtime", "/manual/reference")))
            }

            "news" -> {
                assertTrue(paths.containsAll(listOf("/sections", "/topics", "/events")))
                assertTrue(paths.count { it.startsWith("/dispatches/") } <= 4)
            }

            "marketing" -> {
                assertTrue(paths.containsAll(listOf("/solutions", "/plans", "/support", "/company")))
                assertTrue(paths.count { it.startsWith("/proof/") } <= 3)
            }

            "developer" -> {
                assertTrue(paths.containsAll(listOf("/manual/start", "/manual/storage", "/manual/identity", "/manual/transactions")))
            }
        }
    }

    @Test
    fun `repeated links preserve navigation evidence independently of link order`() {
        val content = "Useful structural information for readers. ".repeat(20)
        for (reverse in listOf(false, true)) {
            val links = listOf("<footer><a href='/target'>A footer link</a></footer>", "<nav><a href='/target'>Z navigation link</a></nav>")
            val seed = "<h1>A useful index</h1><main>$content</main>" + (if (reverse) links.reversed() else links).joinToString("")
            val seen = mutableListOf<String>()
            val result = execute(mapOf("/" to seed, "/target" to "<h1>Structural resource</h1><main>Target $content</main>"), seen)
            assertTrue("PRIMARY_NAVIGATION" in result.resources.single { it.primaryHtmlUrl.endsWith("/target") }.reasonCodes)
            assertEquals(1, seen.count { it == "/target" })
        }
    }

    @Test
    fun `self-linked table of contents cannot upgrade an inbound content link to primary navigation`() {
        val content = "Useful distinct information for the selected resource. ".repeat(20)
        val result = execute(
            mapOf(
                "/" to "<h1>Index</h1><main>Seed $content<a href='/leaf'>A content link</a></main>",
                "/leaf" to
                    (
                        "<nav><a href='/leaf#first'>First section</a><a href='/leaf#second'>Second section</a></nav>" +
                            "<main><h1>Leaf resource</h1>Leaf $content</main>"
                    ),
            ),
            mutableListOf(),
        )
        assertFalse("PRIMARY_NAVIGATION" in result.resources.single { it.primaryHtmlUrl.endsWith("/leaf") }.reasonCodes)
    }

    @Test
    fun `stronger links to an already fetched redirect preserve its source key and bounded evidence`() {
        val content = "Useful verified information about distinct workflows. ".repeat(20)
        val seen = mutableListOf<String>()
        val result = execute(
            mapOf(
                "/" to "<h1>Index</h1><main>Seed $content<a href='/alias'>Alias</a><a href='/other'>Other</a></main>",
                "/destination" to "<main><h1>Destination</h1>Destination $content</main>",
                "/other" to "<nav><a href='/destination'>Destination navigation</a></nav><main><h1>Other</h1>Other $content</main>",
            ),
            seen,
            mapOf("/alias" to "/destination"),
        )
        val resource = result.resources.single { it.primaryHtmlUrl.endsWith("/destination") }
        assertEquals("https://quality.example/alias", resource.sourceUrl)
        assertEquals("VALID", result.checks[resource.sourceUrl])
        assertTrue("PRIMARY_NAVIGATION" in resource.reasonCodes)
        assertEquals(1, seen.count { it == "/destination" })
        assertTrue(result.coverage.pagesUseful <= result.coverage.pagesAttempted)
    }
}
