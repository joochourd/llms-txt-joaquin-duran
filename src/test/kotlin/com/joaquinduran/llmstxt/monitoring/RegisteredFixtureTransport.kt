package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.fetch.PageTransport
import okhttp3.HttpUrl
import java.time.Clock

/** Test-classpath only. Production networking retains its public connection boundary. */
class RegisteredFixtureTransport(
    private val clock: Clock,
    private val startedAt: Long = clock.millis(),
) : PageTransport {
    override fun get(
        url: HttpUrl,
        budget: CrawlBudget,
        maxBytes: Int,
    ): FetchResponse {
        if (url.host != "monitoring-fixture.example") return FetchResponse(url, 404, emptyMap(), byteArrayOf())
        val path = url.encodedPath
        if (path == "/robots.txt") {
            return FetchResponse(
                url,
                200,
                mapOf("Content-Type" to listOf("text/plain")),
                MonitoringSourceFixture.ROBOTS.toByteArray(),
            )
        }
        val page = MonitoringSourceFixture.page(path.removePrefix("/"), clock.millis() - startedAt)
        return FetchResponse(url, page.status, mapOf("Content-Type" to listOf(page.contentType)), page.body.toByteArray())
    }
}
