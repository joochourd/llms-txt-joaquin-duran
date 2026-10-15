package com.joaquinduran.llmstxt.crawl.discovery

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.fetch.FetchResponse
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import crawlercommons.sitemaps.SiteMap
import crawlercommons.sitemaps.SiteMapIndex
import crawlercommons.sitemaps.SiteMapParser
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.net.URI
import java.util.zip.GZIPInputStream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants

class SitemapDiscovery(
    private val config: CrawlConfiguration,
) {
    data class Entries(
        val index: Boolean,
        val urls: List<String>,
    )

    fun parse(
        response: FetchResponse,
        budget: CrawlBudget,
        remainingEntries: Int,
    ): Entries {
        var bytes = response.bytes
        if (bytes.size >= 2 && bytes[0] == 0x1f.toByte() && bytes[1] == 0x8b.toByte()) {
            val output = ByteArrayOutputStream()
            GZIPInputStream(ByteArrayInputStream(bytes)).use { stream ->
                val buffer = ByteArray(8192)
                while (true) {
                    budget.remainingMillis()
                    val bytesRead = stream.read(buffer)
                    if (bytesRead < 0) break
                    if (output.size() + bytesRead >
                        config.discovery.maxSitemapBytes
                    ) {
                        throw CrawlFailure("SITEMAP_BODY_LIMIT", "The decompressed sitemap exceeds its byte limit.")
                    }
                    output.write(buffer, 0, bytesRead)
                }
            }
            bytes = output.toByteArray()
        }
        val factory = XMLInputFactory.newFactory()
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
        factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false)
        factory.xmlResolver = javax.xml.stream.XMLResolver { _, _, _, _ -> throw CrawlFailure("UNSAFE_XML", "External XML entities are forbidden.") }
        val reader = factory.createXMLStreamReader(ByteArrayInputStream(bytes))
        var count = 0
        try {
            while (reader.hasNext()) {
                budget.remainingMillis()
                val event = reader.next()
                if (event == XMLStreamConstants.DTD ||
                    event == XMLStreamConstants.ENTITY_REFERENCE
                ) {
                    throw CrawlFailure("UNSAFE_XML", "DTD and entities are forbidden.")
                }
                if (event == XMLStreamConstants.START_ELEMENT && reader.localName == "loc" &&
                    ++count > remainingEntries
                ) {
                    throw CrawlFailure("SITEMAP_ENTRY_LIMIT", "The sitemap entry budget was exceeded.")
                }
            }
        } finally {
            reader.close()
        }
        budget.remainingMillis()
        // Only the byte-array overload; Crawler Commons never performs networking.
        val parsed = SiteMapParser(false).parseSiteMap("application/xml", bytes, URI(response.url.toString()).toURL())
        budget.remainingMillis()
        val urls = if (parsed is SiteMapIndex) {
            parsed.sitemaps.map {
                it.url.toString()
            }
        } else {
            (parsed as SiteMap).siteMapUrls.map { it.url.toString() }
        }
        return Entries(parsed is SiteMapIndex, urls.sorted().take(remainingEntries))
    }
}
