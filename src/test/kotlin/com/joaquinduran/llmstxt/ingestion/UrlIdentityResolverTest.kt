package com.joaquinduran.llmstxt.ingestion

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class UrlIdentityResolverTest {
    private val settings = AdmissionConfiguration()
    private val urls = UrlIdentityResolver(settings)

    @Test
    fun `stable identity vectors remain compatible with previously persisted URL intents`() {
        val knownIdentities = mapOf(
            "https://EXAMPLE.com:443/docs?utm_source=email#intro" to
                "86eac60e319e46bd391e3be6981f64e4908c013bfffdf4b7f9dce2de523304cc",
            "HTTPS://BÜCHER.example:443/a/../Docs/?b=2&utm_source=x&a=1&fbclid=z&a=3#part" to
                "a22ff48da94ee223140725d2946bb5d584eb67de8f4470f046a8778a368a4bef",
            "http://example.com/docs?b=2&a=1" to
                "bd8a43d67133871d962cece4a03fe565e333609698273d5d3f95be6e36928a8e",
        )

        knownIdentities.forEach { (input, expectedIdentityId) ->
            val identity = urls.normalize(input)

            assertEquals(expectedIdentityId, identity.identityId)
            assertEquals("0189ccd949722328e0bba67e922ad56920ffc1e0f1939bab8d577a45f4a1d219", identity.profileHash)
        }
    }

    @Test
    fun `canonicalization handles IDNA ports fragments dot paths and explicit tracking`() {
        assertEquals(
            "https://xn--bcher-kva.example/Docs/?b=2&a=1&a=3",
            urls.normalize(" HTTPS://BÜCHER.example:443/a/../Docs/?b=2&utm_source=x&a=1&fbclid=z&a=3#part ").normalizedSeedUrl,
        )
        assertEquals(urls.normalize("example.com").identityId, urls.normalize("https://EXAMPLE.com:443/#top").identityId)
    }

    @Test
    fun `semantic differences and query order do not merge identities`() {
        val values = listOf(
            "http://example.com/Docs",
            "https://example.com/Docs",
            "https://www.example.com/Docs",
            "https://example.com/docs",
            "https://example.com/Docs/",
            "https://example.com/Docs?a=1&b=2",
            "https://example.com/Docs?b=2&a=1",
        )
        assertEquals(values.size, values.map { urls.normalize(it).identityId }.toSet().size)
        val id = urls.normalize(values.first()).identityId
        settings.maxPages = 50
        settings.pipelineVersion = "other-version"
        assertEquals(id, urls.normalize(values.first()).identityId)
        assertEquals(64, id.length)
    }

    @Test
    fun `unsafe or malformed inputs and oversized URLs are rejected without networking`() {
        listOf(
            "",
            "ftp://example.com",
            "mailto:a@example.com",
            "https://user:pass@example.com",
            "http://127.0.0.1/",
            "http://[::1]/",
            "http://localhost",
            "http://api.internal",
            "https://example.com:8080/",
            "https://exa mple.com",
            "https://example.com/\nfoo",
            "https://example.com\\evil",
        ).forEach {
            assertThrows(InvalidSeed::class.java, { urls.normalize(it) }, it)
        }
        assertThrows(InvalidSeed::class.java) { urls.normalize("https://example.com/" + "é".repeat(2048)) }
    }
}
