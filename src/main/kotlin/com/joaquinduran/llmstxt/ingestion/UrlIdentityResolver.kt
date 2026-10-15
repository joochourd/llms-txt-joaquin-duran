package com.joaquinduran.llmstxt.ingestion

import jakarta.inject.Singleton
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import java.security.MessageDigest
import java.util.Locale

class InvalidSeed(
    val error: DomainError,
) : RuntimeException(error.message)

@Singleton
class UrlIdentityResolver(
    private val configuration: AdmissionConfiguration,
) {
    /** Stable URL intent identity; this step neither resolves DNS nor hashes downloaded content. */
    fun normalize(input: String): CrawlIdentity {
        val url = parsePublicSeed(input)
        val normalized = removeTracking(url)
        if (normalized.toByteArray(Charsets.UTF_8).size > configuration.maxUrlBytes) invalid()
        return createIdentity(url, normalized)
    }

    private fun parsePublicSeed(input: String): HttpUrl {
        val trimmed = input.trim()
        if (trimmed.isEmpty() || trimmed.any { it.isISOControl() } || '\\' in trimmed) invalid()
        val explicitScheme = Regex("^[a-zA-Z][a-zA-Z0-9+.-]*://").containsMatchIn(trimmed)
        if (explicitScheme && !trimmed.startsWith("http://", true) && !trimmed.startsWith("https://", true)) invalid()
        // Reject non-authority schemes (mailto:, javascript:, malformed http:) rather than guessing a host.
        if (!explicitScheme && Regex("^[A-Za-z][A-Za-z0-9+.-]*:").containsMatchIn(trimmed) &&
            !Regex("^[^/:]+:(80|443)(/|$)").containsMatchIn(trimmed)
        ) {
            invalid()
        }
        val candidate = if (explicitScheme) trimmed else "https://${trimmed.removePrefix("//")}"
        val url = candidate.toHttpUrlOrNull() ?: invalid()
        if (url.username.isNotEmpty() || url.password.isNotEmpty() ||
            '@' in candidate
                .substringAfter("://")
                .substringBefore('/')
                .substringBefore('?')
                .substringBefore('#')
        ) {
            unsafe()
        }
        val host = url.host.lowercase(Locale.ROOT)
        if (url.port !in setOf(80, 443) || ':' in host || host.matches(Regex("[0-9.]+")) ||
            !host.contains('.') || host.endsWith('.') || host.split('.').any { it.isEmpty() } ||
            host == "localhost" || host.endsWith(".localhost") ||
            listOf(".local", ".internal", ".lan", ".home", ".test", ".invalid").any { host.endsWith(it) }
        ) {
            unsafe()
        }
        return url
    }

    private fun removeTracking(url: HttpUrl): String {
        val builder = url.newBuilder().fragment(null)
        // Only this explicit tracking policy is removed. Functional queries and their order survive.
        for (name in url.queryParameterNames) {
            val lower = name.lowercase(Locale.ROOT)
            if (lower.startsWith("utm_") || lower in setOf("gclid", "dclid", "fbclid", "msclkid")) {
                builder.removeAllQueryParameters(name)
            }
        }
        return builder.build().toString()
    }

    private fun createIdentity(
        url: HttpUrl,
        normalized: String,
    ): CrawlIdentity {
        val host = url.host.lowercase(Locale.ROOT)
        val intent = "automatic"
        // Fixed field order and JSON escaping make the hash independent of serializer configuration.
        val canonical = "{\"normalizedSeedUrl\":${jsonString(normalized)},\"intentProfile\":\"$intent\"}"
        val origin = "${url.scheme}://$host" + if (url.port == url.defaultPort()) "" else ":${url.port}"
        return CrawlIdentity(sha256(canonical), normalized, intent, sha256("{\"intentProfile\":\"$intent\"}"), origin)
    }

    private fun HttpUrl.defaultPort() = if (scheme == "https") 443 else 80

    private fun invalid(): Nothing = throw InvalidSeed(DomainError("INVALID_URL", "Only public HTTP and HTTPS URLs are supported."))

    private fun unsafe(): Nothing =
        throw InvalidSeed(DomainError("UNSAFE_TARGET", "The URL must target a public hostname on port 80 or 443, without credentials."))
}

fun sha256(value: String): String = MessageDigest
    .getInstance("SHA-256")
    .digest(value.toByteArray(Charsets.UTF_8))
    .joinToString("") { "%02x".format(it) }

private fun jsonString(value: String): String = buildString {
    append('"')
    for (c in value) {
        when (c) {
            '"' -> append("\\\"")
            '\\' -> append("\\\\")
            else -> if (c.code < 32) append("\\u%04x".format(c.code)) else append(c)
        }
    }
    append('"')
}
