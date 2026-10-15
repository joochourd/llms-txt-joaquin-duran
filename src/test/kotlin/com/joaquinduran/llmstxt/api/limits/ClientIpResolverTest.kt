package com.joaquinduran.llmstxt.api.limits

import com.joaquinduran.llmstxt.api.configuration.ApiLimitConfiguration
import com.joaquinduran.llmstxt.api.configuration.ClientIpConfiguration
import com.joaquinduran.llmstxt.api.errors.ApiProblem
import io.micronaut.http.HttpRequest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetAddress
import java.time.Clock

class ClientIpResolverTest {
    private fun address(value: String): InetAddress = InetAddress.getByName(value)

    private fun trusted() = ClientIpResolver(ClientIpConfiguration().apply { trustedProxyCidrs = "10.20.0.8/32, fd00:1234::/64" })

    @Test
    fun `direct requests ignore forwarding headers including on loopback`() {
        val resolver = ClientIpResolver(ClientIpConfiguration())

        assertEquals("127.0.0.1", resolver.resolve(address("127.0.0.1"), listOf("203.0.113.9")))
        assertEquals("203.0.113.8", resolver.resolve(address("203.0.113.8"), emptyList()))
    }

    @Test
    fun `forwarding markers cannot turn an untrusted socket peer into a trusted proxy`() {
        val request = HttpRequest
            .GET<Any>("/")
            .header("X-Real-IP", "203.0.113.9")
            .header("X-Forwarded-For", "203.0.113.10")
            .header("X-Railway-Request-Id", "client-supplied-marker")

        assertEquals(request.remoteAddress.address.hostAddress, trusted().client(request))
    }

    @Test
    fun `only configured proxy peers may supply the single Railway client address`() {
        val resolver = trusted()

        assertEquals("203.0.113.9", resolver.resolve(address("10.20.0.8"), listOf("203.0.113.9")))
        assertEquals("203.0.113.10", resolver.resolve(address("fd00:1234::2"), listOf("203.0.113.10")))
        assertEquals("10.20.0.9", resolver.resolve(address("10.20.0.9"), listOf("203.0.113.9")))
    }

    @ParameterizedTest
    @ValueSource(strings = ["", "example.com", "203.0.113.9, 203.0.113.10", "999.1.1.1", "[::1]:80", "fe80::1%eth0", "01.2.3.4"])
    fun `malformed forwarding values collapse to the trusted peer bucket`(value: String) {
        assertEquals("10.20.0.8", trusted().resolve(address("10.20.0.8"), listOf(value)))
    }

    @Test
    fun `duplicate forwarding headers cannot select a different rate bucket`() {
        assertEquals("10.20.0.8", trusted().resolve(address("10.20.0.8"), listOf("203.0.113.9", "203.0.113.10")))
    }

    @Test
    fun `equivalent IPv6 spellings identify the same client`() {
        val resolver = trusted()
        val peer = address("10.20.0.8")

        assertEquals(resolver.resolve(peer, listOf("2001:db8::1")), resolver.resolve(peer, listOf("2001:0db8:0:0:0:0:0:1")))
    }

    @Test
    fun `different clients behind one trusted proxy retain independent submission buckets`() {
        val resolver = trusted()
        val peer = address("10.20.0.8")
        val firstClient = resolver.resolve(peer, listOf("203.0.113.9"))
        val secondClient = resolver.resolve(peer, listOf("203.0.113.10"))
        val limiter = ApiRateLimiter(Clock.systemUTC(), ApiLimitConfiguration())
        limiter.check(firstClient, submission = true)
        limiter.check(firstClient, submission = true)

        assertThrows(ApiProblem::class.java) { limiter.check(firstClient, submission = true) }
        assertDoesNotThrow { limiter.check(secondClient, submission = true) }
    }

    @ParameterizedTest
    @ValueSource(strings = ["0.0.0.0/0", "::/0", "localhost/32", "10.0.0.1/33", "10.0.0.1"])
    fun `invalid or universal trusted boundaries are rejected`(value: String) {
        val resolver = ClientIpResolver(ClientIpConfiguration().apply { trustedProxyCidrs = value })

        assertThrows(IllegalArgumentException::class.java) { resolver.resolve(address("10.20.0.8"), emptyList()) }
    }
}
