package com.joaquinduran.llmstxt.crawl.fetch

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.UnknownHostException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPOutputStream

class SafeTransportTest {
    @Test
    fun `special IPv4 IPv6 and mapped addresses are never public`() {
        listOf(
            "127.0.0.1",
            "10.1.2.3",
            "100.64.0.1",
            "169.254.169.254",
            "172.31.1.1",
            "192.168.1.1",
            "192.0.2.1",
            "198.18.1.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "::",
            "::1",
            "fc00::1",
            "fe80::1",
            "ff00::1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
        ).forEach {
            assertFalse(PublicAddresses.allowed(InetAddress.getByName(it)), it)
        }
        listOf("8.8.8.8", "1.1.1.1", "2606:4700:4700::1111").forEach { assertTrue(PublicAddresses.allowed(InetAddress.getByName(it)), it) }
    }

    @Test
    fun `one private address rejects a mixed DNS response`() {
        BoundedDns({ listOf(InetAddress.getByName("8.8.8.8"), InetAddress.getByName("127.0.0.1")) }).use { dns ->
            assertThrows(UnknownHostException::class.java) { dns.lookup("example.com", 1000) }
        }
    }

    @Test
    fun `real OkHttp client connects to the address supplied by its only DNS lookup without implicit redirects`() {
        ServerSocket(0).use { server ->
            val pool = Executors.newSingleThreadExecutor()
            val received = pool.submit<String> {
                server.accept().use { socket ->
                    val reader = socket.getInputStream().bufferedReader()
                    val headers = buildString {
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                            appendLine(line)
                        }
                    }
                    socket.getOutputStream().write(
                        "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1/private\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray(),
                    )
                    headers
                }
            }
            val lookups = AtomicInteger()
            val dns = BoundedDns({
                lookups.incrementAndGet()
                listOf(InetAddress.getLoopbackAddress())
            }, { true })
            SafeTransport(CrawlConfiguration(), dns).use { transport ->
                val response = transport.get("http://unique-public.example:${server.localPort}/".toHttpUrl(), CrawlBudget(5, 5000), 100)
                assertEquals(302, response.status)
                assertEquals(1, lookups.get())
                val headers = received.get(5, TimeUnit.SECONDS)
                assertTrue(headers.contains("Host: unique-public.example:${server.localPort}"))
                assertTrue(headers.contains("User-Agent: LlmsTxtGenerator/0.4"))
                assertFalse(headers.contains("Authorization:"))
                assertFalse(headers.contains("Cookie:"))
            }
            pool.shutdownNow()
        }
    }

    @Test
    fun `real client DNS timeout returns while the bounded resolver is still blocked`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val dns = BoundedDns({
            entered.countDown()
            while (release.count >
                0
            ) {
                try {
                    release.await()
                } catch (_: InterruptedException) {
                }
            }
            listOf(InetAddress.getByName("8.8.8.8"))
        })
        val before = System.nanoTime()
        try {
            SafeTransport(CrawlConfiguration().apply { fetch.requestTimeoutSeconds = 1 }, dns).use { transport ->
                assertThrows(CrawlFailure::class.java) { transport.get("https://unique-public.example/".toHttpUrl(), CrawlBudget(5, 5000), 100) }
                assertEquals(0L, entered.count)
                assertTrue((System.nanoTime() - before) / 1_000_000 < 3000)
                assertEquals(1L, release.count)
            }
        } finally {
            release.countDown()
        }
    }

    @Test
    fun `gzip body limit applies to decompressed bytes`() {
        ServerSocket(0).use { server ->
            val bytes = ByteArrayOutputStream().apply { GZIPOutputStream(this).use { it.write(ByteArray(4000) { 'a'.code.toByte() }) } }.toByteArray()
            val pool = Executors.newSingleThreadExecutor()
            pool.submit {
                server.accept().use { socket ->
                    val reader = socket.getInputStream().bufferedReader()
                    while (!reader.readLine().isNullOrEmpty()) {}
                    socket.getOutputStream().write(
                        (
                            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Type: text/plain\r\n" +
                                "Content-Length: ${bytes.size}\r\nConnection: close\r\n\r\n"
                        ).toByteArray() +
                            bytes,
                    )
                }
            }
            SafeTransport(CrawlConfiguration(), BoundedDns({ listOf(InetAddress.getLoopbackAddress()) }, { true })).use { transport ->
                val failure = assertThrows(
                    CrawlFailure::class.java,
                ) { transport.get("http://fixture.example:${server.localPort}/".toHttpUrl(), CrawlBudget(5, 5000), 1024) }
                assertEquals("BODY_LIMIT", failure.code)
            }
            pool.shutdownNow()
        }
    }
}
