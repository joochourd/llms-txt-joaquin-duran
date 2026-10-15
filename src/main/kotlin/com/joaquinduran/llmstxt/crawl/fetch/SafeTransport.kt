package com.joaquinduran.llmstxt.crawl.fetch

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.execution.CrawlBudget
import com.joaquinduran.llmstxt.crawl.model.CrawlFailure
import jakarta.annotation.PreDestroy
import jakarta.inject.Singleton
import okhttp3.Dns
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.ByteArrayOutputStream
import java.net.InetAddress
import java.net.Proxy
import java.net.UnknownHostException
import java.util.concurrent.*

object PublicAddresses {
    fun allowed(address: InetAddress): Boolean {
        val octets = address.address.map { it.toInt() and 255 }
        if (octets.size == 4) {
            val firstOctet = octets[0]
            val secondOctet = octets[1]
            val thirdOctet = octets[2]
            return !(
                firstOctet == 0 || firstOctet == 10 || firstOctet == 127 || firstOctet >= 224 ||
                    (firstOctet == 100 && secondOctet in 64..127) || (firstOctet == 169 && secondOctet == 254) ||
                    (firstOctet == 172 && secondOctet in 16..31) ||
                    (
                        firstOctet == 192 &&
                            (
                                secondOctet == 168 || (secondOctet == 0 && thirdOctet == 0) || (secondOctet == 0 && thirdOctet == 2) ||
                                    (secondOctet == 88 && thirdOctet == 99)
                            )
                    ) ||
                    (firstOctet == 198 && (secondOctet in 18..19 || (secondOctet == 51 && thirdOctet == 100))) ||
                    (firstOctet == 203 && secondOctet == 0 && thirdOctet == 113)
            )
        }
        // Only globally routable unicast; exclude documentation and protocol assignments.
        return octets.size == 16 && octets[0] in 0x20..0x3f &&
            !(octets[0] == 0x20 && octets[1] == 0x01 && (octets[2] < 2 || (octets[2] == 0x0d && octets[3] == 0xb8))) &&
            !(octets[0] == 0x20 && octets[1] == 0x02) && !(octets[0] == 0x3f && octets[1] == 0xff)
    }
}

/** The same DNS hook supplies validated addresses directly to OkHttp's socket routes. */
class BoundedDns(
    private val resolver: (String) -> List<InetAddress> = { InetAddress.getAllByName(it).toList() },
    private val validate: (InetAddress) -> Boolean = PublicAddresses::allowed,
) : AutoCloseable {
    private val pool = ThreadPoolExecutor(
        4,
        4,
        0,
        TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(16),
        ThreadFactory { Thread(it, "crawl-dns").apply { isDaemon = true } },
        ThreadPoolExecutor.AbortPolicy(),
    )

    fun lookup(
        host: String,
        timeoutMillis: Long,
    ): List<InetAddress> {
        val task = try {
            pool.submit<List<InetAddress>> { resolver(host) }
        } catch (_: RejectedExecutionException) {
            throw UnknownHostException("DNS capacity is exhausted.")
        }
        try {
            val addresses = task.get(timeoutMillis, TimeUnit.MILLISECONDS)
            if (addresses.isEmpty() ||
                addresses.any { !validate(it) }
            ) {
                throw UnknownHostException("The host does not resolve exclusively to public addresses.")
            }
            return addresses
        } catch (exception: Exception) {
            task.cancel(true)
            if (exception is InterruptedException) Thread.currentThread().interrupt()
            if (exception is UnknownHostException) throw exception
            throw UnknownHostException("DNS resolution failed or exceeded its deadline.")
        }
    }

    override fun close() {
        pool.shutdownNow()
    }
}

data class FetchResponse(
    val url: HttpUrl,
    val status: Int,
    val headers: Map<String, List<String>>,
    val bytes: ByteArray,
) {
    fun header(name: String) = headers.entries
        .firstOrNull { it.key.equals(name, true) }
        ?.value
        ?.firstOrNull()
}

// Test fixtures inject this boundary. Production has no private-target exception or arbitrary headers.
fun interface PageTransport {
    fun get(
        url: HttpUrl,
        budget: CrawlBudget,
        maxBytes: Int,
    ): FetchResponse
}

@Singleton
class SafeTransport @jakarta.inject.Inject constructor(
    private val configuration: CrawlConfiguration,
) : PageTransport, AutoCloseable {
    private var dns = BoundedDns()

    internal constructor(configuration: CrawlConfiguration, testDns: BoundedDns) : this(configuration) {
        dns.close()
        dns = testDns
    }

    private val client = OkHttpClient
        .Builder()
        .followRedirects(false)
        .followSslRedirects(false)
        .retryOnConnectionFailure(false)
        .proxy(Proxy.NO_PROXY)
        .build()

    override fun get(
        url: HttpUrl,
        budget: CrawlBudget,
        maxBytes: Int,
    ): FetchResponse {
        val timeout = minOf(budget.remainingMillis(), configuration.fetch.requestTimeoutSeconds * 1000)
        val requestClient = client
            .newBuilder()
            .dns(Dns { host -> dns.lookup(host, minOf(timeout, budget.remainingMillis())) })
            .callTimeout(timeout, TimeUnit.MILLISECONDS)
            .connectTimeout(timeout, TimeUnit.MILLISECONDS)
            .readTimeout(timeout, TimeUnit.MILLISECONDS)
            .build()
        val call = requestClient.newCall(
            Request
                .Builder()
                .url(url)
                .header("User-Agent", "LlmsTxtGenerator/0.4")
                .header("Accept", "text/html, text/plain, text/markdown, application/xml;q=0.9, */*;q=0.1")
                .build(),
        )
        try {
            call.execute().use { response ->
                val output = ByteArrayOutputStream()
                response.body.byteStream().use { stream ->
                    val buffer = ByteArray(8192)
                    while (true) {
                        budget.remainingMillis()
                        val count = stream.read(buffer)
                        if (count < 0) break
                        if (output.size() + count >
                            maxBytes
                        ) {
                            throw CrawlFailure("BODY_LIMIT", "The decompressed response exceeds its byte limit.")
                        }
                        output.write(buffer, 0, count)
                    }
                }
                return FetchResponse(url, response.code, response.headers.toMultimap(), output.toByteArray())
            }
        } catch (exception: CrawlFailure) {
            throw exception
        } catch (_: Exception) {
            throw CrawlFailure("FETCH_FAILED", "The public source could not be fetched safely within its deadline.")
        } finally {
            call.cancel()
        }
    }

    @PreDestroy
    override fun close() {
        client.dispatcher.executorService.shutdownNow()
        client.connectionPool.evictAll()
        dns.close()
    }
}
