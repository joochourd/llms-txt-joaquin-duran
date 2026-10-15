package com.joaquinduran.llmstxt.api.limits

import com.joaquinduran.llmstxt.api.configuration.ClientIpConfiguration
import io.micronaut.http.HttpRequest
import io.netty.util.NetUtil
import jakarta.annotation.PostConstruct
import jakarta.inject.Singleton
import java.net.InetAddress

@Singleton
class ClientIpResolver(private val configuration: ClientIpConfiguration) {
    private val trusted by lazy {
        val values = configuration.trustedProxyCidrs
            .split(',')
            .map(String::trim)
            .filter(String::isNotEmpty)
        require(values.size <= 32) { "Configure at most 32 trusted proxy networks." }
        values.map { value ->
            val parts = value.split('/')
            require(parts.size == 2) { "Trusted proxies require literal IP/CIDR notation." }
            val address = requireNotNull(literal(parts[0])) { "Trusted proxies must use literal IP addresses." }
            val prefix = requireNotNull(parts[1].toIntOrNull()) { "Invalid trusted proxy prefix." }
            require(prefix in 1..address.size * 8) { "Universal or invalid proxy networks are not allowed." }
            Network(address, prefix)
        }
    }

    @PostConstruct
    fun validate() {
        trusted
    }

    fun client(request: HttpRequest<*>): String = request.remoteAddress.address?.let { resolve(it, request.headers.getAll("X-Real-IP")) } ?: "unknown"

    internal fun resolve(
        peer: InetAddress,
        forwarded: List<String>,
    ): String {
        // Only the configured socket-peer boundary may supply Railway's single, edge-written header.
        // X-Forwarded-For, request IDs and other client-controlled headers never establish trust.
        if (trusted.none { it.contains(peer.address) } || forwarded.size != 1) return peer.hostAddress
        val address = literal(forwarded.single().trim()) ?: return peer.hostAddress
        return InetAddress.getByAddress(address).hostAddress
    }

    private fun literal(value: String): ByteArray? {
        if (value.isEmpty() || value.length > 45 || value.any { it !in "0123456789abcdefABCDEF:." }) return null
        if (':' !in value) {
            val parts = value.split('.')
            if (parts.size != 4 || parts.any { part ->
                    part.isEmpty() || (part.length > 1 && part.startsWith('0')) || (part.toIntOrNull() ?: -1) !in 0..255
                }
            ) {
                return null
            }
        }
        return NetUtil.createByteArrayFromIpAddressString(value)
    }

    private data class Network(val address: ByteArray, val prefix: Int) {
        fun contains(peer: ByteArray): Boolean {
            if (peer.size != address.size) return false
            return address.indices.all { index ->
                val bits = (prefix - index * 8).coerceIn(0, 8)
                val mask = (0xff shl (8 - bits)) and 0xff
                (peer[index].toInt() and mask) == (address[index].toInt() and mask)
            }
        }
    }
}
