package com.joaquinduran.llmstxt.crawl.execution.load

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import java.lang.management.ManagementFactory
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

class LoadMeasurements {
    private val sdk = ConcurrentHashMap<String, ConcurrentLinkedQueue<Double>>()
    val api = mutableListOf<Double>()
    val readiness = mutableListOf<Double>()

    /** Measure dispatcher/worker calls only; API, admission and observer calls use the original client. */
    fun client(delegate: DynamoDbClient): DynamoDbClient = Proxy.newProxyInstance(
        DynamoDbClient::class.java.classLoader,
        arrayOf(DynamoDbClient::class.java),
    ) { _, method, arguments ->
        val args = arguments ?: emptyArray()
        val operation = if (method.name == "query") {
            val partition = (args[0] as QueryRequest).expressionAttributeValues()[":pk"]?.s().orEmpty()
            "query:${partition.split('#').getOrNull(1) ?: "other"}"
        } else {
            method.name
        }
        val started = System.nanoTime()
        try {
            method.invoke(delegate, *args)
        } catch (failure: InvocationTargetException) {
            throw requireNotNull(failure.cause)
        } finally {
            sdk.computeIfAbsent(operation) { ConcurrentLinkedQueue() }.add((System.nanoTime() - started) / 1_000_000.0)
        }
    } as DynamoDbClient

    fun sdkSummary(): Map<String, Map<String, Number>> = sdk.toSortedMap().mapValues { (_, values) -> distribution(values.toList()) }

    companion object {
        fun distribution(values: List<Double>): Map<String, Number> {
            require(values.isNotEmpty())
            val sorted = values.sorted()

            fun percentile(fraction: Double) = sorted[(kotlin.math.ceil(sorted.size * fraction).toInt() - 1).coerceAtLeast(0)]
            return mapOf("samples" to values.size, "p50" to percentile(0.5), "p95" to percentile(0.95), "max" to sorted.last())
        }

        fun cpuNanos(): Long = (ManagementFactory.getOperatingSystemMXBean() as com.sun.management.OperatingSystemMXBean).processCpuTime

        fun rssBytes(): Long? {
            val status = Path.of("/proc/self/status")
            if (!Files.isReadable(status)) return null
            return Files
                .readAllLines(status)
                .firstOrNull { it.startsWith("VmRSS:") }
                ?.substringAfter(':')
                ?.trim()
                ?.substringBefore(' ')
                ?.toLong()
                ?.times(1024)
        }
    }
}
