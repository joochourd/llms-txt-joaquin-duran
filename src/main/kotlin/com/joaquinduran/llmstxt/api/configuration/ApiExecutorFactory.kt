package com.joaquinduran.llmstxt.api.configuration

import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Context
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Factory
class ApiExecutorFactory {
    @Context
    @Named("api-io")
    @Bean(preDestroy = "shutdown")
    fun executor(
        @Value("\${API_IO_THREADS:8}") threads: Int,
        @Value("\${API_IO_QUEUE_CAPACITY:32}") queueCapacity: Int,
    ): ExecutorService {
        require(threads > 0 && queueCapacity > 0) { "API executor limits must be positive." }
        val sequence = AtomicInteger()
        return ThreadPoolExecutor(
            threads,
            threads,
            0,
            TimeUnit.MILLISECONDS,
            ArrayBlockingQueue(queueCapacity),
            { task -> Thread(task, "api-io-${sequence.incrementAndGet()}") },
            ThreadPoolExecutor.AbortPolicy(),
        )
    }
}
