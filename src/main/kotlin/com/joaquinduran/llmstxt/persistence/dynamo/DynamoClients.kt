package com.joaquinduran.llmstxt.persistence.dynamo

import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.time.Duration

object DynamoClients {
    fun create(configuration: DynamoConfiguration): DynamoDbClient {
        configuration.validate()
        val builder = DynamoDbClient
            .builder()
            .region(Region.of(configuration.region))
            .httpClientBuilder(
                UrlConnectionHttpClient
                    .builder()
                    .connectionTimeout(Duration.ofSeconds(1))
                    .socketTimeout(Duration.ofSeconds(1)),
            ).overrideConfiguration {
                it
                    .apiCallAttemptTimeout(Duration.ofSeconds(2))
                    .apiCallTimeout(Duration.ofSeconds(3))
                    .retryPolicy(RetryPolicy.builder().numRetries(1).build())
            }
        if (configuration.environment == "local") {
            // Explicit dummy credentials prevent discovery of real profiles or instance credentials.
            builder
                .endpointOverride(configuration.localEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "localSecret")))
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.builder().build())
        }
        return builder.build()
    }
}

@Factory
class DynamoClientFactory {
    @Singleton
    @Bean(preDestroy = "close")
    fun dynamoDbClient(configuration: DynamoConfiguration): DynamoDbClient = DynamoClients.create(configuration)
}
