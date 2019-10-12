@file:JvmName("DynamoDbClients")
package jp.justincase.kafka.dynamodb.auxiliary

import jp.justincase.kafka.dynamodb.DynamoDbClientSettings
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

fun DynamoDbClientSettings.createAsynchronousClient(): DynamoDbAsyncClient =
    DynamoDbAsyncClient
        .builder()
        .endpointOverride(endpointOverride)
        .build()

fun DynamoDbClientSettings.createSynchronousClient(): DynamoDbClient =
    DynamoDbClient
        .builder()
        .endpointOverride(endpointOverride)
        .build()
