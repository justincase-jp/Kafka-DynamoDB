@file:JvmName("DynamoDbStores")
package jp.justincase.kafka.dynamodb

import jp.justincase.kafka.dynamodb.auxiliary.createSynchronousClient
import jp.justincase.kafka.dynamodb.auxiliary.keyValueStoreBuilderSupplier
import java.net.URI

interface DynamoDbStoreSettings {
  val endpointOverride: URI
  val table: String
  val hashKeyColumn: String
  val sortKeyColumn: String
  val valueColumn: String

  data class Value(
      override val endpointOverride: URI,
      override val table: String,
      override val hashKeyColumn: String = "key",
      override val sortKeyColumn: String = "type",
      override val valueColumn: String = "value"
  ) : DynamoDbStoreSettings
}

fun DynamoDbStoreSettings.toClientSettings() =
    DynamoDbClientSettings(endpointOverride)

fun DynamoDbStoreSettings.toTableSettings() =
    DynamoDbTableSettings(table, hashKeyColumn, sortKeyColumn, valueColumn)


data class DynamoDbClientSettings(
    val endpointOverride: URI
)

data class DynamoDbTableSettings(
    val table: String,
    val hashKeyColumn: String,
    val sortKeyColumn: String,
    val valueColumn: String
)


fun DynamoDbStoreSettings.keyValueStoreBuilderSupplier() =
    toClientSettings().keyValueStoreBuilderSupplier(toTableSettings())

fun DynamoDbClientSettings.keyValueStoreBuilderSupplier(
    tableSettings: DynamoDbTableSettings
): KeyValueStoreBuilderSupplier =
    SharedReference(::createSynchronousClient).keyValueStoreBuilderSupplier(tableSettings)
