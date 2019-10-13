@file:JvmName("DynamoDbStores")
package jp.justincase.kafka.dynamodb

import jp.justincase.kafka.dynamodb.auxiliary.createSynchronousClient
import jp.justincase.kafka.dynamodb.auxiliary.createTableSynchronously
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import java.net.URI
import kotlin.LazyThreadSafetyMode.PUBLICATION

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
    SharedReference(::createSynchronousClient).let { client ->
      val createTable = lazy(PUBLICATION) {
        client.createTableSynchronously(tableSettings)
      }

      object : KeyValueStoreBuilderSupplier {
        override fun <K, V> invoke(name: String, keySerde: Serde<K>, valueSerde: Serde<V>) =
            object : AbstractStoreBuilder<KeyValueStore<K, V>> {
              override fun name() = name

              override fun build() = Stores
                  .keyValueStoreBuilder(
                      object : KeyValueBytesStoreSupplier {
                        override fun get() = LateInitializedKeyValueStore(true, name) {
                          createTable.value
                          DynamoDbStore.open(client, name, tableSettings)
                        }
                        override fun name() = name
                        override fun metricsScope() = "dynamodb-state"
                      },
                      keySerde,
                      valueSerde
                  )
                  .withLoggingDisabled()
                  .build()
            }
      }
    }
