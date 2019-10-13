@file:JvmName("DynamoDbStoreSettings")
package jp.justincase.kafka.dynamodb

import jp.justincase.kafka.dynamodb.auxiliary.createAsynchronousClient
import jp.justincase.kafka.dynamodb.auxiliary.createSynchronousClient
import kotlinx.coroutines.future.await
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType.HASH
import software.amazon.awssdk.services.dynamodb.model.KeyType.RANGE
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType.B
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType.S
import java.net.URI

data class DynamoDbClientSettings(
    val endpointOverride: URI
)

data class DynamoDbTableThroughputSettings(
    val readCapacityUnits: Long,
    val writeCapacityUnits: Long
)

data class DynamoDbTableSettings(
    val table: String,
    val hashKeyColumn: String,
    val sortKeyColumn: String,
    val valueColumn: String
)


suspend fun DynamoDbClientSettings.createTable(
    tableThroughputSettings: DynamoDbTableThroughputSettings,
    tableSettings: DynamoDbTableSettings
): Unit =
    createAsynchronousClient().use { client ->
      try {
        client
            .createTable { table ->
              tableThroughputSettings.apply {
                table.provisionedThroughput {
                  it.readCapacityUnits(readCapacityUnits)
                  it.writeCapacityUnits(writeCapacityUnits)
                }
              }
              tableSettings.apply {
                table.keySchema(
                    KeySchemaElement.builder().attributeName(hashKeyColumn).keyType(HASH).build(),
                    KeySchemaElement.builder().attributeName(sortKeyColumn).keyType(RANGE).build()
                )
                table.attributeDefinitions(
                    AttributeDefinition.builder().attributeName(hashKeyColumn).attributeType(B).build(),
                    AttributeDefinition.builder().attributeName(sortKeyColumn).attributeType(S).build()
                )
              }
              table.tableName(tableSettings.table)
            }
            .await()
        Unit
      } catch (_: ResourceInUseException) {
        // Table already exists
      }
    }


@JvmSynthetic
internal
fun DynamoDbClientSettings.keyValueStoreBuilderSupplier(
    tableSettings: DynamoDbTableSettings
): KeyValueStoreBuilderSupplier =
    SharedReference(::createSynchronousClient).let { client ->
      object : KeyValueStoreBuilderSupplier {
        override fun <K, V> invoke(name: String, keySerde: Serde<K>, valueSerde: Serde<V>) =
            object : AbstractStoreBuilder<KeyValueStore<K, V>> {
              override fun name() = name

              override fun build() = Stores
                  .keyValueStoreBuilder(
                      object : KeyValueBytesStoreSupplier {
                        override fun get() = LateInitializedKeyValueStore {
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
