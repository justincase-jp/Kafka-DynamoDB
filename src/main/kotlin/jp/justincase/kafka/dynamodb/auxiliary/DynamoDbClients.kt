@file:JvmName("DynamoDbClients")
package jp.justincase.kafka.dynamodb.auxiliary

import jp.justincase.kafka.dynamodb.*
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.Region.US_WEST_2
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.BillingMode.PAY_PER_REQUEST
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType.HASH
import software.amazon.awssdk.services.dynamodb.model.KeyType.RANGE
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType.B
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType.S
import java.io.Closeable
import kotlin.LazyThreadSafetyMode.PUBLICATION

fun DynamoDbClientSettings.createSynchronousClient(): DynamoDbClient =
    DynamoDbClient
        .builder()
        .apply {
          endpointOverride(endpointOverride)

          // Derive region from the endpoint URI (or else use Oregon)
          Regex("""dynamodb\.(.*)\.amazonaws\.com""")
              .matchEntire(endpointOverride.host)
              ?.destructured
              ?.let { (region) -> region(Region.of(region)) }
              ?: region(US_WEST_2)

          // Configure explicit AWS credential
          credentialOverride?.let { (keyId, key) ->
            credentialsProvider {
              AwsBasicCredentials.create(keyId, key)
            }
          }
        }
        .build()


fun SharedReference<DynamoDbClient>.createTableSynchronously(
    tableSettings: DynamoDbTableSettings
): Unit =
    open().toCloseableReference().use { client ->
      try {
        client().createTable { table ->
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
          table.billingMode(PAY_PER_REQUEST)
        }
        Unit
      } catch (_: ResourceInUseException) {
        // Table already exists
      }
    }

fun SharedReference<DynamoDbClient>.keyValueStoreBuilderSupplier(
    tableSettings: DynamoDbTableSettings
): KeyValueStoreBuilderSupplier =
    object : KeyValueStoreBuilderSupplier {
      val createTable = lazy(PUBLICATION) {
        createTableSynchronously(tableSettings)
      }

      override fun <K, V> invoke(name: String, keySerde: Serde<K>, valueSerde: Serde<V>) =
          object : AbstractStoreBuilder<KeyValueStore<K, V>> {
            override fun name() = name

            override fun build() = Stores
                .keyValueStoreBuilder(
                    object : KeyValueBytesStoreSupplier {
                      override fun get() = LateInitializedKeyValueStore(true, name) {
                        createTable.value
                        DynamoDbStore.open(this@keyValueStoreBuilderSupplier, name, tableSettings)
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


private
interface CloseableReference<T> : Closeable, () -> T

private
fun <T> Pair<Lazy<Unit>, T>.toCloseableReference() = object : CloseableReference<T> {
  override fun invoke() = second

  override fun close() = first.value
}
