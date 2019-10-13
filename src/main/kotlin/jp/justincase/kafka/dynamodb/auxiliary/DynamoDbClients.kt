@file:JvmName("DynamoDbClients")
package jp.justincase.kafka.dynamodb.auxiliary

import jp.justincase.kafka.dynamodb.DynamoDbClientSettings
import jp.justincase.kafka.dynamodb.DynamoDbTableSettings
import jp.justincase.kafka.dynamodb.SharedReference
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.model.BillingMode.PAY_PER_REQUEST
import software.amazon.awssdk.services.dynamodb.model.KeyType.HASH
import software.amazon.awssdk.services.dynamodb.model.KeyType.RANGE
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType.B
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType.S
import java.io.Closeable

fun DynamoDbClientSettings.createSynchronousClient(): DynamoDbClient =
    DynamoDbClient
        .builder()
        .endpointOverride(endpointOverride)
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


private
interface CloseableReference<T> : Closeable, () -> T

private
fun <T> Pair<Lazy<Unit>, T>.toCloseableReference() = object : CloseableReference<T> {
  override fun invoke() = second

  override fun close() = first.value
}
