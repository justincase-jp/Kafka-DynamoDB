package jp.justincase.kafka.dynamodb

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder
import software.amazon.awssdk.services.dynamodb.model.PutRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_OLD
import software.amazon.awssdk.services.dynamodb.model.WriteRequest

private fun s(string: String) = builder().s(string).build()
private fun b(bytes: Bytes) = b(bytes.get())
private fun b(bytes: ByteArray) = builder().b(SdkBytes.fromByteArray(bytes)).build()

class DynamoDbStore(
    private val delegate: DynamoDbClient,
    private val table: String,
    private val hashKeyColumn: String,
    private val sortKeyColumn: String,
    private val valueColumn: String,
    private val name: String
) : AbstractKeyValueStore<Bytes, ByteArray> {
  override fun name() = name
  override fun persistent() = true
  override fun approximateNumEntries() = Long.MAX_VALUE

  override
  fun put(key: Bytes, value: ByteArray) {
    delegate.putItem {
      it.tableName(table)
      it.item(mapOf(
          hashKeyColumn to b(key),
          sortKeyColumn to s(name),
          valueColumn to b(value)
      ))
    }
  }

  override
  fun putAll(entries: List<KeyValue<Bytes, ByteArray>>) {
    delegate.batchWriteItem { request ->
      request.requestItems(mapOf(table to entries.map {
        WriteRequest
            .builder()
            .putRequest(PutRequest
                .builder()
                .item(mapOf(
                    hashKeyColumn to b(it.key),
                    sortKeyColumn to s(name),
                    valueColumn to b(it.value)
                ))
                .build())
            .build()
      }))
    }
  }

  override
  fun putIfAbsent(key: Bytes, value: ByteArray): ByteArray? =
      delegate
          .putItem {
            it.tableName(table)
            it.item(mapOf(
                hashKeyColumn to b(key),
                sortKeyColumn to s(name),
                valueColumn to b(value)
            ))
            it.conditionExpression("attribute_not_exists($sortKeyColumn)")
            it.returnValues(ALL_OLD)
          }
          .attributes()[valueColumn]
          ?.b()
          ?.asByteArray()

  override
  fun get(key: Bytes): ByteArray? =
      delegate
          .getItem {
            it.tableName(table)
            it.key(mapOf(
                hashKeyColumn to b(key),
                sortKeyColumn to s(name)
            ))
          }
          .item()[valueColumn]
          ?.b()
          ?.asByteArray()

  override
  fun delete(key: Bytes): ByteArray? =
      delegate
          .deleteItem {
            it.tableName(table)
            it.key(mapOf(
                hashKeyColumn to b(key),
                sortKeyColumn to s(name)
            ))
            it.returnValues(ALL_OLD)
          }
          .attributes()[valueColumn]
          ?.b()
          ?.asByteArray()
}
