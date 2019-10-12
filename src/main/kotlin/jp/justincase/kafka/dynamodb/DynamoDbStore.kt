package jp.justincase.kafka.dynamodb

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.PutRequest
import software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_OLD
import software.amazon.awssdk.services.dynamodb.model.WriteRequest

private fun s(string: String) = builder().s(string).build()
private fun b(bytes: ByteArray) = builder().b(SdkBytes.fromByteArray(bytes)).build()

// Workaround for DynamoDB not supporting empty binary
private fun bOrNul(bytes: ByteArray) = if (bytes.isNotEmpty()) b(bytes) else builder().nul(true).build()
private fun Bytes.encode() = get().let { it.copyInto(ByteArray(1 + it.size), 1) }

private val EMPTY_BYTE_ARRAY = ByteArray(0)


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
          hashKeyColumn to b(key.encode()),
          sortKeyColumn to s(name),
          valueColumn to bOrNul(value)
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
                    hashKeyColumn to b(it.key.encode()),
                    sortKeyColumn to s(name),
                    valueColumn to bOrNul(it.value)
                ))
                .build())
            .build()
      }))
    }
  }

  override
  tailrec fun putIfAbsent(key: Bytes, value: ByteArray): ByteArray? =
      when (val v = get(key)) {
        null -> when (try {
          delegate.putItem {
            it.tableName(table)
            it.item(mapOf(
                hashKeyColumn to b(key.encode()),
                sortKeyColumn to s(name),
                valueColumn to bOrNul(value)
            ))
            it.conditionExpression("attribute_not_exists(#s)")
            it.expressionAttributeNames(mapOf("#s" to sortKeyColumn))
          }
          null
        } catch (_: ConditionalCheckFailedException) {
        }) {
          // tailrec does not work with try-catch directly
          null -> null
          else -> putIfAbsent(key, value)
        }
        else -> v
      }

  override
  fun get(key: Bytes): ByteArray? =
      delegate
          .getItem {
            it.tableName(table)
            it.key(mapOf(
                hashKeyColumn to b(key.encode()),
                sortKeyColumn to s(name)
            ))
          }
          .item()[valueColumn]
          ?.let {
            it.b()?.asByteArray() ?: EMPTY_BYTE_ARRAY
          }

  override
  fun delete(key: Bytes): ByteArray? =
      delegate
          .deleteItem {
            it.tableName(table)
            it.key(mapOf(
                hashKeyColumn to b(key.encode()),
                sortKeyColumn to s(name)
            ))
            it.returnValues(ALL_OLD)
          }
          .attributes()[valueColumn]
          ?.let {
            it.b()?.asByteArray() ?: EMPTY_BYTE_ARRAY
          }
}
