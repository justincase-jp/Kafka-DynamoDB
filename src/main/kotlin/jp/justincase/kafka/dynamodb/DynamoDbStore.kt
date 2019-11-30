package jp.justincase.kafka.dynamodb

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder
import software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_OLD
import java.net.URI

private fun s(string: String) = builder().s(string).build()
private fun b(bytes: ByteArray) = builder().b(SdkBytes.fromByteArray(bytes)).build()

// Workaround for DynamoDB not supporting empty binary
private fun bOrNul(bytes: ByteArray) = if (bytes.isNotEmpty()) b(bytes) else builder().nul(true).build()
private fun Bytes.encode() = get().let { it.copyInto(ByteArray(1 + it.size), 1) }

private val EMPTY_BYTE_ARRAY = ByteArray(0)


class DynamoDbStore private constructor (
    private val handle: Lazy<Unit>,
    private val delegate: DynamoDbClient,
    private val storeName: String,
    private val table: String,
    private val hashKeyColumn: String,
    private val sortKeyColumn: String,
    private val valueColumn: String
) : AbstractKeyValueStore<Bytes, ByteArray> {
  companion object {
    @JvmStatic
    fun open(client: SharedReference<DynamoDbClient>, storeName: String, tableSettings: DynamoDbTableSettings) =
        client.open().let { (handle, delegate) ->
          DynamoDbStore(
              handle, delegate, storeName,
              tableSettings.table, tableSettings.hashKeyColumn, tableSettings.sortKeyColumn, tableSettings.valueColumn
          )
        }

    @JvmStatic
    fun keyValueStoreBuilderSupplier(
        endpointOverride: URI,
        table: String,
        credentialOverride: Pair<String, String>? = null,
        hashKeyColumn: String = "key",
        sortKeyColumn: String = "type",
        valueColumn: String = "value"
    ): KeyValueStoreBuilderSupplier =
        DynamoDbClientSettings(endpointOverride, credentialOverride).keyValueStoreBuilderSupplier(DynamoDbTableSettings(
            table, hashKeyColumn, sortKeyColumn, valueColumn
        ))
  }

  override fun isOpen() = !handle.isInitialized()
  override fun close() = handle.value
  override fun name() = storeName
  override fun persistent() = true
  override fun approximateNumEntries() = Long.MAX_VALUE

  override
  fun put(key: Bytes, value: ByteArray) {
    delegate.putItem {
      it.tableName(table)
      it.item(mapOf(
          hashKeyColumn to b(key.encode()),
          sortKeyColumn to s(storeName),
          valueColumn to bOrNul(value)
      ))
    }
  }

  override
  fun putAll(entries: List<KeyValue<Bytes, ByteArray>>) =
      entries
          .asSequence()
          .chunked(25) // Hard limit per request
          .forEach { chunk ->
            delegate.batchWriteItem { request ->
              request.requestItems(mapOf(
                  table to chunk
                      .associateBy({ it.key }) { it.value } // Deduplication is mandatory
                      .map {
                        WriteRequest
                            .builder()
                            .putRequest(PutRequest
                                .builder()
                                .item(mapOf(
                                    hashKeyColumn to b(it.key.encode()),
                                    sortKeyColumn to s(storeName),
                                    valueColumn to bOrNul(it.value)
                                ))
                                .build())
                            .build()
                      }
              ))
            }
          }


  override
  fun putIfAbsent(key: Bytes, value: ByteArray): ByteArray? =
      key.encode().let { encodedKey ->
        putIfAbsent(createGetRequest(encodedKey), createPutIfAbsentRequest(encodedKey, value))
      }

  private
  fun createPutIfAbsentRequest(encodedKey: ByteArray, value: ByteArray): PutItemRequest =
      PutItemRequest
          .builder()
          .let {
            it.tableName(table)
            it.item(mapOf(
                hashKeyColumn to b(encodedKey),
                sortKeyColumn to s(storeName),
                valueColumn to bOrNul(value)
            ))
            it.conditionExpression("attribute_not_exists(#s)")
            it.expressionAttributeNames(mapOf("#s" to sortKeyColumn))
            it.build()
          }

  private
  tailrec fun putIfAbsent(getRequest: GetItemRequest, putIfAbsentRequest: PutItemRequest): ByteArray? =
      when (val v = get(getRequest)) {
        null -> when (try {
          delegate.putItem(putIfAbsentRequest)
          null
        } catch (_: ConditionalCheckFailedException) {
        }) {
          // tailrec does not work with try-catch directly
          null -> null
          else -> putIfAbsent(getRequest, putIfAbsentRequest)
        }
        else -> v
      }


  override
  fun get(key: Bytes): ByteArray? =
      get(createGetRequest(key.encode()))

  private
  fun createGetRequest(encodedKey: ByteArray): GetItemRequest =
      GetItemRequest
          .builder()
          .let {
            it.tableName(table)
            it.key(mapOf(
                hashKeyColumn to b(encodedKey),
                sortKeyColumn to s(storeName)
            ))
            it.build()
          }

  private
  fun get(getRequest: GetItemRequest): ByteArray? =
      delegate
          .getItem(getRequest)
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
                sortKeyColumn to s(storeName)
            ))
            it.returnValues(ALL_OLD)
          }
          .attributes()[valueColumn]
          ?.let {
            it.b()?.asByteArray() ?: EMPTY_BYTE_ARRAY
          }
}
