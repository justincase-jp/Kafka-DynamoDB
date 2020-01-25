@file:Suppress("UnstableApiUsage")
package jp.justincase.kafka.dynamodb.hash

import com.google.common.collect.Lists
import com.google.common.hash.Hashing
import jp.justincase.kafka.dynamodb.AbstractKeyValueStore
import jp.justincase.kafka.dynamodb.DynamoDbStore
import jp.justincase.kafka.dynamodb.DynamoDbTableSettings
import jp.justincase.kafka.dynamodb.SharedReference
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

private const val THRESHOLD = 2047 // One byte is used in representing emptiness

private
fun Bytes.hashIfOverSized(): Bytes =
    if (get().size <= THRESHOLD) this else Hashing
        .sha512()
        .hashBytes(get())
        .asBytes()
        .let(::Bytes)


class HashingDynamoDbStore private constructor (
    private val delegate: DynamoDbStore
) : AbstractKeyValueStore<Bytes, ByteArray> by delegate {
  companion object {
    @JvmStatic
    fun open(client: SharedReference<DynamoDbClient>, storeName: String, tableSettings: DynamoDbTableSettings) =
        DynamoDbStore.open(client, storeName, tableSettings).let(::HashingDynamoDbStore)
  }

  override fun put(key: Bytes, value: ByteArray) =
      delegate.put(key.hashIfOverSized(), value)

  override fun putAll(entries: List<KeyValue<Bytes, ByteArray>>) =
      delegate.putAll(Lists.transform(entries) { KeyValue(it!!.key.hashIfOverSized(), it.value) })

  override fun putIfAbsent(key: Bytes, value: ByteArray) =
      delegate.putIfAbsent(key.hashIfOverSized(), value)

  override fun get(key: Bytes) =
      delegate.get(key.hashIfOverSized())

  override fun delete(key: Bytes) =
      delegate.delete(key.hashIfOverSized())
}
