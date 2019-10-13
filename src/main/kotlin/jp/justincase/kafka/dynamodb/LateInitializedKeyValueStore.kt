package jp.justincase.kafka.dynamodb

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.state.KeyValueStore

class LateInitializedKeyValueStore<K, V>(
    private val persistence: Boolean,
    private val name: String,
    private val factory: () -> KeyValueStore<K, V>
) : AbstractKeyValueStore<K, V> {
  private lateinit var delegate: KeyValueStore<K, V>

  override fun init(context: ProcessorContext, root: StateStore) {
    delegate = factory()
    super.init(context, root)
  }
  override fun persistent() = persistence
  override fun name() = name

  override fun isOpen() = delegate.isOpen
  override fun close() = delegate.close()
  override fun approximateNumEntries() = delegate.approximateNumEntries()

  override fun put(key: K, value: V) = delegate.put(key, value)
  override fun putAll(entries: List<KeyValue<K, V>>) = delegate.putAll(entries)
  override fun putIfAbsent(key: K, value: V): V? = delegate.putIfAbsent(key, value)
  override fun get(key: K): V? = delegate.get(key)
  override fun delete(key: K): V? = delegate.delete(key)
}
