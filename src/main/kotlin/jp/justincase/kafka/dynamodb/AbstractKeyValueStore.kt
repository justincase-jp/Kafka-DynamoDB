package jp.justincase.kafka.dynamodb

import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.state.KeyValueIterator
import org.apache.kafka.streams.state.KeyValueStore

interface AbstractKeyValueStore<K, V> : KeyValueStore<K, V> {
  override fun init(context: ProcessorContext, root: StateStore) =
      context.register(root) { _, _ -> }

  override fun flush() = Unit

  override fun range(from: K, to: K): KeyValueIterator<K, V> =
      throw UnsupportedOperationException("$this.range($from, $to)")

  override fun all(): KeyValueIterator<K, V> =
      throw UnsupportedOperationException("$this.all()")

  override fun isOpen() = true

  override fun close() = Unit
}
