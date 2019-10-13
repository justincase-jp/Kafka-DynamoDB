package jp.justincase.kafka.dynamodb

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder

interface KeyValueStoreBuilderSupplier {
  operator fun <K, V> invoke(
      name: String,
      keySerde: Serde<K>,
      valueSerde: Serde<V>
  ): StoreBuilder<KeyValueStore<K, V>>
}
