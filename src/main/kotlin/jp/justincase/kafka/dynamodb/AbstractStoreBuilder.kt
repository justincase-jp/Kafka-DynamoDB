package jp.justincase.kafka.dynamodb

import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.state.StoreBuilder

interface AbstractStoreBuilder<T : StateStore> : StoreBuilder<T> {
  override fun logConfig(): Map<String, String> = mapOf()
  override fun withCachingDisabled(): StoreBuilder<T> = this
  override fun loggingEnabled() = false
  override fun withLoggingDisabled(): StoreBuilder<T> = this
  override fun withCachingEnabled(): StoreBuilder<T> = this
  override fun withLoggingEnabled(config: Map<String, String>?): StoreBuilder<T> = this
}
