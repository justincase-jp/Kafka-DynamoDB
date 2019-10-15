package jp.justincase.kafka.dynamodb.test.utility

import org.apache.kafka.streams.processor.StateStore
import java.io.Closeable

fun <T : StateStore, R> T.use(block: (T) -> R): R =
    Closeable(::close).use {
      block(this)
    }
