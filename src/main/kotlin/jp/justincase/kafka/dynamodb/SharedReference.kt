@file:Suppress("ReplaceNegatedIsEmptyWithIsNotEmpty")
package jp.justincase.kafka.dynamodb

import java.util.*
import java.util.concurrent.atomic.AtomicReference

class SharedReference<T : AutoCloseable>(
    private val factory: () -> T
) {
  class Token
  private val internalFactory get() = Collections.synchronizedSet<Token>(mutableSetOf()) to lazy(factory)
  private val instance = AtomicReference(internalFactory)

  tailrec fun open(): Pair<Token, T> {
    val token = Token()
    val p = instance.get()

    return if (p == null) {
      open()
    } else {
      p.first += token

      if (p !== instance.get()) {
        open()
      } else {
        token to p.second.value
      }
    }
  }

  fun close(token: Token) {
    val p = instance.get()

    if (p != null
        && p.first.remove(token)
        && p.first.isEmpty()
        && instance.getAndSet(null) === p) {
      if (!p.first.isEmpty()) {
        instance.set(p)
      } else {
        instance.set(internalFactory)
        p.second.value.close()
      }
    }
  }
}
