package jp.justincase.kafka.dynamodb

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.LazyThreadSafetyMode.SYNCHRONIZED

class SharedReference<T : AutoCloseable>(
    private val factory: () -> T
) {
  private val internalFactory get() = AtomicInteger() to lazy(SYNCHRONIZED, factory)
  private val instance = AtomicReference(internalFactory)

  tailrec fun open(): Pair<Lazy<Unit>, T> {
    val p = instance.get()

    return if (p == null) {
      open()
    } else {
      p.first.getAndIncrement()

      if (p !== instance.get()) {
        open()
      } else {
        lazy(SYNCHRONIZED, ::close) to p.second.value
      }
    }
  }

  private
  fun close() {
    val p = instance.get()

    if (p != null
        && p.first.decrementAndGet() == 0
        && instance.getAndSet(null) === p) {
      if (p.first.get() > 0) {
        instance.set(p)
      } else {
        instance.set(internalFactory)
        p.second.value.close()
      }
    }
  }
}
