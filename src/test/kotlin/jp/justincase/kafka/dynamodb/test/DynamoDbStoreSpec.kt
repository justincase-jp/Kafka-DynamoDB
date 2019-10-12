package jp.justincase.kafka.dynamodb.test

import io.kotlintest.properties.Gen
import io.kotlintest.properties.assertAll
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import jp.justincase.kafka.dynamodb.*
import jp.justincase.kafka.dynamodb.auxiliary.createSynchronousClient
import kotlinx.coroutines.runBlocking
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import java.net.URI
import java.util.*

class DynamoDbStoreSpec : WordSpec({
  val clientSettings = DynamoDbClientSettings(URI("http://localhost:8000"))
  val client = clientSettings.createSynchronousClient()
  val uuidString = Gen.uuid().map(UUID::toString)

  val stores = Gen.bind(uuidString, uuidString, uuidString, uuidString, uuidString) { t, n, h, s, v ->
    val tableSettings = DynamoDbTableSettings(t, h, s, v)

    runBlocking {
      clientSettings.createTable(
          DynamoDbTableThroughputSettings(1, 1),
          tableSettings
      )
    }
    DynamoDbStore(client, n, tableSettings)
  }

  val byteArrayGen = Gen.list(Gen.byte()).map { it.toByteArray() }
  val bytesGen = byteArrayGen.map(::Bytes)


  "`DynamoDbKeyValueByteStore`" should {
    "be consistent on `put` and `get`" {
      val store = stores.next()

      assertAll(100, bytesGen, byteArrayGen) { key, value ->
        store.put(key, value)

        store.get(key) shouldBe value
      }
    }
    "be consistent on `putAll` and `get`" {
      val store = stores.next()

      assertAll(
          100,
          Gen.list(Gen.bind(bytesGen, byteArrayGen, ::KeyValue))
      ) { entries ->
        store.putAll(entries)

        entries
            .associateBy({ it.key }) { it.value }
            .forEach { (k, v) ->
              store.get(k) shouldBe v
            }
      }
    }
    "be consistent on `putIfAbsent` and `get`" {
      val store = stores.next()

      assertAll(100, bytesGen, byteArrayGen, byteArrayGen) { key, value1, value2 ->
        val expected = when (val v = store.get(key)) {
          null -> {
            store.putIfAbsent(key, value1)
            value1
          }
          else -> v
        }
        store.putIfAbsent(key, value2)

        store.get(key) shouldBe expected
      }
    }
    "be consistent on `delete` and `put`" {
      val store = stores.next()

      assertAll(100, bytesGen, byteArrayGen) { key, value ->
        store.put(key, value)

        store.delete(key) shouldBe value
        store.delete(key) shouldBe null
      }
    }
    "return `null` on clean `get`" {
      val store = stores.next()

      assertAll(100, bytesGen) { key ->
        store.get(key) shouldBe null
      }
    }
    "return `null` on clean `delete`" {
      val store = stores.next()

      assertAll(100, bytesGen) { key ->
        store.delete(key) shouldBe null
      }
    }
  }
})
