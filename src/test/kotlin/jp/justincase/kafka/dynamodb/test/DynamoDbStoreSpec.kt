package jp.justincase.kafka.dynamodb.test

import io.kotlintest.properties.Gen
import io.kotlintest.properties.assertAll
import io.kotlintest.shouldBe
import io.kotlintest.specs.WordSpec
import jp.justincase.kafka.dynamodb.DynamoDbStore
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import java.net.URI
import java.util.*

class DynamoDbStoreSpec : WordSpec({
  val client = DynamoDbClient.builder().endpointOverride(URI("http://localhost:8000")).build()
  val uuidString = Gen.uuid().map(UUID::toString)

  val stores = Gen.bind(uuidString, uuidString, uuidString, uuidString, uuidString) { t, h, s, v, n ->
    client.createTable { table ->
      table.provisionedThroughput {
        it.readCapacityUnits(1)
        it.writeCapacityUnits(1)
      }
      table.keySchema(
          KeySchemaElement.builder().attributeName(h).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(s).keyType(KeyType.RANGE).build()
      )
      table.attributeDefinitions(
          AttributeDefinition.builder().attributeName(h).attributeType(ScalarAttributeType.B).build(),
          AttributeDefinition.builder().attributeName(s).attributeType(ScalarAttributeType.S).build()
      )
      table.tableName(t)
    }
    DynamoDbStore(client, t, h, s, v, n)
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
