[![Release](https://jitpack.io/v/io.github.justincase-jp/kafka-dynamodb.svg)](
  https://jitpack.io/#io.github.justincase-jp/kafka-dynamodb
)

Kafka DynamoDB
===
Kafka Streams state stores backed by AWS DynamoDB

## Usage
```kt
val storeSettings = DynamoDbStoreSettings.Value(
    "http://dynamodb.$REGION.amazonaws.com",
    "tableName",
    null // a key/secret pair or `null` for using contextual credential i.e. running on AWS
    // hashKeyColumn = "key",
    // sortKeyColumn = "type",
    // valueColumn = "value",
)
val factory = storeSettings.keyValueStoreBuilderSupplier()
val storeBuilder = factory("storeName", keySerde, valueSerde)
streamsBuilder.addStateStore(storeBuilder)

// Access state store with `ProcessorContext`
val store = processorContext.getStateStore("storeName")

// ...
```

The idea of this library is to serve a single DynamoDB table as multiple state stores.
`hashKeyColumn` (*binary type*) and `valueColumn` (*binary type*) is the key and value of normal CRUD operations on the store.
And `sortKeyColumn` (*string type*) is the store name.

You may share the underlying DynamoDB client instance with other parts of your program, as such:
```kt
val clientSettings = DynamoDbClientSettings(
    "http://dynamodb.$REGION.amazonaws.com",
    null // a key/secret pair or `null` for using contextual credential i.e. running on AWS
)
val tableSettings = DynamoDbTableSettings(
    "tableName",
    hashKeyColumn = "key",
    sortKeyColumn = "type",
    valueColumn = "value",
)
val sharedClient = SharedReference(clientSettings::createSynchronousClient)

val factory = sharedClient.keyValueStoreBuilderSupplier()
val storeBuilder = factory("storeName", keySerde, valueSerde)
streamsBuilder.addStateStore(storeBuilder)

// Access state store with `ProcessorContext`
val store = processorContext.getStateStore("storeName")

// ...
```

In which `SharedReference` is a reference counting wrapper that creates and destroys `AutoCloseable` instances automatically,
by increasing counts for calls to `fun open(): Pair<Lazy<Unit>, T>`,
and decreasing counts for unwrapping the returned `Lazy<Unit>`.
