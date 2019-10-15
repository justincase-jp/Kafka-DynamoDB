package jp.justincase.kafka.dynamodb.test.utility

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import java.io.File
import java.net.URI
import java.net.URLClassLoader

object EmbeddedDynamoDb {
  val localEndpoint = URI("http://localhost:8000")

  init {
    val names = listOf(
        """libsqlite4java-linux-amd64-.*\.so""".toRegex(),
        """libsqlite4java-osx-.*\.dylib""".toRegex()
    )
    (ClassLoader.getSystemClassLoader() as URLClassLoader)
        .urLs
        .filter { it.protocol == "file" }
        .map { File(it.toURI()) }
        .filter { f -> names.any { it.matches(f.name) } }
        .forEach {
          try {
            System.load(it.path)
          } catch (_: UnsatisfiedLinkError) {
          }
        }

    ServerRunner.main(arrayOf("-inMemory", "-sharedDb"))
  }
}
