plugins {
  maven
  `java-library`
  kotlin("jvm") version "1.3.50"
}

tasks {
  named<Wrapper>("wrapper") {
    gradleVersion = "5.6.2"
  }

  arrayOf(compileKotlin, compileTestKotlin).forEach {
    it {
      kotlinOptions.jvmTarget = JavaVersion.VERSION_1_8.toString()
    }
  }

  named<Test>("test") {
    useJUnitPlatform()
  }
}

repositories {
  mavenCentral()
  maven("https://dynamodb-local.s3-us-west-2.amazonaws.com/release")
}
dependencies {
  implementation(kotlin("stdlib"))

  api("org.apache.kafka", "kafka-streams", "2.3.0")
  implementation("software.amazon.awssdk", "dynamodb", "2.9.20")
  implementation("com.google.guava:guava:28.2-jre") {
    exclude("com.google.code.findbugs", "jsr305")
    exclude("org.checkerframework", "checker-qual")
    exclude("com.google.errorprone", "error_prone_annotations")
    exclude("com.google.j2objc", "j2objc-annotations")
    exclude("org.codehaus.mojo", "animal-sniffer-annotations")
  }

  testImplementation("io.kotlintest", "kotlintest-runner-junit5", "3.4.2")
  testImplementation("com.amazonaws", "DynamoDBLocal", "1.11.477")
}
