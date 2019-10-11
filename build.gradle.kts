plugins {
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
}

repositories {
  jcenter()
}
dependencies {
  implementation(kotlin("stdlib"))

  api("org.apache.kafka", "kafka-streams", "2.3.0")
  implementation("software.amazon.awssdk", "dynamodb", "2.9.17")
}
