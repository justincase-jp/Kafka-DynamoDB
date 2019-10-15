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
  jcenter()
}
dependencies {
  implementation(kotlin("stdlib"))

  api("org.apache.kafka", "kafka-streams", "2.3.0")
  implementation("software.amazon.awssdk", "dynamodb", "2.9.20")

  testImplementation("io.kotlintest", "kotlintest-runner-junit5", "3.4.2")
}
