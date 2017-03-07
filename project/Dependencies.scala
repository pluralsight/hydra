import sbt.{ExclusionRule, _}


object Dependencies {

  val akkaVersion = "2.4.16"
  val scalaTestVersion = "2.2.6"
  val slf4jVersion = "1.7.21"
  val log4jVersion = "2.7"
  val kxbmapConfigVersion = "0.4.4"
  val typesafeConfigVersion = "1.3.0"
  val avroVersion = "1.8.1"
  val springVersion = "4.2.2.RELEASE"
  val jodaTimeVersion = "2.9.3"
  val jodaConvertVersion = "1.8.1"
  val confluentVersion = "3.1.0"
  val sprayJsonVersion = "1.3.2"
  val kafkaVersion = "0.10.2.0"
  val reflectionsVersion = "0.9.10"
  val slackVersion = "0.3.0"
  val akkaHTTPVersion = "10.0.3"
  val akkaKafkaStreamVersion = "0.13"
  val kafkaUnitVersion = "0.6"

  object Compile {

    val scalaConfigs = "com.github.kxbmap" %% "configs" % kxbmapConfigVersion

    val typesafeConfig = "com.typesafe" % "config" % typesafeConfigVersion

    val sprayJson = "io.spray" %% "spray-json" % sprayJsonVersion

    val kafka = Seq(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "info.batey.kafka" % "kafka-unit" % kafkaUnitVersion % "test")

    val confluent = Seq("io.confluent" % "kafka-schema-registry-client" % confluentVersion,
      "io.confluent" % "kafka-avro-serializer" % confluentVersion).map(_.excludeAll(
      ExclusionRule(organization = "org.codehaus.jackson"),
      ExclusionRule(organization = "com.fasterxml.jackson.core")))

    val slf4j = Seq("org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion)

    val akka = Seq("com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.0",
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion)

    val akkaKafkaStream = "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafkaStreamVersion

    val avro = "org.apache.avro" % "avro" % avroVersion

    val spring = "org.springframework" % "spring-core" % springVersion

    val jsonLenses = "net.virtual-void" %% "json-lenses" % "0.6.1"

    val joda = Seq("joda-time" % "joda-time" % jodaTimeVersion, "org.joda" % "joda-convert" % jodaConvertVersion)

    val guavacache = "com.github.cb372" %% "scalacache-guava" % "0.7.5"

    val reflections = "org.reflections" % "reflections" % reflectionsVersion

    val serviceContainer = ("com.github.vonnagy" %% "service-container" % "2.0.4")
      .excludeAll(
        ExclusionRule(organization = "ch.qos.logback"),
        ExclusionRule(organization = "org.slf4j")
      )

    val slackApi = "com.flyberrycapital" %% "scala-slack" % slackVersion
  }

  object Test {
    val akkaTest = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    val scalazStream = "org.scalaz.stream" %% "scalaz-stream" % "0.7a" % "test"
    val junit = "junit" % "junit" % "4.12" % "test"
  }

  import Compile._
  import Test._

  val testDeps = Seq(akkaTest, scalazStream, scalaTest, junit)

  val baseDeps = akka ++ slf4j ++ Seq(scalaConfigs, avro, spring, serviceContainer) ++ joda ++ testDeps

  val coreDeps = baseDeps ++ Seq(guavacache, reflections, slackApi) ++ confluent ++ kafka

  val transportDeps = coreDeps ++ Seq(akkaKafkaStream, jsonLenses)

  val overrides = Set(slf4j, typesafeConfig, joda)
}