import sbt.{ExclusionRule, _}

object Dependencies {

  val aeronVersion = "1.26.0"
  val akkaHTTPCorsVersion = "0.4.2"
  val akkaHTTPVersion = "10.1.11"
  val akkaKafkaStreamVersion = "2.0.2"
  val akkaKryoVersion = "0.5.2"
  val akkaVersion = "2.6.1"
  val avroVersion = "1.9.2"
  val catsEffectVersion = "2.0.0"
  val catsLoggerVersion = "1.0.1"
  val catsRetryVersion = "1.1.0"
  val catsVersion = "2.0.0"
  val cirisVersion = "1.0.4"
  val confluentVersion = "5.4.0"
  val easyMockVersion = "3.5.1" //needed for mocking static java methods
  val fs2KafkaVersion = "1.0.0"
  val hikariCPVersion = "2.7.9"
  val h2DbVersion = "1.4.196"
  val jacksonVersion = "2.9.10"
  val jodaConvertVersion = "1.8.3"
  val jodaTimeVersion = "2.9.9"
  val kafkaVersion = "2.4.0"
  val kamonPVersion = "2.0.1"
  val kamonVersion = "2.0.4"
  val kxbmapConfigVersion = "0.4.4"
  val log4jVersion = "2.7"
  val opRabbitVersion = "2.1.0"
  val powerMockVersion = "2.0.5" //needed for mocking static java methods
  val refinedVersion = "0.9.12"
  val reflectionsVersion = "0.9.12"
  val scalaCacheVersion = "0.28.0"
  val scalaMockVersion = "4.4.0"
  val scalaTestVersion = "3.1.1"
  val scalazVersion = "7.2.30"
  val serviceContainerVersion = "2.1.0"
  val sprayJsonVersion = "1.3.5"
  val typesafeConfigVersion = "1.3.2"
  val vulcanVersion = "1.0.1"

  object Compile {

    val refined = "eu.timepit" %% "refined" % refinedVersion

    val vulcan: Seq[ModuleID] = Seq(
      "com.github.fd4s" %% "vulcan",
      "com.github.fd4s" %% "vulcan-generic",
      "com.github.fd4s" %% "vulcan-refined"
    ).map(_ % vulcanVersion)

    val cats = Seq(
      "com.github.cb372" %% "cats-retry" % catsRetryVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % catsLoggerVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    )

    lazy val catsEffect = Seq(
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "com.github.cb372" %% "cats-retry" % catsRetryVersion
    )

    val fs2Kafka = "com.github.fd4s" %% "fs2-kafka" % fs2KafkaVersion

    val ciris = "is.cir" %% "ciris" % cirisVersion

    val scalaConfigs = "com.github.kxbmap" %% "configs" % kxbmapConfigVersion

    val typesafeConfig = "com.typesafe" % "config" % typesafeConfigVersion

    val sprayJson = "io.spray" %% "spray-json" % sprayJsonVersion

    val scalaz = "org.scalaz" %% "scalaz-core" % scalazVersion

    val retry = "com.softwaremill.retry" %% "retry" % "0.3.3"

    val embeddedKafka = "net.manub" %% "scalatest-embedded-kafka" % "2.0.0"

    val sdNotify = "info.faljse" % "SDNotify" % "1.1"

    lazy val kamon = Seq(
      "io.kamon" %% "kamon-core" % kamonVersion,
      "io.kamon" %% "kamon-prometheus" % kamonPVersion
    )

    val kafka = Seq(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      embeddedKafka % "test"
    )

    val confluent: Seq[ModuleID] =
      Seq("io.confluent" % "kafka-avro-serializer" % confluentVersion).map(
        _.excludeAll(
          ExclusionRule(organization = "org.codehaus.jackson"),
          ExclusionRule(organization = "com.fasterxml.jackson.core")
        )
      )

    val logging = Seq(
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion
    )

    val akka = Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
      "ch.megard" %% "akka-http-cors" % akkaHTTPCorsVersion,
      "org.iq80.leveldb" % "leveldb" % "0.7",
      "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"
    )

    val akkaHttpHal = Seq(
      ("com.github.marcuslange" % "akka-http-hal" % "1.2.1")
        .excludeAll(ExclusionRule(organization = "io.spray"))
    )

    val serviceContainer =
      ("com.github.vonnagy" %% "service-container" % serviceContainerVersion)
        .excludeAll(
          ExclusionRule(organization = "ch.qos.logback"),
          ExclusionRule(organization = "org.slf4j"),
          ExclusionRule(organization = "com.typesafe.akka")
        )

    val akkaKryo =
      ("com.github.romix.akka" %% "akka-kryo-serialization" % akkaKryoVersion)
        .excludeAll {
          ExclusionRule(organization = "com.typesafe.akka")
        }

    val akkaKafkaStream =
      "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafkaStreamVersion

    val avro = "org.apache.avro" % "avro" % avroVersion

    val jsonLenses = "net.virtual-void" %% "json-lenses" % "0.6.2"

    val joda = Seq(
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.joda" % "joda-convert" % jodaConvertVersion
    )

    val guavacache =
      "com.github.cb372" %% "scalacache-guava" % scalaCacheVersion

    val reflections = "org.reflections" % "reflections" % reflectionsVersion

    val hikariCP = "com.zaxxer" % "HikariCP" % hikariCPVersion

    val opRabbit = Seq(
      "com.spingo" %% "op-rabbit-core" % opRabbitVersion,
      "com.spingo" %% "op-rabbit-json4s" % opRabbitVersion,
      "com.spingo" %% "op-rabbit-airbrake" % opRabbitVersion
    )

    val jackson = Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
    )

    val postgres = "org.postgresql" % "postgresql" % "42.2.4"

    val aeron: Seq[ModuleID] = Seq(
      "io.aeron" % "aeron-driver",
      "io.aeron" % "aeron-client"
    ).map(_ % aeronVersion)
  }

  object Test {

    val akkaTest = Seq(
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHTTPVersion % "test",
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
    )

    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    val easyMock = "org.easymock" % "easymock" % easyMockVersion % "test"

    val powerMock = Seq(
      "org.powermock" % "powermock-api-easymock" % powerMockVersion % "test",
      "org.powermock" % "powermock-module-junit4" % powerMockVersion % "test"
    )

    val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVersion % "test"
    val junit = "junit" % "junit" % "4.13" % "test"

    val h2db = "com.h2database" % "h2" % h2DbVersion % "test"

    val embeddedPostgres =
      "com.opentable.components" % "otj-pg-embedded" % "0.13.3" % "test"
  }

  import Compile._
  import Test._

  val testDeps: Seq[ModuleID] =
    Seq(scalaTest, junit, scalaMock, easyMock, embeddedPostgres) ++
      powerMock ++ akkaTest

  val baseDeps: Seq[ModuleID] =
    akka ++ Seq(scalaz, scalaConfigs, avro) ++ cats ++ logging ++ joda ++ testDeps

  val sqlDeps: Seq[ModuleID] =
    logging ++ Seq(scalaConfigs, avro, hikariCP, h2db) ++ joda ++ testDeps

  val avroDeps: Seq[ModuleID] =
    baseDeps ++ confluent ++ jackson ++ Seq(guavacache) ++ catsEffect

  val coreDeps: Seq[ModuleID] = akka ++ baseDeps ++
    Seq(
      guavacache,
      reflections,
      akkaKryo,
      serviceContainer,
      sdNotify,
      postgres,
      h2db,
      retry
    ) ++
    confluent ++ kamon ++ aeron

  val ingestDeps: Seq[ModuleID] = coreDeps ++ akkaHttpHal ++ Seq(ciris)

  val rabbitDeps: Seq[ModuleID] =
    logging ++ Seq(scalaConfigs) ++ joda ++ opRabbit ++ testDeps

  val kafkaDeps: Seq[ModuleID] = coreDeps ++ Seq(
    akkaKafkaStream,
    jsonLenses,
    fs2Kafka,
    refined
  ) ++ kafka ++ akkaHttpHal ++ vulcan

  val overrides = Set(logging, typesafeConfig, joda)
}
