import sbt.{ExclusionRule, _}

object Dependencies {

  val akkaHTTPCorsVersion = "1.0.0"
  val akkaHTTPVersion = "10.1.12"
  val akkaKafkaStreamVersion = "2.0.4"
  val akkaVersion = "2.6.7"
  val avroVersion = "1.10.0"
  val catsEffectVersion = "2.3.0"
  val catsLoggerVersion = "1.1.1"
  val catsRetryVersion = "1.1.1"
  val catsVersion = "2.2.0"
  val cirisVersion = "1.2.1"
  val confluentVersion = "5.4.2"
  val easyMockVersion = "4.2" //needed for mocking static java methods
  val fs2KafkaVersion = "1.0.0"
  val h2DbVersion = "1.4.200"
  val jacksonCoreVersion = "2.10.4"
  val jacksonDatabindVersion = "2.10.4"
  val jodaConvertVersion = "2.2.1"
  val jodaTimeVersion = "2.10.6"
  val kafkaVersion = "2.4.1"
  val kamonPVersion = "2.1.4"
  val kamonVersion = "2.1.4"
  val log4jVersion = "2.13.3"
  val powerMockVersion = "2.0.7" //needed for mocking static java methods
  val refinedVersion = "0.9.19"
  val reflectionsVersion = "0.9.12"
  val scalaCacheVersion = "0.28.0"
  val scalaMockVersion = "5.1.0"
  val scalaTestVersion = "3.2.2"
  val scalazVersion = "7.3.2"
  val sprayJsonVersion = "1.3.5"
  val testContainersVersion = "0.38.8"
  val typesafeConfigVersion = "1.3.2"
  val vulcanVersion = "1.2.0"


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

    val fs2Kafka = Seq(
      "com.github.fd4s" %% "fs2-kafka" % fs2KafkaVersion
    )

    val ciris = "is.cir" %% "ciris" % cirisVersion


    val typesafeConfig = "com.typesafe" % "config" % typesafeConfigVersion

    val sprayJson = "io.spray" %% "spray-json" % sprayJsonVersion

    val scalaz = "org.scalaz" %% "scalaz-core" % scalazVersion

    val retry = "com.softwaremill.retry" %% "retry" % "0.3.3"

    val embeddedKafka = "net.manub" %% "scalatest-embedded-kafka" % "2.0.0"

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
      ("com.github.marcuslange" % "akka-http-hal" % "1.2.5")
        .excludeAll(ExclusionRule(organization = "io.spray"))
    )


    val akkaKafkaStream =
      "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafkaStreamVersion

    val avro = "org.apache.avro" % "avro" % avroVersion

    val jsonLenses = "net.virtual-void" %% "json-lenses" % "0.6.2"

    val joda = Seq(
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.joda" % "joda-convert" % jodaConvertVersion
    )

    val guavacache =
      Seq(
        "com.github.cb372" %% "scalacache-guava",
        "com.github.cb372" %% "scalacache-cats-effect"
      ).map(_ % scalaCacheVersion)

    val reflections = "org.reflections" % "reflections" % reflectionsVersion

    val jackson = Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonCoreVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDatabindVersion
    )

    val postgres = "org.postgresql" % "postgresql" % "42.2.14"
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

  object Integration {
    private val testcontainersJavaVersion = "1.15.1"
    val testContainers = Seq(
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testContainersVersion % "it",
      "com.dimafeng" %% "testcontainers-scala-kafka" % testContainersVersion % "it",
      "org.testcontainers" % "testcontainers"                  % testcontainersJavaVersion  % "it",
      "org.testcontainers" % "database-commons"                % testcontainersJavaVersion  % "it",
      "org.testcontainers" % "postgresql"                      % testcontainersJavaVersion  % "it",
      "org.testcontainers" % "jdbc"                            % testcontainersJavaVersion  % "it"
    )

  }

  import Compile._
  import Test._
  import Integration._

  val integrationDeps: Seq[ModuleID] = testContainers

  val testDeps: Seq[ModuleID] =
    Seq(scalaTest, junit, scalaMock, easyMock, embeddedPostgres) ++
      powerMock ++ akkaTest

  val baseDeps: Seq[ModuleID] =
    akka ++ Seq(scalaz, avro) ++ cats ++ logging ++ joda ++ testDeps

  val avroDeps: Seq[ModuleID] =
    baseDeps ++ confluent ++ jackson ++ guavacache ++ catsEffect

  val coreDeps: Seq[ModuleID] = akka ++ baseDeps ++
    Seq(
      reflections,
      postgres,
      h2db,
      retry
    ) ++ guavacache ++
    confluent ++ kamon

  val ingestDeps: Seq[ModuleID] = coreDeps ++ akkaHttpHal ++ Seq(ciris, embeddedKafka)

  val kafkaDeps: Seq[ModuleID] = coreDeps ++ Seq(
    akkaKafkaStream,
    jsonLenses,
    refined
  ) ++ kafka ++ akkaHttpHal ++ vulcan ++ fs2Kafka ++ integrationDeps

}
