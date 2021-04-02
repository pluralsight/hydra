import sbt.{ExclusionRule, _}

object Dependencies {

  val akkaHTTPCorsVersion = "1.0.0"
  val akkaHTTPVersion = "10.1.13"
  val akkaKafkaStreamVersion = "2.0.4"
  val akkaVersion = "2.6.7"
  val avroVersion = "1.10.0"
  val catsEffectVersion = "2.3.1"
  val catsLoggerVersion = "1.2.0"
  val catsRetryVersion = "2.1.0"
  val catsVersion = "2.2.0"
  val cirisVersion = "1.2.1"
  val confluentVersion = "5.4.2"
  val fs2KafkaVersion = "1.0.0"
  val jacksonCoreVersion = "2.10.4"
  val jacksonDatabindVersion = "2.10.4"
  val jodaConvertVersion = "2.2.1"
  val jodaTimeVersion = "2.10.9"
  val kafkaVersion = "2.4.1"
  val kamonPVersion = "2.1.10"
  val kamonVersion = "2.1.10"
  val log4jVersion = "2.13.3"
  val refinedVersion = "0.9.20"
  val reflectionsVersion = "0.9.12"
  val scalaCacheVersion = "0.28.0"
  val scalaMockVersion = "5.1.0"
  val scalaTestVersion = "3.2.3"
  val sprayJsonVersion = "1.3.6"
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
      "org.typelevel" %% "log4cats-slf4j" % catsLoggerVersion,
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

    val retry = "com.softwaremill.retry" %% "retry" % "0.3.3"

    val embeddedKafka = "io.github.embeddedkafka" %% "embedded-kafka" % "2.4.1.1" % "test"

    lazy val kamon = Seq(
      "io.kamon" %% "kamon-core" % kamonVersion,
      "io.kamon" %% "kamon-prometheus" % kamonPVersion
    )

    val kafka = Seq(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      embeddedKafka
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

  }

  // oneOf test, it, main
  object TestLibraries {

    def getTestLibraries(module: String): Seq[ModuleID] = {
      val akkaTest = Seq(
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % module,
        "com.typesafe.akka" %% "akka-http-testkit" % akkaHTTPVersion % module,
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % module
      )

      val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % module

      val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVersion % module
      val junit = "junit" % "junit" % "4.13.1" % module
      akkaTest ++ Seq(scalaTest, scalaMock, junit)
    }

  }

  object Integration {
    private val testcontainersJavaVersion = "1.15.1"
    val testContainers = Seq(
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testContainersVersion % "it",
      "com.dimafeng" %% "testcontainers-scala-kafka" % testContainersVersion % "it",
      "org.testcontainers" % "testcontainers"                  % testcontainersJavaVersion  % "it",
      "org.testcontainers" % "database-commons"                % testcontainersJavaVersion  % "it",
      "org.testcontainers" % "jdbc"                            % testcontainersJavaVersion  % "it"
    )

  }

  import Compile._
  import Test._
  import Integration._

  val testDeps: Seq[ModuleID] = TestLibraries.getTestLibraries(module = "test")

  val integrationDeps: Seq[ModuleID] = testContainers ++ TestLibraries.getTestLibraries(module = "it")

  val baseDeps: Seq[ModuleID] =
    akka ++ Seq(avro) ++ cats ++ logging ++ joda ++ testDeps

  val avroDeps: Seq[ModuleID] =
    baseDeps ++ confluent ++ jackson ++ guavacache ++ catsEffect

  val coreDeps: Seq[ModuleID] = akka ++ baseDeps ++
    Seq(
      reflections,
      retry
    ) ++ guavacache ++
    confluent ++ kamon

  val ingestDeps: Seq[ModuleID] = coreDeps ++ akkaHttpHal ++ Seq(ciris, embeddedKafka, sprayJson)

  val kafkaDeps: Seq[ModuleID] = coreDeps ++ Seq(
    akkaKafkaStream,
    refined
  ) ++ kafka ++ akkaHttpHal ++ vulcan ++ fs2Kafka ++ integrationDeps

}
