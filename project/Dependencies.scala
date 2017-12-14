import sbt.{ExclusionRule, _}


object Dependencies {

  val akkaVersion = "2.5.7"
  val scalaTestVersion = "3.0.4"
  val slf4jVersion = "1.7.29"
  val log4jVersion = "2.7"
  val kxbmapConfigVersion = "0.4.4"
  val typesafeConfigVersion = "1.3.1"
  val avroVersion = "1.8.1"
  val springVersion = "4.2.2.RELEASE"
  val jodaTimeVersion = "2.9.9"
  val jodaConvertVersion = "1.8.1"
  val confluentVersion = "3.2.0"
  val sprayJsonVersion = "1.3.c2"
  val kafkaVersion = "0.10.2.1"
  val reflectionsVersion = "0.9.11"
  val akkaHTTPVersion = "10.0.9"
  val akkaKafkaStreamVersion = "0.14"
  val scalazVersion = "7.2.9"
  val scalaMockVersion = "3.5.0"
  val serviceContainerVersion = "2.0.6"
  val scalaCacheVersion = "0.9.3"
  val postgresVersion = "9.4.1209"
  val commonsDbcpVersion = "1.4"
  val hikariCPVersion = "2.6.2"
  val jacksonVersion = "2.8.4"
  val opRabbitVersion = "2.0.0"
  val constructRVersion = "0.18.0"
  val akkaHTTPCorsVersion = "0.2.2"
  val akkaClusterManagementHttpVersion = "0.6"
  val akkaKryoVersion = "0.5.1"

  object Compile {

    val scalaConfigs = "com.github.kxbmap" %% "configs" % kxbmapConfigVersion

    val typesafeConfig = "com.typesafe" % "config" % typesafeConfigVersion

    val sprayJson = "io.spray" %% "spray-json" % sprayJsonVersion

    val scalaz = "org.scalaz" %% "scalaz-core" % scalazVersion

    val kafka = Seq(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "net.manub" %% "scalatest-embedded-kafka" % "0.14.0" % "test")

    val confluent = Seq("io.confluent" % "kafka-schema-registry-client" % confluentVersion,
      "io.confluent" % "kafka-avro-serializer" % confluentVersion).map(_.excludeAll(
      ExclusionRule(organization = "org.codehaus.jackson"),
      ExclusionRule(organization = "com.fasterxml.jackson.core")))

    val logging = Seq(
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion)

    val akka = Seq("com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
      "com.github.romix.akka" %% "akka-kryo-serialization" % akkaKryoVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
      "com.lightbend.akka" %% "akka-management-cluster-http" % akkaClusterManagementHttpVersion,
      "ch.megard" %% "akka-http-cors" % akkaHTTPCorsVersion,
      "org.iq80.leveldb" % "leveldb" % "0.7",
      "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8")

    val akkaKafkaStream = "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafkaStreamVersion

    val avro = "org.apache.avro" % "avro" % avroVersion

    val spring = "org.springframework" % "spring-core" % springVersion

    val jsonLenses = "net.virtual-void" %% "json-lenses" % "0.6.2"

    val joda = Seq("joda-time" % "joda-time" % jodaTimeVersion, "org.joda" % "joda-convert" % jodaConvertVersion)

    val guavacache = "com.github.cb372" %% "scalacache-guava" % scalaCacheVersion

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

    val serviceContainer = ("com.github.vonnagy" %% "service-container" % serviceContainerVersion)
      .excludeAll(
        ExclusionRule(organization = "ch.qos.logback"),
        ExclusionRule(organization = "org.slf4j")
      )

    val constructR = Seq(
      "de.heikoseeberger" %% "constructr" % constructRVersion,
      "com.lightbend.constructr" %% "constructr-coordination-zookeeper" % "0.4.0" //if using zk
    )
  }

  object Test {
    val akkaTest = Seq("com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHTTPVersion % "test",
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test")

    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    val scalaMock = "org.scalamock" %% "scalamock-scalatest-support" % scalaMockVersion % "test"
    val junit = "junit" % "junit" % "4.12" % "test"

    val h2db = "com.h2database" % "h2" % "1.4.196" % "test"
  }

  import Compile._
  import Test._

  val testDeps = Seq(scalaTest, junit, scalaMock) ++ akkaTest

  val baseDeps = akka ++ logging ++ Seq(scalaz, scalaConfigs, avro, spring) ++ joda ++ testDeps

  val avroDeps = baseDeps ++ confluent ++ jackson ++ Seq(guavacache)

  val coreDeps = akka ++ baseDeps ++ Seq(guavacache, reflections, serviceContainer) ++ confluent ++ constructR

  val ingestDeps = coreDeps

  val sqlDeps = logging ++ Seq(scalaConfigs, avro, hikariCP, h2db) ++ joda ++ testDeps

  val rabbitDeps = logging ++ Seq(scalaConfigs) ++ joda ++ opRabbit ++ testDeps

  val kafkaDeps = coreDeps ++ Seq(akkaKafkaStream, jsonLenses) ++ kafka

  val sandboxDeps = kafkaDeps ++ sqlDeps ++ Seq("com.h2database" % "h2" % "1.4.196")

  val overrides = Set(logging, typesafeConfig, joda)
}