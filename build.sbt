import sbt.Keys.version
import sbt._

enablePlugins(JavaAppPackaging)

val JDK = "1.8"
val buildNumber = scala.util.Properties.envOrNone("version").map(v => "." + v).getOrElse("")
val hydraVersion = "0.9.5" + buildNumber

lazy val defaultSettings = Seq(
  organization := "pluralsight",
  version := hydraVersion,
  scalaVersion := "2.12.2",
  description := "Hydra",
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  excludeDependencies += "org.slf4j" % "slf4j-log4j12",
  excludeDependencies += "log4j" % "log4j",
  coverageEnabled := true,
  packageOptions in(Compile, packageBin) +=
    Package.ManifestAttributes("Implementation-Build" -> buildNumber),
  logLevel := Level.Info,
  scalacOptions ++= Seq("-encoding", "UTF-8", "-feature", "-language:_", "-deprecation", "-unchecked"),
  javacOptions in Compile ++= Seq("-encoding", "UTF-8", "-source", JDK, "-target", JDK,
    "-Xlint:unchecked", "-Xlint:deprecation", "-Xlint:-options"),

  resolvers += Resolver.mavenLocal,
  resolvers += "Scalaz Bintray Repo" at "https://dl.bintray.com/scalaz/releases",
  resolvers += "Confluent Maven Repo" at "http://packages.confluent.io/maven/",
  resolvers += "jitpack" at "https://jitpack.io",
  coverageExcludedPackages := "hydra\\.ingest\\.HydraIngestApp.*",
  ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet
)

lazy val restartSettings = Seq(
  javaOptions in reStart += "-Xmx2g",
  mainClass in reStart := Some("hydra.sandbox.app.HydraIngest")
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  // required until these tickets are closed https://github.com/sbt/sbt-pgp/issues/42,
  // https://github.com/sbt/sbt-pgp/issues/36
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))
)

lazy val moduleSettings = defaultSettings ++ Test.testSettings //++ Publish.settings

lazy val root = Project(
  id = "hydra",
  base = file(".")
).settings(defaultSettings).aggregate(common, core, avro, ingest, kafka, sql, sandbox)

lazy val common = Project(
  id = "common",
  base = file("common")
).settings(moduleSettings, name := "hydra-common", libraryDependencies ++= Dependencies.baseDeps)

lazy val core = Project(
  id = "core",
  base = file("core")
).dependsOn(common, avro)
  .settings(moduleSettings, name := "hydra-core", libraryDependencies ++= Dependencies.coreDeps)


lazy val ingest = Project(
  id = "ingest",
  base = file("ingest")
).dependsOn(core)
  .settings(moduleSettings, name := "hydra-ingest", libraryDependencies ++= Dependencies.coreDeps)

lazy val kafka = Project(
  id = "kafka",
  base = file("kafka")
).dependsOn(core).settings(moduleSettings, name := "hydra-kafka", libraryDependencies ++= Dependencies.kafkaDeps)

lazy val avro = Project(
  id = "avro",
  base = file("avro")
).settings(moduleSettings, name := "hydra-avro", libraryDependencies ++= Dependencies.avroDeps)

lazy val sql = Project(
  id = "sql",
  base = file("sql")
).dependsOn(core)
  .settings(moduleSettings, name := "hydra-sql", libraryDependencies ++= Dependencies.sqlDeps)

lazy val jdbc = Project(
  id = "jdbc",
  base = file("jdbc")
).dependsOn(sql)
  .settings(moduleSettings, name := "hydra-jdbc", libraryDependencies ++= Dependencies.sqlDeps)

lazy val rabbitmq = Project(
  id = "rabbitmq",
  base = file("rabbitmq")
).dependsOn(core)
  .settings(moduleSettings, name := "hydra-rabbitmq", libraryDependencies ++= Dependencies.rabbitDeps)

val sbSettings = defaultSettings ++ Test.testSettings ++ noPublishSettings ++ restartSettings
lazy val sandbox = Project(
  id = "sandbox",
  base = file("sandbox")
).dependsOn(ingest, kafka, jdbc)
  .settings(sbSettings, name := "hydra-examples", libraryDependencies ++= Dependencies.sandboxDeps)

//scala style
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := scalastyle.in(sbt.Test).toTask("").value