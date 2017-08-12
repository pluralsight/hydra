import sbt.Keys.version
import sbt._

enablePlugins(JavaAppPackaging)

val JDK = "1.8"
val buildNumber = scala.util.Properties.envOrNone("version").map(v => "." + v).getOrElse("")
val hydraVersion = "0.8.1" + buildNumber

lazy val defaultSettings = Seq(
  organization := "pluralsight",
  version := hydraVersion,
  scalaVersion := "2.12.1",
  description := "Hydra",
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  excludeDependencies += "org.slf4j" % "slf4j-log4j12",
  excludeDependencies += "log4j" % "log4j",
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

  ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
  ivyScala := ivyScala.value map (_.copy(overrideScalaVersion = true))
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
  base = file("."),
  settings = defaultSettings // ++ noPublishSettings
).aggregate(common, core, kafka, ingest, sandbox)

lazy val common = Project(
  id = "common",
  base = file("common"),
  settings = moduleSettings
    ++ Seq(libraryDependencies ++= Dependencies.baseDeps)
).settings(name := "hydra-common")

lazy val core = Project(
  id = "core",
  base = file("core"),
  settings = moduleSettings
    ++ Seq(libraryDependencies ++= Dependencies.coreDeps)
).dependsOn(common).settings(name := "hydra-core")

lazy val ingest = Project(
  id = "ingest",
  base = file("ingest"),
  settings = moduleSettings
    ++ Seq(libraryDependencies ++= Dependencies.coreDeps)
).dependsOn(core).settings(name := "hydra-ingest")

lazy val kafka = Project(
  id = "kafka",
  base = file("kafka"),
  settings = moduleSettings
    ++ Seq(libraryDependencies ++= Dependencies.kafkaDeps)
).dependsOn(core).settings(name := "hydra-kafka")


lazy val sandbox = Project(
  id = "sandbox",
  base = file("sandbox"),
  settings = defaultSettings ++ Test.testSettings ++ noPublishSettings ++ restartSettings
    ++ Seq(libraryDependencies ++= Dependencies.kafkaDeps)
).dependsOn(ingest, kafka).settings(name := "hydra-examples")