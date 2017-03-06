import sbt.Keys.version
import sbt._

enablePlugins(JavaAppPackaging)

val JDK = "1.8"
val buildNumber = sys.env.get("BUILD_NUMBER").getOrElse("000")


lazy val defaultSettings = Seq(
  organization := "pluralsight",
  version := "0.6.5",
  scalaVersion := "2.11.8",
  description := "Hydra",
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
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

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  // required until these tickets are closed https://github.com/sbt/sbt-pgp/issues/42,
  // https://github.com/sbt/sbt-pgp/issues/36
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))
)

lazy val moduleSettings = defaultSettings ++ Test.testSettings ++ Publish.settings

lazy val root = Project(
  id = "hydra",
  base = file("."),
  settings = noPublishSettings ++ defaultSettings
    ++ Seq(libraryDependencies ++= Dependencies.coreDeps)
).aggregate(common, core, kafka, ingest)

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
).dependsOn(core, kafka).settings(name := "hydra-ingest")

lazy val kafka = Project(
  id = "kafka",
  base = file("kafka"),
  settings = moduleSettings
    ++ Seq(libraryDependencies ++= Dependencies.transportDeps)
).dependsOn(core).settings(name := "hydra-kafka")