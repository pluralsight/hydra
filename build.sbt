import sbt.Resolver

val JDK = "1.8"
val buildNumber = scala.util.Properties.envOrNone("version").map(v => "." + v).getOrElse("")
val hydraVersion = "0.10.0" + buildNumber

lazy val defaultSettings = Seq(
  organization := "pluralsight",
  version := hydraVersion,
  scalaVersion := "2.12.2",
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
  resolvers += Resolver.bintrayRepo("hseeberger", "maven"),
  ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet
)

lazy val restartSettings = Seq(
  javaOptions in reStart += "-Xmx2g",
  mainClass in reStart := Some("hydra.sandbox.app.HydraSandbox")
)

val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  // required until these tickets are closed https://github.com/sbt/sbt-pgp/issues/42,
  // https://github.com/sbt/sbt-pgp/issues/36
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
  packageBin := {
    new File("")
  },
  packageSrc := {
    new File("")
  },
  packageDoc := {
    new File("")
  }
)

lazy val moduleSettings = defaultSettings ++ Test.testSettings //++ Publish.settings

lazy val root = Project(
  id = "hydra",
  base = file(".")
).settings(defaultSettings).aggregate(common, core, avro, ingest, kafka, sql, jdbc, sandbox)

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
  .settings(moduleSettings, name := "hydra-ingest", libraryDependencies ++= Dependencies.ingestDeps)

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
).dependsOn(ingest, jdbc, rabbitmq)
  .settings(sbSettings, name := "hydra-examples", libraryDependencies ++= Dependencies.sandboxDeps)

lazy val app = Project(
  id = "app",
  base = file("app")
).dependsOn(ingest, kafka, jdbc, rabbitmq)
  .settings(moduleSettings ++ noPublishSettings ++ dockerSettings, name := "hydra-app",
    libraryDependencies ++= Dependencies.sandboxDeps)
  .enablePlugins(JavaAppPackaging, sbtdocker.DockerPlugin)

//scala style
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := scalastyle.in(sbt.Test).toTask("").value

lazy val dockerSettings = Seq(
  buildOptions in docker := BuildOptions(
    cache = false,
    removeIntermediateContainers = BuildOptions.Remove.Always,
    pullBaseImage = BuildOptions.Pull.IfMissing
  ),
  dockerfile in docker := {
    val appDir: File = stage.value
    val targetDir = "/app"
    val dockerFiles = IO.listFiles(new java.io.File("docker")).find(_.getPath.endsWith(".conf")).toSeq

    new Dockerfile {
      from("java")
      maintainer("Alex Silva <alex-silva@pluralsight.com>")
      user("root")
      env("JAVA_OPTS", "-Xmx2G")
      runRaw("mkdir -p /etc/hydra")
      run("mkdir", "-p", "/var/log/hydra")
      copy(dockerFiles, "/etc/hydra/")
      expose(8088)
      entryPoint(s"$targetDir/bin/${executableScriptName.value}")
      copy(appDir, targetDir)
    }
  },

  imageNames in docker := Seq(
    // Sets the latest tag
    ImageName(s"${organization.value}/hydra:latest"),

    // Sets a name with a tag that contains the project version
    ImageName(
      namespace = Some(organization.value),
      repository = "hydra",
      tag = Some(version.value)
    )
  )
)
