import sbt.Resolver

val JDK = "1.8"

val buildNumber =
  scala.util.Properties.envOrNone("version").map(v => "." + v).getOrElse("")
val hydraVersion = "0.11.3" + buildNumber
val jvmMaxMemoryFlag = sys.env.getOrElse("MAX_JVM_MEMORY_FLAG", "-Xmx2g")

lazy val defaultSettings = Seq(
  organization := "pluralsight",
  version := hydraVersion,
  scalaVersion := "2.12.13",
  description := "Hydra",
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  excludeDependencies += "org.slf4j" % "slf4j-log4j12",
  excludeDependencies += "log4j" % "log4j",
  addCompilerPlugin(
    "org.typelevel" %% "kind-projector" % "0.11.2" cross CrossVersion.full
  ),
  packageOptions in (Compile, packageBin) +=
    Package.ManifestAttributes("Implementation-Build" -> buildNumber),
  logLevel := Level.Info,
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:_",
    "-deprecation",
    "-unchecked",
    "-Ypartial-unification"
  ),
  javacOptions in Compile ++= Seq(
    "-encoding",
    "UTF-8",
    "-source",
    JDK,
    "-target",
    JDK,
    "-Xlint:unchecked",
    "-Xlint:deprecation",
    "-Xlint:-options"
  ),
  resolvers += Resolver.mavenLocal,
  resolvers += "Scalaz Bintray Repo" at "https://dl.bintray.com/scalaz/releases",
  resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
  resolvers += "jitpack" at "https://jitpack.io",
  resolvers += Resolver.bintrayRepo("hseeberger", "maven"),
  ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
  parallelExecution in sbt.Test := false,
  javaOptions in Universal ++= Seq(
    "-Dorg.aspectj.tracing.factory=default",
    "-J" + jvmMaxMemoryFlag
  )
)

lazy val restartSettings = Seq(
  javaOptions in reStart += jvmMaxMemoryFlag
)

val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  // required until these tickets are closed https://github.com/sbt/sbt-pgp/issues/42,
  // https://github.com/sbt/sbt-pgp/issues/36
  publishTo := Some(
    Resolver.file("Unused transient repository", file("target/unusedrepo"))
  ),
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

lazy val moduleSettings =
  defaultSettings ++ Test.testSettings //++ Publish.settings

lazy val root = Project(
  id = "hydra",
  base = file(".")
)
  .settings(moduleSettings)
  .aggregate(common, core, avro, ingest, kafka)

lazy val common = Project(
  id = "common",
  base = file("common")
).settings(
  moduleSettings,
  name := "hydra-common",
  libraryDependencies ++= Dependencies.baseDeps
)

lazy val core = Project(
  id = "core",
  base = file("core")
).dependsOn(avro)
  .settings(
    moduleSettings,
    name := "hydra-core",
    libraryDependencies ++= Dependencies.coreDeps
  )

lazy val kafka = Project(
  id = "kafka",
  base = file("ingestors/kafka")
).dependsOn(core)
  .configs(IntegrationTest)
  .settings(
    moduleSettings ++ Defaults.itSettings,
    name := "hydra-kafka",
    libraryDependencies ++= Dependencies.kafkaDeps
  )

lazy val avro = Project(
  id = "avro",
  base = file("avro")
).dependsOn(common)
  .settings(
    moduleSettings,
    name := "hydra-avro",
    libraryDependencies ++= Dependencies.avroDeps
  )

val sbSettings =
  defaultSettings ++ Test.testSettings ++ noPublishSettings ++ restartSettings

lazy val ingest = Project(
  id = "ingest",
  base = file("ingest")
)
  .dependsOn(core, kafka)
  .settings(
    moduleSettings ++ dockerSettings,
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "hydra.ingest.bootstrap",
    buildInfoOptions += BuildInfoOption.ToJson,
    buildInfoOptions += BuildInfoOption.BuildTime,
    javaAgents += "org.aspectj" % "aspectjweaver" % "1.8.14",
    name := "hydra-ingest",
    libraryDependencies ++= Dependencies.ingestDeps
  )
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, JavaAgent, sbtdocker.DockerPlugin)

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

    new Dockerfile {
      from("java")
      maintainer("Alex Silva <alex-silva@pluralsight.com>")
      user("root")
      env("JAVA_OPTS", "-Xmx2G")
      runRaw("mkdir -p /etc/hydra")
      run("mkdir", "-p", "/var/log/hydra")
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
