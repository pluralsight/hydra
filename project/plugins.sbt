logLevel := Level.Warn

resolvers += Classpaths.sbtPluginReleases
resolvers += Resolver.bintrayRepo("kamon-io", "sbt-plugins")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.2.2-RC2")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.5.0")
addSbtPlugin("au.com.onegeek" %% "sbt-dotenv" % "1.2.88")
addSbtPlugin("com.lightbend.sbt" % "sbt-javaagent" % "0.1.4")
addSbtPlugin("io.kamon" % "sbt-aspectj-runner" % "1.1.0")
//addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.0")
