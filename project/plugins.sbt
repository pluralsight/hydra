logLevel := Level.Warn

addSbtPlugin("io.spray" % "sbt-revolver" % "0.8.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.1.0-RC1")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.6.1")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.1.0")

// Add the release plugin
addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.8.5")

// Add the Scalastyle plugin for reviewing code
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.7.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")
