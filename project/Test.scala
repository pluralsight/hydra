import sbt.Keys._
import sbt._
import scoverage.ScoverageKeys._

object Test {

  // Create a default Scala style task to run with tests
  lazy val testScalastyle = taskKey[Unit]("testScalastyle")

  lazy val testSettings = Seq(
    // Don't run the tests in parallel
    parallelExecution in sbt.Test := false,
    logBuffered in sbt.Test := false,
    coverageExcludedPackages := "hydra\\.ingest\\.HydraIngestApp.*",
    // Don't package test jars since it does not handle resources properly
    exportJars in sbt.Test := false,
    coverageEnabled in sbt.Test := true,
    // Include the code coverage settings
    coverageExcludedPackages := "<empty>;akka.contrib.*",
    coverageMinimum := 70,
    coverageFailOnMinimum := true,
    coverageHighlighting := {
      true
    }
  )
}
