import sbt.Keys._
import sbt._
import scoverage.ScoverageKeys._

object Test {

  // Create a default Scala style task to run with tests
  lazy val testScalastyle = taskKey[Unit]("testScalastyle")

  lazy val testSettings = Seq(
    // Don't run the tests in parallel
    (sbt.Test / parallelExecution) := false,
    (sbt.Test / logBuffered) := false,
    coverageExcludedPackages := "hydra\\.ingest\\.HydraIngestApp.*",
    // Don't package test jars since it does not handle resources properly
    (sbt.Test / exportJars) := false,
    (sbt.Test / coverageEnabled) := true,
    // Include the code coverage settings
    coverageExcludedPackages := "<empty>;akka.contrib.*",
    coverageMinimum := 70,
    coverageFailOnMinimum := true,
    coverageHighlighting := {
      true
    }
  )
}
