// Backport of Spark 3.4 protobuf connector to Spark 3.2.1

// See README.md for details.  This project targets Scala 2.12 and Spark 3.2.x.

ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / version          := "0.1.0-SNAPSHOT"

// Provide Spark as a provided dependency – users must supply their own
// `spark-core` and `spark-sql` jars at runtime.  Note that the
// protobuf runtime is shaded via sbt‑assembly; see assembly.sbt.
lazy val sparkVersion         = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  // Bring in the same protobuf runtime version as used by Spark 3.2.x
  "com.google.protobuf" % "protobuf-java" % "3.11.4",
  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// Enable sbt-assembly for creating a single fat jar.  See
// Bring in the sbt-assembly plugin settings at the root level.  The
// shading rules and merge strategies are defined here rather than
// in project/assembly.sbt to avoid build‑loading issues.  See
// project/plugins.sbt for enabling the plugin.
import sbtassembly.AssemblyPlugin.autoImport._

lazy val root = (project in file(".")).settings(
  name := "spark-protobuf-backport",
  publishArtifact := false,
  // Add JVM options for test execution to handle Java module access
  Test / javaOptions ++= Seq(
    "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED"
  ),
  Test / fork := true,
  // Merge strategy discards META‑INF files to avoid duplicate resource issues
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", _ @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  // Shade all Protobuf classes to avoid version conflicts with Spark's own protobuf dependency
  assemblyShadeRules ++= Seq(
    ShadeRule.rename("com.google.protobuf.**" -> "org.sparkproject.spark.protobuf311.@1").inAll
  ),
  // Skip running tests during assembly to avoid dependency conflicts
  assembly / test := {}
)