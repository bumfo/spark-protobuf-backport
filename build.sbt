// Backport of Spark 3.4 protobuf connector to Spark 3.2.1

// See README.md for details.  This project targets Scala 2.12 and Spark 3.2.x.

ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / version          := "0.1.0-SNAPSHOT"

// Provide Spark as a provided dependency – users must supply their own
// `spark-core` and `spark-sql` jars at runtime.  Note that the
// protobuf runtime is shaded via sbt‑assembly; see assembly.sbt.
lazy val sparkVersion         = "3.2.1"
lazy val protobufVersion      = "3.21.7"

// Common dependencies for both projects
lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// Enable sbt-assembly for creating a single fat jar.  See
// Bring in the sbt-assembly plugin settings at the root level.  The
// shading rules and merge strategies are defined here rather than
// in project/assembly.sbt to avoid build‑loading issues.  See
// project/plugins.sbt for enabling the plugin.
import sbtassembly.AssemblyPlugin.autoImport._

// Base project with common settings
lazy val commonSettings = Seq(
  scalaVersion := "2.12.15",
  version := "0.1.0-SNAPSHOT",
  // Add JVM options for test execution to handle Java module access
  Test / javaOptions ++= Seq(
    "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED"
  ),
  Test / fork := true
)

// Root project - core with source code, protobuf provided
lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    name := "spark-protobuf-backport",
    publishArtifact := true,
    libraryDependencies ++= commonDependencies ++ Seq(
      // Protobuf as provided for compilation
      "com.google.protobuf" % "protobuf-java" % protobufVersion % Provided
    )
  )

// Shaded project - protobuf compiled and shaded
lazy val shaded = (project in file("shaded"))
  .dependsOn(root)
  .settings(commonSettings)
  .settings(
    name := "spark-protobuf-backport-shaded", 
    publishArtifact := true,
    libraryDependencies ++= commonDependencies ++ Seq(
      // Protobuf as compile dependency - will be shaded and included
      "com.google.protobuf" % "protobuf-java" % protobufVersion
    ),
    
    // Assembly settings for shaded project only
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assembly / test := {},
    
    // Shade protobuf to match Spark's expected shading pattern
    assemblyShadeRules ++= Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "org.sparkproject.spark_protobuf.protobuf.@1").inAll
    ),
    
    assembly / assemblyJarName := "spark-protobuf-backport-shaded-" + version.value + ".jar"
  )