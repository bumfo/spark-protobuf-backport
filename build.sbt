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
lazy val core = project
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(
    name := "spark-protobuf-backport",
    publishArtifact := true,
    // Exclude benchmark tests from regular test runs only
    Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, "-l", "benchmark.Benchmark")),
    // Add a benchmark task that bypasses tag filtering
    TaskKey[Unit]("benchmark") := {
      (Test / testOnly).toTask(" benchmark.ProtobufConversionBenchmark").value
    },
    libraryDependencies ++= commonDependencies ++ Seq(
      // Protobuf as provided for compilation
      "com.google.protobuf" % "protobuf-java" % protobufVersion % Provided
    )
  )

// UberJar project - builds the assembly JAR with all dependencies
lazy val uberJar = (project in file("uber"))
  .enablePlugins(AssemblyPlugin)
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "spark-protobuf-backport-uber",
    publish / skip := true,
    libraryDependencies ++= commonDependencies ++ Seq(
      // Protobuf as compile dependency - will be shaded and included
      "com.google.protobuf" % "protobuf-java" % protobufVersion
    ),
    
    // Assembly settings
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assembly / test := {},
    assembly / assemblyJarName := "spark-protobuf-backport-shaded-" + version.value + ".jar",
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),
    
    // Shade protobuf to match Spark's expected shading pattern
    assemblyShadeRules ++= Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "org.sparkproject.spark_protobuf.protobuf.@1").inAll
    )
  )

// Shaded cosmetic project - publishes the assembly JAR as main artifact
lazy val shaded = project
  .disablePlugins(AssemblyPlugin)
  .settings(
    name := "spark-protobuf-backport-shaded",
    // Use the assembly JAR from uberJar project as our main artifact
    Compile / packageBin := (uberJar / assembly).value
  )
  
lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(core, uberJar, shaded)
  .settings(
    publish / skip := true,
  )
