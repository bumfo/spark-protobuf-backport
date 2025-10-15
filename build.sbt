// Backport of Spark 3.4 protobuf connector to Spark 3.2.1

// See README.md for details.  This project targets Scala 2.12 and Spark 3.2.x.

ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "0.1.0-SNAPSHOT"

// Provide Spark as a provided dependency – users must supply their own
// `spark-core` and `spark-sql` jars at runtime.  Note that the
// protobuf runtime is shaded via sbt‑assembly; see assembly.sbt.
lazy val sparkVersion = "3.2.1"
lazy val protobufVersion = "3.21.7"

// Task keys
lazy val generateParsers = taskKey[Unit]("Generate parser code from protobuf files")
lazy val generateWireFormat = taskKey[Unit]("Generate WireFormat parser code from protobuf schemas")

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
    // Simple task to run the parser code generator
    generateParsers := {
      println("Running parser code generation...")
      (Test / runMain).toTask(" fastproto.GenerateParsersTask").value
    },
    // Task to run the WireFormat code generator
    generateWireFormat := {
      println("Running WireFormat parser code generation...")
      (Test / runMain).toTask(" fastproto.GenerateWireFormatTask").value
    },
    libraryDependencies ++= commonDependencies ++ Seq(
      // Protobuf as provided for compilation
      "com.google.protobuf" % "protobuf-java" % protobufVersion % Provided
    ),
    // Compile / scalacOptions ++= Seq(
    //   "-opt:l:inline",
    //   "-opt-inline-from:fastproto**",
    // )
  )

// Dedicated benchmark project
lazy val bench = project
  .dependsOn(core, core % "test->test")
  .enablePlugins(JmhPlugin)
  .settings(commonSettings)
  .settings(
    name := "spark-protobuf-backport-bench",
    publish / skip := true,
    // JMH benchmark configuration
    Jmh / sourceDirectory := (Test / sourceDirectory).value,
    Jmh / classDirectory := (Test / classDirectory).value,
    Jmh / dependencyClasspath := (Test / dependencyClasspath).value,
    // Compile JMH benchmarks with test classpath
    Jmh / compile := (Jmh / compile).dependsOn(Test / compile).value,
    libraryDependencies ++= commonDependencies ++ Seq(
      // Add full Spark and protobuf dependencies for benchmarking
      "org.apache.spark" %% "spark-sql" % sparkVersion % "jmh,test",
      "org.apache.spark" %% "spark-core" % sparkVersion % "jmh,test",
      "com.google.protobuf" % "protobuf-java" % protobufVersion
    ),
    // Protocol buffers compilation for Java sources
    Compile / PB.targets := Seq(
      PB.gens.java -> (Compile / sourceManaged).value
    ),
    // Make sure protobuf compilation happens before regular compilation
    Compile / compile := (Compile / compile).dependsOn(Compile / PB.generate).value
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
      case PathList("META-INF", _@_*) => MergeStrategy.discard
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

// Dedicated testing project with comprehensive protobuf feature coverage
lazy val tests = project
  .dependsOn(core, core % "test->test")
  .settings(commonSettings)
  .settings(
    name := "spark-protobuf-backport-tests",
    publish / skip := true,

    // Protocol buffers compilation (same as bench module)
    Compile / PB.targets := Seq(
      PB.gens.java -> (Compile / sourceManaged).value
    ),
    // Make sure protobuf compilation happens before regular compilation
    Compile / compile := (Compile / compile).dependsOn(Compile / PB.generate).value,

    // Additional JVM options for Java module system access (Spark serialization needs these)
    Test / javaOptions ++= Seq(
      "--add-exports", "java.base/sun.security.action=ALL-UNNAMED"
    ),

    // Test tier configuration - exclude property tests by default (ScalaCheck Properties)
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow"),

    libraryDependencies ++= commonDependencies ++ Seq(
      "com.google.protobuf" % "protobuf-java" % protobufVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion % "test",
      "org.apache.spark" %% "spark-core" % sparkVersion % "test",
      "org.scalacheck" %% "scalacheck" % "1.17.0" % Test,
      "org.scalatestplus" %% "scalacheck-1-17" % "3.2.17.0" % Test
    )
  )

// Test execution tasks
lazy val unitTests = taskKey[Unit]("Run Tier 1 unit tests (<5s)")
unitTests := {
  (tests / Test / testOnly).toTask(" unit.*").value
}

lazy val propertyTests = taskKey[Unit]("Run Tier 2 property tests (<30s)")
propertyTests := {
  (tests / Test / testOnly).toTask(" properties.* -- -n Property").value
}

lazy val integrationTests = taskKey[Unit]("Run Tier 3 integration tests (<60s)")
integrationTests := {
  (tests / Test / testOnly).toTask(" integration.*").value
}

lazy val allTestTiers = taskKey[Unit]("Run all test tiers sequentially")
allTestTiers := Def.sequential(
  unitTests,
  propertyTests,
  integrationTests
).value

lazy val showGeneratedCode = inputKey[Unit]("Show generated InlineParser code (usage: showGeneratedCode [MessageName])")
showGeneratedCode := Def.inputTaskDyn {
  val args = Def.spaceDelimited("<message>").parsed
  val messageArg = if (args.nonEmpty) s" ${args.head}" else ""
  Def.taskDyn {
    (tests / Test / runMain).toTask(s" ShowGeneratedParser$messageArg")
  }
}.evaluated

lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(core, bench, tests, uberJar, shaded)
  .settings(
    name := "spark-protobuf",
    publish / skip := true,
    sourcesInBase := false
  )

// Command aliases for convenience
addCommandAlias("jmh", "bench/Test/compile; bench/Jmh/run")
addCommandAlias("jmhQuick", "bench/Test/compile; bench/Jmh/run -wi 2 -i 3 -f 1")
addCommandAlias("jmhSimple", "bench/Test/compile; bench/Jmh/run .*ProtobufConversionJmhBenchmarkSimple.*")
addCommandAlias("jmhComplex", "bench/Test/compile; bench/Jmh/run .*ProtobufConversionJmhBenchmarkComplex.*")
addCommandAlias("jmhDom", "bench/Test/compile; bench/Jmh/run .*ProtobufConversionJmhBenchmarkDom.*")
addCommandAlias("jmhArray", "bench/Test/compile; bench/Jmh/run .*ArrayWriterBenchmark.*")
