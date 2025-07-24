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
  "com.google.protobuf" % "protobuf-java" % "3.11.4"
)

// Enable sbt-assembly for creating a single fat jar.  See
// project/plugins.sbt and project/assembly.sbt for details.
lazy val root = (project in file(".")).settings(
  name := "spark-protobuf-backport",
  // Use assembly/skip in tests to avoid packaging test code
  test := {},
  publishArtifact := false
)