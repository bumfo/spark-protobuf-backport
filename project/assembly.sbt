// Settings for sbt-assembly that produce a shaded jar with relocated
// protobuf classes.  This prevents conflicts with Spark's own protobuf
// dependency when running on Spark 3.2.1.

import sbtassembly.AssemblyPlugin.autoImport._

// Produce a single, shaded jar; omit tests
assembly / test := {}
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Relocate Protobuf runtime to avoid classpath collisions.  All
// classes under com.google.protobuf.* will be moved to
// org.sparkproject.spark.protobuf311.*.  Note that this must match
// what Spark itself does with its protobuf dependency.
assemblyShadeRules ++= Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "org.sparkproject.spark.protobuf311.@1").inAll
)