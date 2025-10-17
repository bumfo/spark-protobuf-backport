package org.apache.spark.sql.protobuf.backport.jmh

import benchmark.BinaryTreeBenchmarkProtos.BinaryTreeDocument
import benchmark.BinaryTreeTestDataGenerator
import fastproto.{InlineParserConfig, InlineParserToRowGenerator, StreamWireParser}
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * JMH benchmark for full binary tree protobuf conversion with pruned schema.
 *
 * Tests uniform recursive structure where every node has the same pattern:
 * - Non-leaf nodes: have left and right children
 * - Leaf nodes: no children
 *
 * This controlled structure allows us to verify JIT dispatch assumptions:
 * - Canonical depth=0: Maximum class reuse (megamorphic call sites)
 * - Canonical depth=1: Balanced approach (monomorphic for immediate children)
 * - Canonical depth=∞: Maximum specialization (monomorphic for all levels)
 *
 * Configurable Parameters:
 * - accessDepth: How deep to access (1-8, default: 3)
 *   - 1 = root.left.value
 *   - 2 = root.left.left.value
 *   - 3 = root.left.left.left.value (default)
 *   - etc.
 * - payloadSize: Size of binary payload per node in bytes (default: 0 = no payload)
 *   - Tests impact of variable-length fields on canonical depth optimization
 *   - 0 = no payload, 100 = 100 bytes, 1000 = 1KB, etc.
 * - canonicalDepth: Canonical key depth for parser generation (default: 1)
 *   - 0 = maximum class reuse, 1 = balanced, 100 = deep specialization
 *
 * Usage:
 * sbt jmhBinaryTree
 * sbt "jmhBinaryTree -p accessDepth=3"
 * sbt "jmhBinaryTree -p accessDepth=1,2,3,4,5"
 * sbt "jmhBinaryTree -p payloadSize=0,100,1000"
 * sbt "jmhBinaryTree -p accessDepth=4 -p payloadSize=100"
 * sbt "jmhBinaryTree -p canonicalDepth=0,1,100"
 *
 * Quick canonical depth comparison test:
 * sbt clean "jmhBinaryTree -p accessDepth=2,4,6 -p payloadSize=10000 -p canonicalDepth=0,1,100"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class BinaryTreeProtoBenchmark {

  // JMH parameters
  @Param(Array("3"))
  var accessDepth: Int = _

  @Param(Array("0"))
  var payloadSize: Int = _

  @Param(Array("1"))
  var canonicalDepth: Int = _

  // Test data
  var binaryTreeBinary: Array[Byte] = _ // depth=8

  // Descriptors and schemas
  var treeDescriptor: com.google.protobuf.Descriptors.Descriptor = _
  var treePrunedSchema: StructType = _

  // Parsers
  var treePrunedInlineParser: StreamWireParser = _

  @Setup
  def setup(): Unit = {
    // Create deep binary tree for testing pruned access
    val tree = BinaryTreeTestDataGenerator.createBinaryTree(maxDepth = 8, payloadSize = payloadSize)
    binaryTreeBinary = tree.toByteArray
    treeDescriptor = tree.getDescriptorForType

    // Create pruned schema based on accessDepth parameter
    treePrunedSchema = createPrunedSchema(accessDepth)

    // Initialize parser with pruned schema and canonical depth config
    val config = InlineParserConfig(canonicalKeyDepth = canonicalDepth)
    treePrunedInlineParser = InlineParserToRowGenerator.generateParser(treeDescriptor, treePrunedSchema, config)

    // Print setup info
    println(s"BinaryTree Benchmark Setup:")
    println(s"  Access depth: $accessDepth")
    println(s"  Payload size: $payloadSize bytes" + (if (payloadSize == 0) " (no payload)" else ""))
    println(s"  Canonical depth: $canonicalDepth")
    println(s"  Binary size: ${binaryTreeBinary.length} bytes")
    println(s"  Schema: ${formatSchema(treePrunedSchema)}")
  }

  /**
   * Create a pruned schema that accesses value field at specified depth.
   *
   * Examples:
   * - depth=1: root.left.value
   * - depth=2: root.left.left.value
   * - depth=3: root.left.left.left.value
   */
  private def createPrunedSchema(depth: Int): StructType = {
    require(depth >= 1 && depth <= 8, s"accessDepth must be 1-8, got $depth")

    // Leaf level: just the value field
    val leafSchema = StructType(Seq(
      StructField("value", IntegerType, nullable = false)
    ))

    // Build nested structure from leaf up to desired depth
    // We access via left child only for simplicity
    var currentSchema = leafSchema
    for (_ <- 1 until depth) {
      currentSchema = StructType(Seq(
        StructField("left", currentSchema, nullable = true)
      ))
    }

    // Wrap in root field
    StructType(Seq(
      StructField("root", currentSchema, nullable = true)
    ))
  }

  private def formatSchema(schema: StructType): String = {
    def formatFields(s: StructType, prefix: String = ""): String = {
      s.fields.map { f =>
        f.dataType match {
          case st: StructType => s"$prefix${f.name}.${formatFields(st, "")}"
          case _ => s"$prefix${f.name}"
        }
      }.mkString(", ")
    }
    formatFields(schema)
  }

  // === Pruned Schema Benchmark ===

  @Benchmark
  def inlineParser(bh: Blackhole): Unit = {
    bh.consume(treePrunedInlineParser.parse(binaryTreeBinary))
  }

  // === Baseline: Protobuf-only parsing (no Spark conversion) ===

  // @Benchmark
  def protoParsing(bh: Blackhole): Unit = {
    bh.consume(BinaryTreeDocument.parseFrom(binaryTreeBinary))
  }
}
