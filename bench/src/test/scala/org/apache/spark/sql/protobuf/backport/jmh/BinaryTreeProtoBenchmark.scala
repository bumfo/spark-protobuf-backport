package org.apache.spark.sql.protobuf.backport.jmh

import benchmark.BinaryTreeBenchmarkProtos.BinaryTreeDocument
import benchmark.BinaryTreeTestDataGenerator
import fastproto.{InlineParserToRowGenerator, StreamWireParser}
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
 * - Canonical depth=0: Maximum class reuse
 * - Canonical depth=1: Balanced approach
 * - Canonical depth=∞: Maximum specialization
 *
 * Configurable Parameters:
 * - accessDepth: How deep to access (1-8)
 *   - 1 = root.left.value
 *   - 2 = root.left.left.value
 *   - 3 = root.left.left.left.value (default)
 *   - etc.
 *
 * Usage:
 * sbt jmhBinaryTree
 * sbt "jmhBinaryTree -p accessDepth=3"
 * sbt "jmhBinaryTree -p accessDepth=1,2,3,4,5"
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
    val tree = BinaryTreeTestDataGenerator.createBinaryTree(maxDepth = 8)
    binaryTreeBinary = tree.toByteArray
    treeDescriptor = tree.getDescriptorForType

    // Create pruned schema based on accessDepth parameter
    treePrunedSchema = createPrunedSchema(accessDepth)

    // Initialize parser with pruned schema
    treePrunedInlineParser = InlineParserToRowGenerator.generateParser(treeDescriptor, treePrunedSchema)

    // Print setup info
    println(s"BinaryTree Benchmark Setup:")
    println(s"  Access depth: $accessDepth")
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
