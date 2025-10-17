package org.apache.spark.sql.protobuf.backport.jmh

import benchmark.MultiwayTreeTestDataGenerator
import fastproto.{InlineParserConfig, InlineParserToRowGenerator, StreamWireParser}
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * Benchmark for multiway tree parsing with configurable depth and branching factor.
 *
 * Tests parser performance on tree structures with different shapes:
 * - depth: How deep the tree is (default: 5)
 * - branchingFactor: Number of children per internal node (default: 4)
 * - accessDepth: How deep to access in the pruned schema (default: same as depth)
 * - accessPath: Which child pointer to follow - "left" (singular) or "right" (repeated array)
 * - canonicalDepth: Canonical key depth for parser generation (default: 1)
 *
 * Access patterns:
 * - accessPath=left: root.left.left...left.value (singular nested fields)
 * - accessPath=right: root.right[].right[]...right[].value (repeated nested fields)
 *
 * Run with:
 *   sbt "jmhMultiwayTree -p depth=5 -p branchingFactor=4 -p accessDepth=5"
 *   sbt "jmhMultiwayTree -p canonicalDepth=0,1,100"
 *   sbt "jmhMultiwayTree -p accessPath=left,right"
 *
 * Quick canonical depth comparison test:
 *   sbt clean "jmhMultiwayTree -p depth=5 -p branchingFactor=4 -p accessDepth=5 -p canonicalDepth=0,1,100"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class MultiwayTreeProtoBenchmark {

  /**
   * Tree depth (number of levels).
   * Default: 5 levels
   */
  @Param(Array("5"))
  var depth: Int = _

  /**
   * Branching factor (number of children per internal node).
   * Default: 4 children per node
   */
  @Param(Array("4"))
  var branchingFactor: Int = _

  /**
   * Access depth for pruned schema (how deep to access along chosen path).
   * Default: same as tree depth
   */
  @Param(Array("5"))
  var accessDepth: Int = _

  /**
   * Which child pointer to follow in pruned schema.
   * - "left": singular nested field (root.left.left...left.value)
   * - "right": repeated nested field (root.right[].right[]...right[].value)
   * Default: "left"
   */
  @Param(Array("left"))
  var accessPath: String = _

  /**
   * Canonical key depth for parser generation.
   * Default: 1 (balanced approach)
   */
  @Param(Array("1"))
  var canonicalDepth: Int = _

  var treeData: Array[Byte] = _
  var treeDescriptor: com.google.protobuf.Descriptors.Descriptor = _
  var fullSchemaParser: StreamWireParser = _
  var prunedSchemaParser: StreamWireParser = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    // Generate tree
    val tree = MultiwayTreeTestDataGenerator.generateTree(depth, branchingFactor)
    treeData = tree.toByteArray
    treeDescriptor = tree.getDescriptorForType

    val nodeCount = MultiwayTreeTestDataGenerator.calculateNodeCount(depth, branchingFactor)
    println(s"MultiwayTree Benchmark Setup:")
    println(s"  Tree: depth=$depth, branching=$branchingFactor, nodes=$nodeCount, bytes=${treeData.length}")
    println(s"  Access depth: $accessDepth")
    println(s"  Access path: $accessPath")
    println(s"  Canonical depth: $canonicalDepth")

    // Create config with canonical depth
    val config = InlineParserConfig(canonicalKeyDepth = canonicalDepth)

    // Full schema
    val fullSchema = buildFullSchema(depth)
    fullSchemaParser = InlineParserToRowGenerator.generateParser(treeDescriptor, fullSchema, config)

    // Pruned schema - access via specified path
    val prunedSchema = buildPrunedSchema(accessDepth, accessPath)
    prunedSchemaParser = InlineParserToRowGenerator.generateParser(treeDescriptor, prunedSchema, config)

    println(s"  Full schema: ${formatSchema(fullSchema)}")
    println(s"  Pruned schema: ${formatSchema(prunedSchema)}")
  }

  /**
   * Build full schema for a tree of given depth.
   */
  private def buildFullSchema(remainingDepth: Int): StructType = {
    if (remainingDepth <= 1) {
      // Leaf node
      StructType(Seq(
        StructField("value", IntegerType, nullable = false),
        StructField("payload", StringType, nullable = false)
      ))
    } else {
      // Internal node with left and right children
      val childSchema = buildFullSchema(remainingDepth - 1)
      StructType(Seq(
        StructField("value", IntegerType, nullable = false),
        StructField("payload", StringType, nullable = false),
        StructField("left", childSchema, nullable = true),
        StructField("right", ArrayType(childSchema, containsNull = true), nullable = true)
      ))
    }
  }

  /**
   * Build pruned schema accessing only the specified path to specified depth.
   * Access patterns:
   * - "left": left.left...left.value (singular nested fields)
   * - "right": right[].right[]...right[].value (repeated nested fields)
   */
  private def buildPrunedSchema(remainingDepth: Int, path: String): StructType = {
    if (remainingDepth <= 1) {
      // Access just value at this level
      StructType(Seq(
        StructField("value", IntegerType, nullable = false)
      ))
    } else {
      // Access child via specified path
      val childSchema = buildPrunedSchema(remainingDepth - 1, path)
      path match {
        case "left" =>
          // Singular nested field
          StructType(Seq(
            StructField("left", childSchema, nullable = true)
          ))
        case "right" =>
          // Repeated nested field (array)
          StructType(Seq(
            StructField("right", ArrayType(childSchema, containsNull = true), nullable = true)
          ))
        case other =>
          throw new IllegalArgumentException(s"Invalid accessPath: $other. Must be 'left' or 'right'")
      }
    }
  }

  /**
   * Format schema for display.
   */
  private def formatSchema(schema: StructType): String = {
    def formatFields(s: StructType, prefix: String = ""): String = {
      s.fields.map { f =>
        f.dataType match {
          case st: StructType => s"$prefix${f.name}.${formatFields(st, "")}"
          case ArrayType(st: StructType, _) => s"$prefix${f.name}[].${formatFields(st, "")}"
          case _ => s"$prefix${f.name}"
        }
      }.mkString(", ")
    }
    formatFields(schema)
  }

  @Benchmark
  def inlineParser(bh: Blackhole): Unit = {
    bh.consume(prunedSchemaParser.parse(treeData))
  }

  // @Benchmark
  def inlineParserFullSchema(bh: Blackhole): Unit = {
    bh.consume(fullSchemaParser.parse(treeData))
  }
}
