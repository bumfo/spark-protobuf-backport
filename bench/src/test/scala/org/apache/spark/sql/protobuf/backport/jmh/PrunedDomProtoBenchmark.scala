package org.apache.spark.sql.protobuf.backport.jmh

import benchmark.DomBenchmarkProtos.DomDocument
import benchmark.DomTestDataGenerator
import com.google.protobuf.{DescriptorProtos, Descriptors}
import fastproto.{InlineParserToRowGenerator, RecursiveSchemaConverters, StreamWireParser, WireFormatParser}
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * JMH benchmark for DOM tree protobuf conversion with pruned schema.
 *
 * Tests selective field access scenarios where only a deeply nested field is accessed
 * (e.g., root.children[].children[].children[].tag_name). This simulates real-world
 * query patterns where users SELECT specific nested fields rather than entire structures.
 *
 * Configurable Parameters:
 * - accessDepth: How many levels deep to access the field (1-8)
 *   - 1 = root.{field} (shallow)
 *   - 2 = root.children[].{field}
 *   - 3 = root.children[].children[].{field}
 *   - 4 = root.children[].children[].children[].{field} (default)
 *   - 5+ = even deeper nesting
 *
 * - accessField: Which field to access at the leaf level
 *   - tag_name (default): StringType field
 *   - depth: IntegerType field
 *   - node_id: StringType field
 *   - node_type: IntegerType field (enum)
 *
 * Usage:
 * sbt jmhDomPruned
 * sbt "jmhDomPruned -p accessDepth=4"
 * sbt "jmhDomPruned -p accessDepth=1,2,3,4,5"
 * sbt "jmhDomPruned -p accessField=tag_name,depth,node_id"
 * sbt "jmhDomPruned -p accessDepth=2,4 -p accessField=tag_name,depth"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class PrunedDomProtoBenchmark {

  // JMH parameters
  @Param(Array("4"))
  var accessDepth: Int = _

  @Param(Array("depth"))
  var accessField: String = _

  // Test data
  var deepDomBinary: Array[Byte] = _ // depth=8, breadth=4

  // Descriptors and schemas
  var domDescriptor: Descriptors.Descriptor = _
  var domPrunedSchema: StructType = _

  // Parsers
  var domPrunedWireParser: WireFormatParser = _
  var domPrunedInlineParser: StreamWireParser = _

  @Setup
  def setup(): Unit = {
    // Create deep DOM for testing pruned access
    val deepDom = DomTestDataGenerator.createDomDocument(maxDepth = 8, breadthFactor = 4)
    deepDomBinary = deepDom.toByteArray
    domDescriptor = deepDom.getDescriptorForType

    // Create pruned schema based on accessDepth and accessField parameters
    domPrunedSchema = createPrunedSchema(accessDepth, accessField)

    // Initialize parsers with pruned schema
    domPrunedWireParser = new WireFormatParser(domDescriptor, domPrunedSchema)
    domPrunedInlineParser = InlineParserToRowGenerator.generateParser(domDescriptor, domPrunedSchema)
  }

  /**
   * Create a pruned schema that accesses the specified field at the specified depth.
   *
   * Examples:
   * - depth=1, field=tag_name: root.tag_name
   * - depth=2, field=depth: root.children[].depth
   * - depth=3, field=node_id: root.children[].children[].node_id
   * - depth=4, field=tag_name: root.children[].children[].children[].tag_name
   */
  private def createPrunedSchema(depth: Int, fieldName: String): StructType = {
    require(depth >= 1 && depth <= 8, s"accessDepth must be 1-8, got $depth")

    // Get field type based on field name
    val (sparkType, nullable) = fieldName match {
      case "tag_name" => (StringType, false)
      case "depth" => (IntegerType, false)
      case "node_id" => (StringType, false)
      case "node_type" => (IntegerType, false) // enum represented as int
      case other => throw new IllegalArgumentException(
        s"Unsupported accessField: $other. Valid values: tag_name, depth, node_id, node_type")
    }

    // Leaf level: just the specified field
    val leafSchema = StructType(Seq(
      StructField(fieldName, sparkType, nullable = nullable)
    ))

    // Build nested structure from leaf up to desired depth
    var currentSchema = leafSchema
    for (_ <- 1 until depth) {
      currentSchema = StructType(Seq(
        StructField("children", ArrayType(currentSchema), nullable = true)
      ))
    }

    // Wrap in root field
    StructType(Seq(
      StructField("root", currentSchema, nullable = true)
    ))
  }

  // === Pruned Schema Benchmarks ===

  @Benchmark
  def inlineParser(bh: Blackhole): Unit = {
    bh.consume(domPrunedInlineParser.parse(deepDomBinary))
  }

  // @Benchmark
  def wireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domPrunedWireParser.parse(deepDomBinary))
  }

  // === Baseline: Protobuf-only parsing (no Spark conversion) ===

  // @Benchmark
  def protoParsing(bh: Blackhole): Unit = {
    bh.consume(DomDocument.parseFrom(deepDomBinary))
  }
}
