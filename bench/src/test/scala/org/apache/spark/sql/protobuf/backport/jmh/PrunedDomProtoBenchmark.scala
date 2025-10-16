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
 *   - 1 = root.tag_name (shallow)
 *   - 2 = root.children[].tag_name
 *   - 3 = root.children[].children[].tag_name
 *   - 4 = root.children[].children[].children[].tag_name (default)
 *   - 5+ = even deeper nesting
 *
 * Usage:
 * sbt jmhDomPruned
 * sbt "jmhDomPruned -p accessDepth=4"
 * sbt "jmhDomPruned -p accessDepth=1,2,3,4,5"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class PrunedDomProtoBenchmark {

  // JMH parameter: depth of field access (1-8)
  @Param(Array("4"))
  var accessDepth: Int = _

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

    // Create pruned schema based on accessDepth parameter
    domPrunedSchema = createPrunedSchema(accessDepth)

    // Initialize parsers with pruned schema
    domPrunedWireParser = new WireFormatParser(domDescriptor, domPrunedSchema)
    domPrunedInlineParser = InlineParserToRowGenerator.generateParser(domDescriptor, domPrunedSchema)
  }

  /**
   * Create a pruned schema that accesses tag_name at the specified depth.
   *
   * Examples:
   * - depth=1: root.tag_name
   * - depth=2: root.children[].tag_name
   * - depth=3: root.children[].children[].tag_name
   * - depth=4: root.children[].children[].children[].tag_name
   */
  private def createPrunedSchema(depth: Int): StructType = {
    require(depth >= 1 && depth <= 8, s"accessDepth must be 1-8, got $depth")

    // Leaf level: just tag_name
    val leafSchema = StructType(Seq(
      StructField("tag_name", StringType, nullable = false)
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
  def prunedInlineParser(bh: Blackhole): Unit = {
    bh.consume(domPrunedInlineParser.parse(deepDomBinary))
  }

  @Benchmark
  def prunedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domPrunedWireParser.parse(deepDomBinary))
  }

  // === Baseline: Protobuf-only parsing (no Spark conversion) ===

  @Benchmark
  def protoParsing(bh: Blackhole): Unit = {
    bh.consume(DomDocument.parseFrom(deepDomBinary))
  }
}
