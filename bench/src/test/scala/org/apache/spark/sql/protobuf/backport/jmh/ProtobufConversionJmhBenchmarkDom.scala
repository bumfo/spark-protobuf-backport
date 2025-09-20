package org.apache.spark.sql.protobuf.backport.jmh

import benchmark.{RecursiveSchemaConverters, DomBenchmarkProtos, DomTestDataGenerator}
import benchmark.DomBenchmarkProtos.DomDocument
import com.google.protobuf.{DescriptorProtos, Descriptors}
import fastproto.{Parser, ProtoToRowGenerator, StreamWireParser, WireFormatParser, WireFormatToRowGenerator}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.protobuf.backport.{DynamicMessageParser, ProtobufDataToCatalyst}
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * JMH benchmark for DOM tree protobuf conversion performance.
 *
 * This benchmark uses realistic HTML DOM tree structures to test recursive
 * protobuf parsing performance. The DOM structure provides a meaningful
 * real-world example of recursive data that everyone can understand.
 *
 * Key features tested:
 * - Recursive message parsing (DomNode.children)
 * - Mixed data types (strings, maps, repeated fields, enums)
 * - Variable tree depth and breadth
 * - Schema mocking for Spark SQL compatibility
 *
 * The recursive DomNode.children field is mocked as BinaryType in the Spark schema
 * to avoid infinite type recursion, while the actual protobuf parsing still
 * handles the full recursive structure.
 *
 * Usage:
 * sbt "bench/Jmh/run .*ProtobufConversionJmhBenchmarkDom.*"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class ProtobufConversionJmhBenchmarkDom {

  // Test data with different complexity levels
  var shallowDomBinary: Array[Byte] = _     // depth=3, breadth=2
  var standardDomBinary: Array[Byte] = _    // depth=6, breadth=3
  var deepDomBinary: Array[Byte] = _        // depth=8, breadth=4

  // Descriptors and schemas
  var domDescriptor: Descriptors.Descriptor = _
  var domSparkSchema: StructType = _
  var domDescSet: Array[Byte] = _
  var domTempDescFile: java.io.File = _

  // Parsers for DOM structure (using standard complexity)
  // var domDirectParser: WireFormatParser = _
  var domGeneratedWireParser: StreamWireParser = _
  var domProtoToRowParser: Parser = _  // ProtoToRowGenerator produces parsers that implement Parser
  // var domDynamicParser: DynamicMessageParser = _

  @Setup
  def setup(): Unit = {
    // === Create DOM test data with different complexity levels ===

    // Shallow tree: good for testing breadth
    val shallowDom = DomTestDataGenerator.createDomDocument(maxDepth = 3, breadthFactor = 2)
    shallowDomBinary = shallowDom.toByteArray

    // Standard tree: realistic web page complexity
    val standardDom = DomTestDataGenerator.createDomDocument(maxDepth = 6, breadthFactor = 3)
    standardDomBinary = standardDom.toByteArray

    // Deep tree: stress test for recursion
    val deepDom = DomTestDataGenerator.createDomDocument(maxDepth = 8, breadthFactor = 4)
    deepDomBinary = deepDom.toByteArray

    // Use standard complexity for setup
    domDescriptor = standardDom.getDescriptorForType

    // === Create Spark schema with TRUE recursion ===
    // This creates actual recursive schemas where DomNode.children refers back to DomNode
    // Now works correctly since we pass the schema explicitly to parsers
    domSparkSchema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(domDescriptor)

    println(s"DOM Schema created with ${domSparkSchema.fields.length} fields")
    RecursiveSchemaConverters.printTrueRecursiveSchemaInfo(domDescriptor)

    // === Create FileDescriptorSet ===
    domDescSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(domDescriptor.getFile.toProto)
      .build()
      .toByteArray

    // === Create temporary descriptor file ===
    domTempDescFile = java.io.File.createTempFile("dom_benchmark_descriptor", ".desc")
    java.nio.file.Files.write(domTempDescFile.toPath, domDescSet)
    domTempDescFile.deleteOnExit()

    // === Initialize DOM Parsers ===
    // domDirectParser = new WireFormatParser(domDescriptor, domSparkSchema)
    domGeneratedWireParser = WireFormatToRowGenerator.generateParser(domDescriptor, domSparkSchema)
    domProtoToRowParser = ProtoToRowGenerator.generateParser(domDescriptor, classOf[DomDocument], domSparkSchema)
    // domDynamicParser = new DynamicMessageParser(domDescriptor, domSparkSchema)

    // Print test data statistics
    println(s"Shallow DOM: ${shallowDomBinary.length} bytes")
    println(s"Standard DOM: ${standardDomBinary.length} bytes")
    println(s"Deep DOM: ${deepDomBinary.length} bytes")
  }

  @TearDown
  def teardown(): Unit = {
    if (domTempDescFile != null) domTempDescFile.delete()
  }

  // === DOM Tree Benchmarks (Standard Complexity: depth=6, breadth=3) ===

  // @Benchmark
  def domStandardGeneratedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domGeneratedWireParser.parse(standardDomBinary))
  }

  // @Benchmark
  def domStandardProtoToRowParser(bh: Blackhole): Unit = {
    bh.consume(domProtoToRowParser.parse(standardDomBinary))
  }

  // @Benchmark
  // def domStandardDirectWireFormatParser(bh: Blackhole): Unit = {
  //   bh.consume(domDirectParser.parse(standardDomBinary))
  // }

  // @Benchmark
  // def domStandardDynamicMessageParser(bh: Blackhole): Unit = {
  //   bh.consume(domDynamicParser.parse(standardDomBinary))
  // }

  // === Shallow DOM Benchmarks (depth=3, breadth=2) ===
  // Tests performance with wide but shallow trees

  // @Benchmark
  def domShallowGeneratedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domGeneratedWireParser.parse(shallowDomBinary))
  }

  // @Benchmark
  def domShallowProtoToRowParser(bh: Blackhole): Unit = {
    bh.consume(domProtoToRowParser.parse(shallowDomBinary))
  }

  // @Benchmark
  // def domShallowDirectWireFormatParser(bh: Blackhole): Unit = {
  //   bh.consume(domDirectParser.parse(shallowDomBinary))
  // }

  // @Benchmark
  // def domShallowDynamicMessageParser(bh: Blackhole): Unit = {
  //   bh.consume(domDynamicParser.parse(shallowDomBinary))
  // }

  // === Deep DOM Benchmarks (depth=8, breadth=4) ===
  // Tests performance with deep recursive structures

  @Benchmark
  def domDeepGeneratedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domGeneratedWireParser.parse(deepDomBinary))
  }

  @Benchmark
  def domDeepProtoToRowParser(bh: Blackhole): Unit = {
    bh.consume(domProtoToRowParser.parse(deepDomBinary))
  }

  // @Benchmark
  // def domDeepDirectWireFormatParser(bh: Blackhole): Unit = {
  //   bh.consume(domDirectParser.parse(deepDomBinary))
  // }

  // @Benchmark
  // def domDeepDynamicMessageParser(bh: Blackhole): Unit = {
  //   bh.consume(domDynamicParser.parse(deepDomBinary))
  // }
}