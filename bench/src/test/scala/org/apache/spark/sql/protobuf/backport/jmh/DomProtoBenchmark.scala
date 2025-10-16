package org.apache.spark.sql.protobuf.backport.jmh

import benchmark.DomBenchmarkProtos.DomDocument
import benchmark.DomTestDataGenerator
import com.google.protobuf.{DescriptorProtos, Descriptors}
import fastproto.{InlineParserToRowGenerator, Parser, ProtoToRowGenerator, RecursiveSchemaConverters, StreamWireParser, WireFormatParser, WireFormatToRowGenerator}
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * JMH benchmark for DOM tree protobuf conversion with full (non-pruned) schema.
 *
 * This benchmark tests parsing performance when accessing all fields in the DOM structure.
 * Complements PrunedDomProtoBenchmark which tests selective field access scenarios.
 *
 * Usage:
 * sbt "bench/Jmh/run .*DomProtoBenchmark.*"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class DomProtoBenchmark {

  // Test data with different complexity levels
  var shallowDomBinary: Array[Byte] = _ // depth=3, breadth=2
  var standardDomBinary: Array[Byte] = _ // depth=6, breadth=3
  var deepDomBinary: Array[Byte] = _ // depth=8, breadth=4

  // Descriptors and schemas
  var domDescriptor: Descriptors.Descriptor = _
  var domSparkSchema: StructType = _
  var domDescSet: Array[Byte] = _
  var domTempDescFile: java.io.File = _

  // Parsers for DOM structure (using standard complexity)
  var domDirectParser: WireFormatParser = _
  var domGeneratedWireParser: StreamWireParser = _
  var domInlineParser: StreamWireParser = _
  var domProtoToRowParser: Parser = _

  @Setup
  def setup(): Unit = {
    // Create DOM test data with different complexity levels
    val shallowDom = DomTestDataGenerator.createDomDocument(maxDepth = 3, breadthFactor = 2)
    shallowDomBinary = shallowDom.toByteArray

    val standardDom = DomTestDataGenerator.createDomDocument(maxDepth = 6, breadthFactor = 3)
    standardDomBinary = standardDom.toByteArray

    val deepDom = DomTestDataGenerator.createDomDocument(maxDepth = 8, breadthFactor = 4)
    deepDomBinary = deepDom.toByteArray

    domDescriptor = standardDom.getDescriptorForType

    // Create Spark schema with TRUE recursion (full schema, all fields)
    domSparkSchema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(domDescriptor)

    // Create FileDescriptorSet
    domDescSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(domDescriptor.getFile.toProto)
      .build()
      .toByteArray

    // Create temporary descriptor file
    domTempDescFile = java.io.File.createTempFile("dom_benchmark_descriptor", ".desc")
    java.nio.file.Files.write(domTempDescFile.toPath, domDescSet)
    domTempDescFile.deleteOnExit()

    // Initialize parsers
    domDirectParser = new WireFormatParser(domDescriptor, domSparkSchema)
    domGeneratedWireParser = WireFormatToRowGenerator.generateParser(domDescriptor, domSparkSchema)
    domInlineParser = InlineParserToRowGenerator.generateParser(domDescriptor, domSparkSchema)
    domProtoToRowParser = ProtoToRowGenerator.generateParser(domDescriptor, classOf[DomDocument], domSparkSchema)
  }

  @TearDown
  def teardown(): Unit = {
    if (domTempDescFile != null) domTempDescFile.delete()
  }

  // === Standard Complexity Benchmarks (depth=6, breadth=3) ===

  // @Benchmark
  def standardAnInlineParser(bh: Blackhole): Unit = {
    bh.consume(domInlineParser.parse(standardDomBinary))
  }

  // @Benchmark
  def standardGeneratedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domGeneratedWireParser.parse(standardDomBinary))
  }

  // @Benchmark
  def standardProtoToRowParser(bh: Blackhole): Unit = {
    bh.consume(domProtoToRowParser.parse(standardDomBinary))
  }

  // @Benchmark
  def standardDirectWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domDirectParser.parse(standardDomBinary))
  }

  // === Shallow Complexity Benchmarks (depth=3, breadth=2) ===

  // @Benchmark
  def shallowAnInlineParser(bh: Blackhole): Unit = {
    bh.consume(domInlineParser.parse(shallowDomBinary))
  }

  // @Benchmark
  def shallowGeneratedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domGeneratedWireParser.parse(shallowDomBinary))
  }

  // @Benchmark
  def shallowProtoToRowParser(bh: Blackhole): Unit = {
    bh.consume(domProtoToRowParser.parse(shallowDomBinary))
  }

  // @Benchmark
  def shallowDirectWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domDirectParser.parse(shallowDomBinary))
  }

  // === Deep Complexity Benchmarks (depth=8, breadth=4) ===

  @Benchmark
  def deepAnInlineParser(bh: Blackhole): Unit = {
    bh.consume(domInlineParser.parse(deepDomBinary))
  }

  @Benchmark
  def deepGeneratedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domGeneratedWireParser.parse(deepDomBinary))
  }

  // @Benchmark
  def deepProtoToRowParser(bh: Blackhole): Unit = {
    bh.consume(domProtoToRowParser.parse(deepDomBinary))
  }

  // @Benchmark
  def deepDirectWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(domDirectParser.parse(deepDomBinary))
  }

  // === Baseline: Protobuf-only parsing (no Spark conversion) ===

  @Benchmark
  def protoParsing(bh: Blackhole): Unit = {
    bh.consume(DomDocument.parseFrom(deepDomBinary))
  }
}
