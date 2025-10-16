package org.apache.spark.sql.protobuf.backport.jmh

import benchmark.{SimpleBenchmarkProtos, TestDataGenerator}
import com.google.protobuf.{DescriptorProtos, Descriptors}
import fastproto.{InlineParserToRowGenerator, StreamWireParser, WireFormatParser, WireFormatToRowGenerator}
import org.apache.spark.sql.protobuf.backport.DynamicMessageParser
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit


/**
 * JMH benchmark for simple protobuf conversion performance (120 scalar fields).
 *
 * This benchmark uses JMH (Java Microbenchmark Harness) to provide:
 * - JVM fork isolation to prevent warmup contamination between benchmarks
 * - Proper warmup cycles before measurement
 * - Statistical analysis with confidence intervals
 * - Protection against dead code elimination
 *
 * Tests simple schema with 120 fields (scalar and repeated scalar).
 *
 * Usage:
 * sbt "bench/Jmh/run .*SimpleProtoBenchmark.*"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class SimpleProtoBenchmark {

  var simpleBinary: Array[Byte] = _
  var simpleDescriptor: Descriptors.Descriptor = _
  var simpleSparkSchema: StructType = _
  var simpleDescSet: Array[Byte] = _
  var simpleTempDescFile: java.io.File = _

  // Parsers for simple schema
  var simpleDirectParser: WireFormatParser = _
  var simpleGeneratedParser: StreamWireParser = _
  var simpleInlineParser: StreamWireParser = _
  var simpleDynamicParser: DynamicMessageParser = _

  // Pruned schema parser (single field)
  var simplePrunedSchema: StructType = _
  var simplePrunedParser: WireFormatParser = _
  var simpleInlinePrunedParser: StreamWireParser = _

  @Setup
  def setup(): Unit = {
    // === Simple Schema Setup ===
    val simpleMsg = TestDataGenerator.createSimpleMessage()
    simpleBinary = simpleMsg.toByteArray
    simpleDescriptor = simpleMsg.getDescriptorForType

    simpleSparkSchema = SchemaConverters.toSqlType(simpleDescriptor).dataType.asInstanceOf[StructType]

    // === Create FileDescriptorSet ===
    simpleDescSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(simpleDescriptor.getFile.toProto)
      .build()
      .toByteArray

    // === Create temporary descriptor file ===
    simpleTempDescFile = java.io.File.createTempFile("simple_benchmark_descriptor", ".desc")
    java.nio.file.Files.write(simpleTempDescFile.toPath, simpleDescSet)
    simpleTempDescFile.deleteOnExit()

    // === Initialize Simple Schema Parsers ===
    simpleDirectParser = new WireFormatParser(simpleDescriptor, simpleSparkSchema)
    simpleGeneratedParser = WireFormatToRowGenerator.generateParser(simpleDescriptor, simpleSparkSchema)
    simpleInlineParser = InlineParserToRowGenerator.generateParser(simpleDescriptor, simpleSparkSchema)
    simpleDynamicParser = new DynamicMessageParser(simpleDescriptor, simpleSparkSchema)

    // === Initialize Pruned Schema Parser (single scalar field from middle) ===
    simplePrunedSchema = StructType(Seq(
      StructField("field_double_055", DoubleType, nullable = false)
    ))
    simplePrunedParser = new WireFormatParser(simpleDescriptor, simplePrunedSchema)
    simpleInlinePrunedParser = InlineParserToRowGenerator.generateParser(simpleDescriptor, simplePrunedSchema)
  }

  @TearDown
  def teardown(): Unit = {
    if (simpleTempDescFile != null) simpleTempDescFile.delete()
  }

  // === Simple Schema Benchmarks (120 fields) ===

  @Benchmark
  def anInlineParser(bh: Blackhole): Unit = {
    bh.consume(simpleInlineParser.parse(simpleBinary))
  }

  @Benchmark
  def anInlineParserPruned(bh: Blackhole): Unit = {
    bh.consume(simpleInlinePrunedParser.parse(simpleBinary))
  }

  @Benchmark
  def generatedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(simpleGeneratedParser.parse(simpleBinary))
  }

  @Benchmark
  def directWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(simpleDirectParser.parse(simpleBinary))
  }

  @Benchmark
  def prunedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(simplePrunedParser.parse(simpleBinary))
  }

  // @Benchmark
  def dynamicMessageParser(bh: Blackhole): Unit = {
    bh.consume(simpleDynamicParser.parse(simpleBinary))
  }

  @Benchmark
  def protoParsing(bh: Blackhole): Unit = {
    bh.consume(SimpleBenchmarkProtos.SimpleMessage.parser().parseFrom(simpleBinary))
  }
}