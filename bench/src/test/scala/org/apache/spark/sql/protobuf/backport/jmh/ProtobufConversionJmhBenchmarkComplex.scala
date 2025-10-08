package org.apache.spark.sql.protobuf.backport.jmh

import benchmark.{ComplexBenchmarkProtos, TestDataGenerator}
import com.google.protobuf.{DescriptorProtos, Descriptors}
import fastproto.{StreamWireParser, WireFormatParser, WireFormatToRowGenerator}
import org.apache.spark.sql.protobuf.backport.DynamicMessageParser
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit


/**
 * JMH benchmark for complex protobuf conversion performance (recursive schema A <=> B).
 *
 * This benchmark uses JMH (Java Microbenchmark Harness) to provide:
 * - JVM fork isolation to prevent warmup contamination between benchmarks
 * - Proper warmup cycles before measurement
 * - Statistical analysis with confidence intervals
 * - Protection against dead code elimination
 *
 * Tests complex recursive schema (A <=> B with self-reference).
 *
 * Usage:
 * sbt "bench/Jmh/run .*ProtobufConversionJmhBenchmarkComplex.*"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class ProtobufConversionJmhBenchmarkComplex {

  var complexBinary: Array[Byte] = _
  var complexDescriptor: Descriptors.Descriptor = _
  var complexSparkSchema: StructType = _
  var complexDescSet: Array[Byte] = _
  var complexTempDescFile: java.io.File = _

  // Parsers for complex schema
  var complexDirectParser: WireFormatParser = _
  var complexGeneratedParser: StreamWireParser = _
  var complexDynamicParser: DynamicMessageParser = _

  // Pruned schema parser (single nested scalar field: message_b.nested_data.count)
  var complexPrunedSchema: StructType = _
  var complexPrunedParser: WireFormatParser = _

  @Setup
  def setup(): Unit = {
    // === Complex Schema Setup ===
    val complexMsg = TestDataGenerator.createComplexMessage()
    complexBinary = complexMsg.toByteArray
    complexDescriptor = complexMsg.getDescriptorForType

    complexSparkSchema = SchemaConverters.toSqlType(complexDescriptor).dataType.asInstanceOf[StructType]

    // === Create FileDescriptorSet ===
    complexDescSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(complexDescriptor.getFile.toProto)
      .build()
      .toByteArray

    // === Create temporary descriptor file ===
    complexTempDescFile = java.io.File.createTempFile("complex_benchmark_descriptor", ".desc")
    java.nio.file.Files.write(complexTempDescFile.toPath, complexDescSet)
    complexTempDescFile.deleteOnExit()

    // === Initialize Complex Schema Parsers ===
    complexDirectParser = new WireFormatParser(complexDescriptor, complexSparkSchema)
    complexGeneratedParser = WireFormatToRowGenerator.generateParser(complexDescriptor, complexSparkSchema)
    complexDynamicParser = new DynamicMessageParser(complexDescriptor, complexSparkSchema)

    // === Initialize Pruned Schema Parser (nested scalar: message_b.nested_data.count) ===
    val prunedNestedDataSchema = StructType(Seq(
      StructField("count", IntegerType, nullable = false)
    ))
    val prunedMessageBSchema = StructType(Seq(
      StructField("nested_data", prunedNestedDataSchema, nullable = true)
    ))
    complexPrunedSchema = StructType(Seq(
      StructField("message_b", prunedMessageBSchema, nullable = true)
    ))
    complexPrunedParser = new WireFormatParser(complexDescriptor, complexPrunedSchema)
  }

  @TearDown
  def teardown(): Unit = {
    if (complexTempDescFile != null) complexTempDescFile.delete()
  }

  // === Complex Schema Benchmarks (Recursive A <=> B) ===

  @Benchmark
  def complexGeneratedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(complexGeneratedParser.parse(complexBinary))
  }

  @Benchmark
  def complexDirectWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(complexDirectParser.parse(complexBinary))
  }

  @Benchmark
  def complexPrunedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(complexPrunedParser.parse(complexBinary))
  }

  // @Benchmark
  def complexProtoParsing(bh: Blackhole): Unit = {
    bh.consume(ComplexBenchmarkProtos.ComplexMessageA.parseFrom(complexBinary))
  }

  // @Benchmark
  def complexDynamicMessageParser(bh: Blackhole): Unit = {
    bh.consume(complexDynamicParser.parse(complexBinary))
  }
}