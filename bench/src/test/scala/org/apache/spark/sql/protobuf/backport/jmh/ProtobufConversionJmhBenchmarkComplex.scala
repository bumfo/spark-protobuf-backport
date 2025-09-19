package org.apache.spark.sql.protobuf.backport.jmh

import java.util.concurrent.TimeUnit
import benchmark.{ComplexBenchmarkProtos, TestDataGenerator}
import com.google.protobuf.{DescriptorProtos, Descriptors}
import fastproto.{ProtoToRowGenerator, StreamWireParser, WireFormatParser, WireFormatToRowGenerator}
import org.apache.spark.sql.protobuf.backport.DynamicMessageParser
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.protobuf.backport.ProtobufDataToCatalyst
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters._


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
  var complexCompiledParser: Option[Any] = _

  // Expressions for integration testing
  var complexBinaryDescExpression: ProtobufDataToCatalyst = _
  var complexDescriptorFileExpression: ProtobufDataToCatalyst = _

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

    // Try to create compiled parser
    try {
      complexCompiledParser = Some(ProtoToRowGenerator.generateParser(
        complexDescriptor,
        classOf[ComplexBenchmarkProtos.ComplexMessageA]
      ))
    } catch {
      case _: Exception => complexCompiledParser = None
    }

    // === Initialize Catalyst Expressions ===
    complexBinaryDescExpression = ProtobufDataToCatalyst(
      child = Literal.create(complexBinary, BinaryType),
      messageName = complexDescriptor.getFullName,
      descFilePath = None,
      options = Map.empty,
      binaryDescriptorSet = Some(complexDescSet)
    )

    complexDescriptorFileExpression = ProtobufDataToCatalyst(
      child = Literal.create(complexBinary, BinaryType),
      messageName = complexDescriptor.getFullName,
      descFilePath = Some(complexTempDescFile.getAbsolutePath),
      options = Map.empty,
      binaryDescriptorSet = None
    )
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
  def complexDynamicMessageParser(bh: Blackhole): Unit = {
    bh.consume(complexDynamicParser.parse(complexBinary))
  }

  @Benchmark
  def complexCompiledMessageParser(bh: Blackhole): Unit = {
    complexCompiledParser match {
      case Some(parser) =>
        // Use reflection to call parse method since we don't know exact type
        val parseMethod = parser.getClass.getMethod("parse", classOf[Array[Byte]])
        bh.consume(parseMethod.invoke(parser, complexBinary))
      case None =>
        // Fallback to dynamic parser if compiled parser not available
        bh.consume(complexDynamicParser.parse(complexBinary))
    }
  }

  @Benchmark
  def complexDynamicMessageDescriptorFile(bh: Blackhole): Unit = {
    bh.consume(complexDescriptorFileExpression.nullSafeEval(complexBinary))
  }

  @Benchmark
  def complexDynamicMessageBinaryDescriptor(bh: Blackhole): Unit = {
    bh.consume(complexBinaryDescExpression.nullSafeEval(complexBinary))
  }
}