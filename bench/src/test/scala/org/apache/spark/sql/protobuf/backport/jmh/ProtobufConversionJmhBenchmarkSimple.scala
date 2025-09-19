package org.apache.spark.sql.protobuf.backport.jmh

import java.util.concurrent.TimeUnit
import benchmark.{SimpleBenchmarkProtos, TestDataGenerator}
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
 * sbt "bench/Jmh/run .*ProtobufConversionJmhBenchmarkSimple.*"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class ProtobufConversionJmhBenchmarkSimple {

  var simpleBinary: Array[Byte] = _
  var simpleDescriptor: Descriptors.Descriptor = _
  var simpleSparkSchema: StructType = _
  var simpleDescSet: Array[Byte] = _
  var simpleTempDescFile: java.io.File = _

  // Parsers for simple schema
  var simpleDirectParser: WireFormatParser = _
  var simpleGeneratedParser: StreamWireParser = _
  var simpleDynamicParser: DynamicMessageParser = _
  var simpleCompiledParser: Option[Any] = _

  // Expressions for integration testing
  var simpleBinaryDescExpression: ProtobufDataToCatalyst = _
  var simpleDescriptorFileExpression: ProtobufDataToCatalyst = _

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
    simpleDynamicParser = new DynamicMessageParser(simpleDescriptor, simpleSparkSchema)

    // Try to create compiled parser
    try {
      simpleCompiledParser = Some(ProtoToRowGenerator.generateParser(
        simpleDescriptor,
        classOf[SimpleBenchmarkProtos.SimpleMessage]
      ))
    } catch {
      case _: Exception => simpleCompiledParser = None
    }

    // === Initialize Catalyst Expressions ===
    simpleBinaryDescExpression = ProtobufDataToCatalyst(
      child = Literal.create(simpleBinary, BinaryType),
      messageName = simpleDescriptor.getFullName,
      descFilePath = None,
      options = Map.empty,
      binaryDescriptorSet = Some(simpleDescSet)
    )

    simpleDescriptorFileExpression = ProtobufDataToCatalyst(
      child = Literal.create(simpleBinary, BinaryType),
      messageName = simpleDescriptor.getFullName,
      descFilePath = Some(simpleTempDescFile.getAbsolutePath),
      options = Map.empty,
      binaryDescriptorSet = None
    )
  }

  @TearDown
  def teardown(): Unit = {
    if (simpleTempDescFile != null) simpleTempDescFile.delete()
  }

  // === Simple Schema Benchmarks (120 fields) ===

  @Benchmark
  def simpleGeneratedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(simpleGeneratedParser.parse(simpleBinary))
  }

  @Benchmark
  def simpleDirectWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(simpleDirectParser.parse(simpleBinary))
  }

  @Benchmark
  def simpleDynamicMessageParser(bh: Blackhole): Unit = {
    bh.consume(simpleDynamicParser.parse(simpleBinary))
  }

  @Benchmark
  def simpleCompiledMessageParser(bh: Blackhole): Unit = {
    simpleCompiledParser match {
      case Some(parser) =>
        // Use reflection to call parse method since we don't know exact type
        val parseMethod = parser.getClass.getMethod("parse", classOf[Array[Byte]])
        bh.consume(parseMethod.invoke(parser, simpleBinary))
      case None =>
        // Fallback to dynamic parser if compiled parser not available
        bh.consume(simpleDynamicParser.parse(simpleBinary))
    }
  }

  @Benchmark
  def simpleDynamicMessageDescriptorFile(bh: Blackhole): Unit = {
    bh.consume(simpleDescriptorFileExpression.nullSafeEval(simpleBinary))
  }

  @Benchmark
  def simpleDynamicMessageBinaryDescriptor(bh: Blackhole): Unit = {
    bh.consume(simpleBinaryDescExpression.nullSafeEval(simpleBinary))
  }
}