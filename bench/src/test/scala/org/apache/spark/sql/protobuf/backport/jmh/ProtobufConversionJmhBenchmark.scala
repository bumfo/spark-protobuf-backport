package org.apache.spark.sql.protobuf.backport.jmh

import java.util.concurrent.TimeUnit
import benchmark.{ComplexBenchmarkProtos, SimpleBenchmarkProtos, TestDataGenerator}
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
 * Professional JMH benchmark for protobuf conversion performance using custom benchmark schemas.
 *
 * This benchmark uses JMH (Java Microbenchmark Harness) to provide:
 * - JVM fork isolation to prevent warmup contamination between benchmarks
 * - Proper warmup cycles before measurement
 * - Statistical analysis with confidence intervals
 * - Protection against dead code elimination
 *
 * Tests two scenarios:
 * 1. Simple schema with 120 fields (scalar and repeated scalar)
 * 2. Complex recursive schema (A <=> B with self-reference)
 *
 * Usage:
 * sbt "bench/Jmh/run .*ProtobufConversionJmhBenchmark.*"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class ProtobufConversionJmhBenchmark {

  var simpleBinary: Array[Byte] = _
  var complexBinary: Array[Byte] = _
  var simpleDescriptor: Descriptors.Descriptor = _
  var complexDescriptor: Descriptors.Descriptor = _
  var simpleSparkSchema: StructType = _
  var complexSparkSchema: StructType = _
  var simpleDescSet: Array[Byte] = _
  var complexDescSet: Array[Byte] = _
  var simpleTempDescFile: java.io.File = _
  var complexTempDescFile: java.io.File = _

  // Parsers for simple schema
  var simpleDirectParser: WireFormatParser = _
  var simpleGeneratedParser: StreamWireParser = _
  var simpleDynamicParser: DynamicMessageParser = _

  // Parsers for complex schema
  var complexDirectParser: WireFormatParser = _
  var complexGeneratedParser: StreamWireParser = _
  var complexDynamicParser: DynamicMessageParser = _

  @Setup
  def setup(): Unit = {
    // === Simple Schema Setup ===
    val simpleMsg = TestDataGenerator.createSimpleMessage()
    simpleBinary = simpleMsg.toByteArray
    simpleDescriptor = simpleMsg.getDescriptorForType

    simpleSparkSchema = SchemaConverters.toSqlType(simpleDescriptor).dataType.asInstanceOf[StructType]

    // === Complex Schema Setup ===
    val complexMsg = TestDataGenerator.createComplexMessage()
    complexBinary = complexMsg.toByteArray
    complexDescriptor = complexMsg.getDescriptorForType

    complexSparkSchema = SchemaConverters.toSqlType(complexDescriptor).dataType.asInstanceOf[StructType]

    // === Create FileDescriptorSets ===
    simpleDescSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(simpleDescriptor.getFile.toProto)
      .build()
      .toByteArray

    complexDescSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(complexDescriptor.getFile.toProto)
      .build()
      .toByteArray

    // === Create temporary descriptor files ===
    simpleTempDescFile = java.io.File.createTempFile("simple_benchmark_descriptor", ".desc")
    java.nio.file.Files.write(simpleTempDescFile.toPath, simpleDescSet)
    simpleTempDescFile.deleteOnExit()

    complexTempDescFile = java.io.File.createTempFile("complex_benchmark_descriptor", ".desc")
    java.nio.file.Files.write(complexTempDescFile.toPath, complexDescSet)
    complexTempDescFile.deleteOnExit()

    // === Initialize Simple Schema Parsers ===
    simpleDirectParser = new WireFormatParser(simpleDescriptor, simpleSparkSchema)
    simpleGeneratedParser = WireFormatToRowGenerator.generateParser(simpleDescriptor, simpleSparkSchema)

    val simpleFieldsNumbers = simpleDescriptor.getFields.asScala.map(_.getNumber).toSet
    simpleDynamicParser = new DynamicMessageParser(
      simpleDescriptor, simpleSparkSchema)

    // simpleBinaryDescExpression = ProtobufDataToCatalyst(
    //   child = Literal.create(simpleBinary, BinaryType),
    //   messageName = simpleDescriptor.getFullName,
    //   descFilePath = None,
    //   options = Map.empty,
    //   binaryDescriptorSet = Some(simpleDescSet)
    // )
    //
    // simpleDescriptorFileExpression = ProtobufDataToCatalyst(
    //   child = Literal.create(simpleBinary, BinaryType),
    //   messageName = simpleDescriptor.getFullName,
    //   descFilePath = Some(simpleTempDescFile.getAbsolutePath),
    //   options = Map.empty,
    //   binaryDescriptorSet = None
    // )
    //
    // simpleCompiledParser = ProtoToRowGenerator.generateParser(
    //   simpleDescriptor,
    //   classOf[SimpleBenchmarkProtos.SimpleMessage]
    // )


    // === Initialize Complex Schema Parsers ===
    complexDirectParser = new WireFormatParser(complexDescriptor, complexSparkSchema)
    complexGeneratedParser = WireFormatToRowGenerator.generateParser(complexDescriptor, complexSparkSchema)

    val complexFieldsNumbers = complexDescriptor.getFields.asScala.map(_.getNumber).toSet
    complexDynamicParser = new DynamicMessageParser(
      complexDescriptor, complexSparkSchema)

    // complexBinaryDescExpression = ProtobufDataToCatalyst(
    //   child = Literal.create(complexBinary, BinaryType),
    //   messageName = complexDescriptor.getFullName,
    //   descFilePath = None,
    //   options = Map.empty,
    //   binaryDescriptorSet = Some(complexDescSet)
    // )
    //
    // complexDescriptorFileExpression = ProtobufDataToCatalyst(
    //   child = Literal.create(complexBinary, BinaryType),
    //   messageName = complexDescriptor.getFullName,
    //   descFilePath = Some(complexTempDescFile.getAbsolutePath),
    //   options = Map.empty,
    //   binaryDescriptorSet = None
    // )
    //
    // complexCompiledParser = ProtoToRowGenerator.generateParser(
    //   complexDescriptor,
    //   classOf[ComplexBenchmarkProtos.ComplexMessageA]
    // )

  }

  @TearDown
  def teardown(): Unit = {
    if (simpleTempDescFile != null) simpleTempDescFile.delete()
    if (complexTempDescFile != null) complexTempDescFile.delete()
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

  // @Benchmark
  def simpleDynamicMessageParser(bh: Blackhole): Unit = {
    bh.consume(simpleDynamicParser.parse(simpleBinary))
  }

  // @Benchmark
  // def simpleCompiledMessageParser(bh: Blackhole): Unit = {
  //   bh.consume(simpleCompiledParser.convert(simpleBinary))
  // }
  //
  // @Benchmark
  // def simpleDynamicMessageDescriptorFile(bh: Blackhole): Unit = {
  //   bh.consume(simpleDescriptorFileExpression.nullSafeEval(simpleBinary))
  // }
  //
  // @Benchmark
  // def simpleDynamicMessageBinaryDescriptor(bh: Blackhole): Unit = {
  //   bh.consume(simpleBinaryDescExpression.nullSafeEval(simpleBinary))
  // }

  // === Complex Schema Benchmarks (Recursive A <=> B) ===

  // @Benchmark
  // def complexGeneratedWireFormatParser(bh: Blackhole): Unit = {
  //   bh.consume(complexGeneratedParser.convert(complexBinary))
  // }
  //
  // @Benchmark
  // def complexDirectWireFormatParser(bh: Blackhole): Unit = {
  //   bh.consume(complexDirectParser.convert(complexBinary))
  // }
  //
  // @Benchmark
  // def complexCompiledMessageParser(bh: Blackhole): Unit = {
  //   bh.consume(complexCompiledParser.convert(complexBinary))
  // }
  //
  // @Benchmark
  // def complexDynamicMessageDescriptorFile(bh: Blackhole): Unit = {
  //   bh.consume(complexDescriptorFileExpression.nullSafeEval(complexBinary))
  // }
  //
  // @Benchmark
  // def complexDynamicMessageBinaryDescriptor(bh: Blackhole): Unit = {
  //   bh.consume(complexBinaryDescExpression.nullSafeEval(complexBinary))
  // }

}