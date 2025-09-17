package org.apache.spark.sql.protobuf.backport.jmh

import java.util.concurrent.TimeUnit

import benchmark.{ComplexBenchmarkProtos, SimpleBenchmarkProtos, TestDataGenerator}
import com.google.protobuf.{DescriptorProtos, Descriptors}
import fastproto.{ProtoToRowGenerator, WireFormatConverter, WireFormatToRowGenerator}
import org.apache.spark.sql.protobuf.backport.DynamicMessageConverter
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

  // Converters for simple schema
  var simpleDirectConverter: WireFormatConverter = _
  var simpleGeneratedConverter: fastproto.AbstractWireFormatConverter = _
  var simpleDynamicConverter: DynamicMessageConverter = _

  // Converters for complex schema
  var complexDirectConverter: WireFormatConverter = _
  var complexGeneratedConverter: fastproto.AbstractWireFormatConverter = _
  var complexDynamicConverter: DynamicMessageConverter = _

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

    // === Initialize Simple Schema Converters ===
    simpleDirectConverter = new WireFormatConverter(simpleDescriptor, simpleSparkSchema)
    simpleGeneratedConverter = WireFormatToRowGenerator.generateConverter(simpleDescriptor, simpleSparkSchema)

    val simpleFieldsNumbers = simpleDescriptor.getFields.asScala.map(_.getNumber).toSet
    simpleDynamicConverter = new DynamicMessageConverter(
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
    // simpleCompiledConverter = ProtoToRowGenerator.generateConverter(
    //   simpleDescriptor,
    //   classOf[SimpleBenchmarkProtos.SimpleMessage]
    // )


    // === Initialize Complex Schema Converters ===
    complexDirectConverter = new WireFormatConverter(complexDescriptor, complexSparkSchema)
    complexGeneratedConverter = WireFormatToRowGenerator.generateConverter(complexDescriptor, complexSparkSchema)

    val complexFieldsNumbers = complexDescriptor.getFields.asScala.map(_.getNumber).toSet
    complexDynamicConverter = new DynamicMessageConverter(
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
    // complexCompiledConverter = ProtoToRowGenerator.generateConverter(
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
  def simpleGeneratedWireFormatConverter(bh: Blackhole): Unit = {
    bh.consume(simpleGeneratedConverter.convert(simpleBinary))
  }

  @Benchmark
  def simpleDirectWireFormatConverter(bh: Blackhole): Unit = {
    bh.consume(simpleDirectConverter.convert(simpleBinary))
  }

  @Benchmark
  def simpleDynamicMessageConverter(bh: Blackhole): Unit = {
    bh.consume(simpleDynamicConverter.convert(simpleBinary))
  }

  // @Benchmark
  // def simpleCompiledMessageConverter(bh: Blackhole): Unit = {
  //   bh.consume(simpleCompiledConverter.convert(simpleBinary))
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
  // def complexGeneratedWireFormatConverter(bh: Blackhole): Unit = {
  //   bh.consume(complexGeneratedConverter.convert(complexBinary))
  // }
  //
  // @Benchmark
  // def complexDirectWireFormatConverter(bh: Blackhole): Unit = {
  //   bh.consume(complexDirectConverter.convert(complexBinary))
  // }
  //
  // @Benchmark
  // def complexCompiledMessageConverter(bh: Blackhole): Unit = {
  //   bh.consume(complexCompiledConverter.convert(complexBinary))
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