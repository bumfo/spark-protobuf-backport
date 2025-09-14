package org.apache.spark.sql.protobuf.backport.jmh

import java.util.concurrent.TimeUnit

import com.google.protobuf._
import fastproto.{ProtoToRowGenerator, WireFormatConverter, WireFormatToRowGenerator}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.protobuf.backport.ProtobufDataToCatalyst
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

/**
 * Professional JMH benchmark for protobuf conversion performance.
 *
 * This benchmark uses JMH (Java Microbenchmark Harness) to provide:
 * - JVM fork isolation to prevent warmup contamination between benchmarks
 * - Proper warmup cycles before measurement
 * - Statistical analysis with confidence intervals
 * - Protection against dead code elimination
 *
 * Usage:
 * sbt "core/Jmh/run .*ProtobufConversionJmhBenchmark.*"
 *
 * Or for specific benchmark:
 * sbt "core/Jmh/run .*ProtobufConversionJmhBenchmark.directWireFormatConverter"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class ProtobufConversionJmhBenchmark {

  var binary: Array[Byte] = _
  var complexBinary: Array[Byte] = _
  var descriptor: com.google.protobuf.Descriptors.Descriptor = _
  var complexDescriptor: com.google.protobuf.Descriptors.Descriptor = _
  var sparkSchema: StructType = _
  var complexSparkSchema: StructType = _
  var descSet: Array[Byte] = _
  var complexDescSet: Array[Byte] = _
  var tempDescFile: java.io.File = _
  var complexTempDescFile: java.io.File = _

  // Converters
  var directConverter: WireFormatConverter = _
  var complexDirectConverter: WireFormatConverter = _
  var binaryDescExpression: ProtobufDataToCatalyst = _
  var complexBinaryDescExpression: ProtobufDataToCatalyst = _
  var descriptorFileExpression: ProtobufDataToCatalyst = _
  var complexDescriptorFileExpression: ProtobufDataToCatalyst = _
  var compiledConverter: fastproto.RowConverter = _
  var complexCompiledConverter: fastproto.RowConverter = _

  // Generated WireFormatConverter instances
  var generatedConverter: fastproto.AbstractWireFormatConverter = _
  var complexGeneratedConverter: fastproto.AbstractWireFormatConverter = _

  @Setup
  def setup(): Unit = {
    // Create simple test data
    val typeMsg = Type.newBuilder()
      .setName("jmh_benchmark_type")
      .addFields(Field.newBuilder()
        .setName("field1")
        .setNumber(1)
        .setKind(Field.Kind.TYPE_STRING)
        .build())
      .addFields(Field.newBuilder()
        .setName("field2")
        .setNumber(2)
        .setKind(Field.Kind.TYPE_INT32)
        .build())
      .build()

    binary = typeMsg.toByteArray
    descriptor = typeMsg.getDescriptorForType

    sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true),
        StructField("kind", StringType, nullable = true)
      ))), nullable = true)
    ))

    // Create complex test data
    val nestedField = Field.newBuilder()
      .setName("nested_field")
      .setNumber(1)
      .setKind(Field.Kind.TYPE_MESSAGE)
      .setTypeUrl("google.protobuf.Type")
      .build()

    val complexType = Type.newBuilder()
      .setName("jmh_complex_benchmark_type")
      .addFields(nestedField)
      .setSourceContext(SourceContext.newBuilder()
        .setFileName("jmh_benchmark.proto")
        .build())
      .setSyntax(Syntax.SYNTAX_PROTO3)
      .build()

    complexBinary = complexType.toByteArray
    complexDescriptor = complexType.getDescriptorForType

    complexSparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true),
        StructField("kind", StringType, nullable = true),
        StructField("type_url", StringType, nullable = true)
      ))), nullable = true),
      StructField("source_context", StructType(Seq(
        StructField("file_name", StringType, nullable = true)
      )), nullable = true),
      StructField("syntax", StringType, nullable = true)
    ))

    // Create FileDescriptorSets
    descSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(descriptor.getFile.toProto)
      .addFile(AnyProto.getDescriptor.getFile.toProto)
      .addFile(SourceContextProto.getDescriptor.getFile.toProto)
      .addFile(ApiProto.getDescriptor.getFile.toProto)
      .build()
      .toByteArray

    complexDescSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(complexDescriptor.getFile.toProto)
      .addFile(AnyProto.getDescriptor.getFile.toProto)
      .addFile(SourceContextProto.getDescriptor.getFile.toProto)
      .addFile(ApiProto.getDescriptor.getFile.toProto)
      .build()
      .toByteArray

    // Create temporary descriptor files for DynamicMessage path
    tempDescFile = java.io.File.createTempFile("jmh_benchmark_descriptor", ".desc")
    java.nio.file.Files.write(tempDescFile.toPath, descSet)
    tempDescFile.deleteOnExit()

    complexTempDescFile = java.io.File.createTempFile("jmh_complex_benchmark_descriptor", ".desc")
    java.nio.file.Files.write(complexTempDescFile.toPath, complexDescSet)
    complexTempDescFile.deleteOnExit()

    // Initialize converters
    directConverter = new WireFormatConverter(descriptor, sparkSchema)
    complexDirectConverter = new WireFormatConverter(complexDescriptor, complexSparkSchema)

    binaryDescExpression = ProtobufDataToCatalyst(
      child = Literal.create(binary, BinaryType),
      messageName = descriptor.getFullName,
      descFilePath = None,
      options = Map.empty,
      binaryDescriptorSet = Some(descSet)
    )

    complexBinaryDescExpression = ProtobufDataToCatalyst(
      child = Literal.create(complexBinary, BinaryType),
      messageName = complexDescriptor.getFullName,
      descFilePath = None,
      options = Map.empty,
      binaryDescriptorSet = Some(complexDescSet)
    )

    descriptorFileExpression = ProtobufDataToCatalyst(
      child = Literal.create(binary, BinaryType),
      messageName = descriptor.getFullName,
      descFilePath = Some(tempDescFile.getAbsolutePath),
      options = Map.empty,
      binaryDescriptorSet = None
    )

    complexDescriptorFileExpression = ProtobufDataToCatalyst(
      child = Literal.create(complexBinary, BinaryType),
      messageName = complexDescriptor.getFullName,
      descFilePath = Some(complexTempDescFile.getAbsolutePath),
      options = Map.empty,
      binaryDescriptorSet = None
    )

    compiledConverter = ProtoToRowGenerator.generateConverter(descriptor, classOf[com.google.protobuf.Type])
    complexCompiledConverter = ProtoToRowGenerator.generateConverter(complexDescriptor, classOf[com.google.protobuf.Type])

    // Initialize generated WireFormat converters
    generatedConverter = WireFormatToRowGenerator.generateConverter(descriptor, sparkSchema)
    complexGeneratedConverter = WireFormatToRowGenerator.generateConverter(complexDescriptor, complexSparkSchema)
  }

  @TearDown
  def teardown(): Unit = {
    if (tempDescFile != null) tempDescFile.delete()
    if (complexTempDescFile != null) complexTempDescFile.delete()
  }

  // Simple structure benchmarks

  @Benchmark
  def generatedWireFormatConverter(bh: Blackhole): Unit = {
    bh.consume(generatedConverter.convert(binary))
  }

  @Benchmark
  def directWireFormatConverter(bh: Blackhole): Unit = {
    bh.consume(directConverter.convert(binary))
  }

  @Benchmark
  def compiledMessageConverter(bh: Blackhole): Unit = {
    bh.consume(compiledConverter.convert(binary))
  }

  // Complex structure benchmarks

  @Benchmark
  def complexGeneratedWireFormatConverter(bh: Blackhole): Unit = {
    bh.consume(complexGeneratedConverter.convert(complexBinary))
  }

  @Benchmark
  def complexDirectWireFormatConverter(bh: Blackhole): Unit = {
    bh.consume(complexDirectConverter.convert(complexBinary))
  }

  @Benchmark
  def complexCompiledMessageConverter(bh: Blackhole): Unit = {
    bh.consume(complexCompiledConverter.convert(complexBinary))
  }
}