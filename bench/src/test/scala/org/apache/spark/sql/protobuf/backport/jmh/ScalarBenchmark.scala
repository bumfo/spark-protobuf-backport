package org.apache.spark.sql.protobuf.backport.jmh

import benchmark.{ScalarBenchmarkProtos, TestDataGenerator}
import com.google.protobuf.Descriptors
import fastproto.{InlineParserToRowGenerator, StreamWireParser, WireFormatParser, WireFormatToRowGenerator}
import org.apache.spark.sql.protobuf.backport.DynamicMessageParser
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * JMH benchmark for scalar field parsing performance.
 *
 * Compares WireFormatToRowGenerator vs InlineParserToRowGenerator for 4 scalar fields:
 * - int32 (varint encoding)
 * - int64 (varint encoding)
 * - float (fixed32 encoding)
 * - double (fixed64 encoding)
 *
 * Field selection options:
 * - "int32": Parse only int32_field
 * - "int64": Parse only int64_field
 * - "float": Parse only float_field
 * - "double": Parse only double_field
 * - "all": Parse all 4 fields
 *
 * Usage:
 *   sbt "bench/Jmh/run .*ScalarBenchmark.*"
 *   sbt "bench/Jmh/run .*ScalarBenchmark.* -p field=int32"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class ScalarBenchmark {

  // @Param(Array("int32", "int64", "float", "double", "all"))
  @Param(Array("int32"))
  var field: String = _

  var scalarBinary: Array[Byte] = _
  var scalarDescriptor: Descriptors.Descriptor = _
  var scalarSchema: StructType = _

  // Parsers
  var directParser: WireFormatParser = _
  var generatedParser: StreamWireParser = _
  var inlineParser: StreamWireParser = _
  var dynamicParser: DynamicMessageParser = _

  @Setup
  def setup(): Unit = {
    val scalarMsg = TestDataGenerator.createScalarMessage()
    scalarBinary = scalarMsg.toByteArray
    scalarDescriptor = scalarMsg.getDescriptorForType

    // Create schema based on field parameter
    scalarSchema = field match {
      case "int32" => StructType(Seq(StructField("int32_field", IntegerType, nullable = false)))
      case "int64" => StructType(Seq(StructField("int64_field", LongType, nullable = false)))
      case "float" => StructType(Seq(StructField("float_field", FloatType, nullable = false)))
      case "double" => StructType(Seq(StructField("double_field", DoubleType, nullable = false)))
      case "all" => SchemaConverters.toSqlType(scalarDescriptor).dataType.asInstanceOf[StructType]
    }

    // Initialize parsers
    directParser = new WireFormatParser(scalarDescriptor, scalarSchema)
    generatedParser = WireFormatToRowGenerator.generateParser(scalarDescriptor, scalarSchema)
    inlineParser = InlineParserToRowGenerator.generateParser(scalarDescriptor, scalarSchema)
    dynamicParser = new DynamicMessageParser(scalarDescriptor, scalarSchema)
  }

  @Benchmark
  def anInlineParser(bh: Blackhole): Unit = {
    bh.consume(inlineParser.parse(scalarBinary))
  }

  @Benchmark
  def generatedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(generatedParser.parse(scalarBinary))
  }

  // @Benchmark
  def directWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(directParser.parse(scalarBinary))
  }

  // @Benchmark
  def dynamicMessageParser(bh: Blackhole): Unit = {
    bh.consume(dynamicParser.parse(scalarBinary))
  }

  @Benchmark
  def protoParsing(bh: Blackhole): Unit = {
    bh.consume(ScalarBenchmarkProtos.ScalarMessage.parser().parseFrom(scalarBinary))
  }
}
