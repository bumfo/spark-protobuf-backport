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
 * JMH benchmark for repeated scalar field parsing performance (unpacked encoding).
 *
 * Compares WireFormatToRowGenerator vs InlineParserToRowGenerator for 4 repeated scalar fields:
 * - repeated int32 (unpacked - one tag per element)
 * - repeated int64 (unpacked - one tag per element)
 * - repeated float (unpacked - one tag per element)
 * - repeated double (unpacked - one tag per element)
 *
 * Parameters:
 * - size: Array size (1, 100, 10000)
 * - field: Field selection ("int32", "int64", "float", "double", "all")
 *
 * Usage:
 *   sbt "bench/Jmh/run .*ScalarArrayUnpackedBenchmark.*"
 *   sbt "bench/Jmh/run .*ScalarArrayUnpackedBenchmark.* -p size=100 -p field=int32"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class ScalarArrayUnpackedBenchmark {

  @Param(Array("1", "100", "10000"))
  var size: Int = _

  // @Param(Array("int32", "int64", "float", "double", "all"))
  @Param(Array("int32"))
  var field: String = _

  var scalarArrayBinary: Array[Byte] = _
  var scalarArrayDescriptor: Descriptors.Descriptor = _
  var scalarArraySchema: StructType = _

  // Parsers
  var directParser: WireFormatParser = _
  var generatedParser: StreamWireParser = _
  var inlineParser: StreamWireParser = _
  var dynamicParser: DynamicMessageParser = _

  @Setup
  def setup(): Unit = {
    val scalarArrayMsg = TestDataGenerator.createScalarArrayUnpackedMessage(size)
    scalarArrayBinary = scalarArrayMsg.toByteArray
    scalarArrayDescriptor = scalarArrayMsg.getDescriptorForType

    // Create schema based on field parameter
    scalarArraySchema = field match {
      case "int32" => StructType(Seq(StructField("int32_array", ArrayType(IntegerType, containsNull = false), nullable = false)))
      case "int64" => StructType(Seq(StructField("int64_array", ArrayType(LongType, containsNull = false), nullable = false)))
      case "float" => StructType(Seq(StructField("float_array", ArrayType(FloatType, containsNull = false), nullable = false)))
      case "double" => StructType(Seq(StructField("double_array", ArrayType(DoubleType, containsNull = false), nullable = false)))
      case "all" => SchemaConverters.toSqlType(scalarArrayDescriptor).dataType.asInstanceOf[StructType]
    }

    // Initialize parsers
    directParser = new WireFormatParser(scalarArrayDescriptor, scalarArraySchema)
    generatedParser = WireFormatToRowGenerator.generateParser(scalarArrayDescriptor, scalarArraySchema)
    inlineParser = InlineParserToRowGenerator.generateParser(scalarArrayDescriptor, scalarArraySchema)
    dynamicParser = new DynamicMessageParser(scalarArrayDescriptor, scalarArraySchema)
  }

  @Benchmark
  def inlineParser(bh: Blackhole): Unit = {
    bh.consume(inlineParser.parse(scalarArrayBinary))
  }

  @Benchmark
  def generatedWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(generatedParser.parse(scalarArrayBinary))
  }

  // @Benchmark
  def directWireFormatParser(bh: Blackhole): Unit = {
    bh.consume(directParser.parse(scalarArrayBinary))
  }

  // @Benchmark
  def dynamicMessageParser(bh: Blackhole): Unit = {
    bh.consume(dynamicParser.parse(scalarArrayBinary))
  }

  @Benchmark
  def protoParsing(bh: Blackhole): Unit = {
    bh.consume(ScalarBenchmarkProtos.ScalarArrayUnpackedMessage.parser().parseFrom(scalarArrayBinary))
  }
}
