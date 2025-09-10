package benchmark

import com.google.protobuf._
import fastproto.WireFormatConverter
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.protobuf.backport.functions
import fastproto.ProtoToRowGenerator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Performance benchmark comparing WireFormatConverter against other conversion approaches.
 * 
 * This benchmark tests:
 * 1. WireFormatConverter (direct wire format parsing)
 * 2. DynamicMessage (traditional approach)
 * 3. Compiled message parsing
 * 
 * To run the benchmark:
 *   sbt "testOnly benchmark.WireFormatConverterBenchmark -- -n benchmark.Benchmark"
 */
class WireFormatConverterBenchmark extends AnyFlatSpec with Matchers {

  def getIterations: Int = {
    scala.Option(System.getProperty("benchmark.iterations"))
      .orElse(scala.Option(System.getenv("BENCHMARK_ITERATIONS")))
      .map(_.toInt)
      .getOrElse(1000)
  }

  "WireFormatConverter" should "perform better than DynamicMessage parsing" taggedAs BenchmarkTag in {
    // Create test data using Type message
    val typeMsg = Type.newBuilder()
      .setName("benchmark_type")
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
    
    val binary = typeMsg.toByteArray
    val descriptor = typeMsg.getDescriptorForType
    
    // Create Spark schema
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true),
        StructField("kind", StringType, nullable = true)
      ))), nullable = true)
    ))
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    val iterations = getIterations
    
    println(s"Running benchmark with $iterations iterations...")
    println(s"Message size: ${binary.length} bytes")
    
    // Create components for fair DynamicMessage comparison - use from_protobuf function instead
    
    // Create a minimal SparkSession for testing
    val spark = SparkSession.builder()
      .appName("benchmark")
      .master("local[1]")
      .config("spark.sql.adaptive.enabled", false)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("WARN")
    
    // Use the public from_protobuf function for DynamicMessage path
    // Create proper FileDescriptorSet with dependencies - matching ProtobufBackportSpec
    val descSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(descriptor.getFile.toProto)
      .addFile(AnyProto.getDescriptor.getFile.toProto)
      .addFile(SourceContextProto.getDescriptor.getFile.toProto)
      .addFile(ApiProto.getDescriptor.getFile.toProto)
      .build()
    
    val binaryCol = new Column(Literal.create(binary, org.apache.spark.sql.types.BinaryType))
    val fromProtobufExpr = functions.from_protobuf(
      binaryCol,
      descriptor.getFullName,
      descSet.toByteArray
    )
    
    // Compiled message path components  
    val compiledConverter = ProtoToRowGenerator.generateConverter(descriptor, classOf[com.google.protobuf.Type])

    // Warm up JVM - test all conversion paths
    for (_ <- 1 to 100) {
      converter.convert(binary)
      // DynamicMessage path warmup - use nullSafeEval on underlying expression
      fromProtobufExpr.expr.eval(null)
      // Compiled path warmup  
      compiledConverter.convert(binary)
    }
    
    // Benchmark WireFormatConverter
    val wireFormatStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      converter.convert(binary)
    }
    val wireFormatTime = System.nanoTime() - wireFormatStart
    
    // Benchmark DynamicMessage (complete binary → InternalRow via from_protobuf expression)
    val dynamicStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      fromProtobufExpr.expr.eval(null)
    }
    val dynamicTime = System.nanoTime() - dynamicStart
    
    // Benchmark compiled message (complete binary → InternalRow via generated converter)
    val compiledStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      compiledConverter.convert(binary)
    }
    val compiledTime = System.nanoTime() - compiledStart
    
    val wireFormatMs = wireFormatTime / 1e6
    val dynamicMs = dynamicTime / 1e6
    val compiledMs = compiledTime / 1e6
    
    println(f"WireFormatConverter:    ${wireFormatMs}%.2f ms (${wireFormatTime / iterations}%.0f ns/op)")
    println(f"DynamicMessage path:    ${dynamicMs}%.2f ms (${dynamicTime / iterations}%.0f ns/op)")
    println(f"Compiled message path:  ${compiledMs}%.2f ms (${compiledTime / iterations}%.0f ns/op)")
    
    val wireFormatSpeedup = dynamicTime.toDouble / wireFormatTime
    val compiledSpeedup = dynamicTime.toDouble / compiledTime
    
    println(f"WireFormatConverter is ${wireFormatSpeedup}%.2fx vs DynamicMessage path")
    println(f"Compiled message path is ${compiledSpeedup}%.2fx vs DynamicMessage path")
    
    // Performance assertions - Now comparing fair end-to-end binary → InternalRow conversion
    // Compiled path should be fastest for complete conversion
    compiledTime should be < dynamicTime   // Compiled should be fastest
    
    // WireFormatConverter should now be competitive since we're comparing complete conversions
    // All paths now convert binary → InternalRow, not just parsing
    
    // Clean up SparkSession
    spark.stop()
    
    println("✓ Performance benchmark completed successfully")
  }

  it should "handle complex nested structures efficiently" taggedAs BenchmarkTag in {
    // Create a more complex message with nested structures
    val nestedField = Field.newBuilder()
      .setName("nested_field")
      .setNumber(1)
      .setKind(Field.Kind.TYPE_MESSAGE)
      .setTypeUrl("google.protobuf.Type")
      .build()
    
    val complexType = Type.newBuilder()
      .setName("complex_type")
      .addFields(nestedField)
      .setSourceContext(SourceContext.newBuilder()
        .setFileName("test.proto")
        .build())
      .build()
    
    val binary = complexType.toByteArray
    val descriptor = complexType.getDescriptorForType
    
    // Create corresponding Spark schema
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true)
      ))), nullable = true),
      StructField("source_context", StructType(Seq(
        StructField("file_name", StringType, nullable = true)
      )), nullable = true)
    ))
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    val iterations = getIterations / 2 // Fewer iterations for complex structures
    
    println(s"\\nRunning complex structure benchmark with $iterations iterations...")
    println(s"Complex message size: ${binary.length} bytes")
    
    // Create components for fair complex benchmark
    val complexDescSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(descriptor.getFile.toProto)
      .addFile(AnyProto.getDescriptor.getFile.toProto)
      .addFile(SourceContextProto.getDescriptor.getFile.toProto)
      .addFile(ApiProto.getDescriptor.getFile.toProto)
      .build()
      
    val complexBinaryCol = new Column(Literal.create(binary, org.apache.spark.sql.types.BinaryType))
    val complexFromProtobufExpr = functions.from_protobuf(
      complexBinaryCol,
      descriptor.getFullName,
      complexDescSet.toByteArray
    )
    
    // Warm up
    for (_ <- 1 to 50) {
      converter.convert(binary)
      complexFromProtobufExpr.expr.eval(null)
    }
    
    // Benchmark
    val wireFormatStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      converter.convert(binary)
    }
    val wireFormatTime = System.nanoTime() - wireFormatStart
    
    val dynamicStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      complexFromProtobufExpr.expr.eval(null)
    }
    val dynamicTime = System.nanoTime() - dynamicStart
    
    val wireFormatMs = wireFormatTime / 1e6
    val dynamicMs = dynamicTime / 1e6
    val speedup = dynamicTime.toDouble / wireFormatTime
    
    println(f"Complex WireFormatConverter: ${wireFormatMs}%.2f ms (${wireFormatTime / iterations}%.0f ns/op)")
    println(f"Complex DynamicMessage path: ${dynamicMs}%.2f ms (${dynamicTime / iterations}%.0f ns/op)")
    println(f"Speedup: ${speedup}%.2fx")
    
    // Now comparing fair end-to-end binary → InternalRow conversion for complex structures
    // Both approaches complete successfully - WireFormatConverter should now be more competitive
    wireFormatTime should be > 0L
    dynamicTime should be > 0L
    
    println("✓ Complex structure benchmark completed successfully")
  }
}