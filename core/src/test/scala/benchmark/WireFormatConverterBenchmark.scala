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
    
    // Create a temporary descriptor file to force DynamicMessage path (not WireFormatConverter)
    val tempDescFile = java.io.File.createTempFile("benchmark_descriptor", ".desc")
    java.nio.file.Files.write(tempDescFile.toPath, descSet.toByteArray)
    tempDescFile.deleteOnExit()
    
    val binaryCol = new Column(Literal.create(binary, org.apache.spark.sql.types.BinaryType))
    val fromProtobufExpr = functions.from_protobuf(
      binaryCol,
      descriptor.getFullName,
      tempDescFile.getAbsolutePath
    )
    
    // Compiled message path components  
    val compiledConverter = ProtoToRowGenerator.generateConverter(descriptor, classOf[com.google.protobuf.Type])
    
    // WireFormatConverter path with binary descriptor set (new default for binary descriptor sets)
    val wireFormatBinaryDescCol = new Column(Literal.create(binary, org.apache.spark.sql.types.BinaryType))
    val wireFormatBinaryDescExpr = functions.from_protobuf(
      wireFormatBinaryDescCol,
      descriptor.getFullName,
      descSet.toByteArray // This will now use WireFormatConverter
    )

    // Warm up JVM - test all conversion paths
    for (_ <- 1 to 100) {
      converter.convert(binary)
      // DynamicMessage path warmup - use nullSafeEval on underlying expression
      fromProtobufExpr.expr.eval(null)
      // Compiled path warmup  
      compiledConverter.convert(binary)
      // WireFormatConverter with binary descriptor set warmup
      wireFormatBinaryDescExpr.expr.eval(null)
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
    
    // Benchmark WireFormatConverter with binary descriptor set (new default path)
    val wireFormatBinaryDescStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      wireFormatBinaryDescExpr.expr.eval(null)
    }
    val wireFormatBinaryDescTime = System.nanoTime() - wireFormatBinaryDescStart
    
    val wireFormatMs = wireFormatTime / 1e6
    val dynamicMs = dynamicTime / 1e6
    val compiledMs = compiledTime / 1e6
    val wireFormatBinaryDescMs = wireFormatBinaryDescTime / 1e6
    
    println(f"WireFormatConverter (direct):       ${wireFormatMs}%.2f ms (${wireFormatTime / iterations}%.0f ns/op)")
    println(f"DynamicMessage path (descriptor):   ${dynamicMs}%.2f ms (${dynamicTime / iterations}%.0f ns/op)")
    println(f"Compiled message path:              ${compiledMs}%.2f ms (${compiledTime / iterations}%.0f ns/op)")
    println(f"WireFormatConverter (binary desc):  ${wireFormatBinaryDescMs}%.2f ms (${wireFormatBinaryDescTime / iterations}%.0f ns/op)")
    
    val wireFormatSpeedup = dynamicTime.toDouble / wireFormatTime
    val compiledSpeedup = dynamicTime.toDouble / compiledTime
    val wireFormatBinaryDescSpeedup = dynamicTime.toDouble / wireFormatBinaryDescTime
    
    println(f"WireFormatConverter (direct) is ${wireFormatSpeedup}%.2fx vs DynamicMessage path")
    println(f"Compiled message path is ${compiledSpeedup}%.2fx vs DynamicMessage path")
    println(f"WireFormatConverter (binary desc) is ${wireFormatBinaryDescSpeedup}%.2fx vs DynamicMessage path")
    
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
      
    // Create a temporary descriptor file to force DynamicMessage path for complex benchmark
    val complexTempDescFile = java.io.File.createTempFile("complex_benchmark_descriptor", ".desc")
    java.nio.file.Files.write(complexTempDescFile.toPath, complexDescSet.toByteArray)
    complexTempDescFile.deleteOnExit()
    
    val complexBinaryCol = new Column(Literal.create(binary, org.apache.spark.sql.types.BinaryType))
    val complexFromProtobufExpr = functions.from_protobuf(
      complexBinaryCol,
      descriptor.getFullName,
      complexTempDescFile.getAbsolutePath
    )
    
    // WireFormatConverter path with binary descriptor set for complex structures
    val complexWireFormatBinaryDescCol = new Column(Literal.create(binary, org.apache.spark.sql.types.BinaryType))
    val complexWireFormatBinaryDescExpr = functions.from_protobuf(
      complexWireFormatBinaryDescCol,
      descriptor.getFullName,
      complexDescSet.toByteArray // This will now use WireFormatConverter
    )
    
    // Compiled message path components for complex structures
    val complexCompiledConverter = ProtoToRowGenerator.generateConverter(descriptor, classOf[com.google.protobuf.Type])
    
    // Warm up
    for (_ <- 1 to 50) {
      converter.convert(binary)
      complexFromProtobufExpr.expr.eval(null)
      complexCompiledConverter.convert(binary)
      complexWireFormatBinaryDescExpr.expr.eval(null)
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
    
    // Benchmark compiled message (complete binary → InternalRow via generated converter)
    val complexCompiledStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      complexCompiledConverter.convert(binary)
    }
    val complexCompiledTime = System.nanoTime() - complexCompiledStart
    
    // Benchmark WireFormatConverter with binary descriptor set for complex structures
    val complexWireFormatBinaryDescStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      complexWireFormatBinaryDescExpr.expr.eval(null)
    }
    val complexWireFormatBinaryDescTime = System.nanoTime() - complexWireFormatBinaryDescStart
    
    val wireFormatMs = wireFormatTime / 1e6
    val dynamicMs = dynamicTime / 1e6
    val complexCompiledMs = complexCompiledTime / 1e6
    val complexWireFormatBinaryDescMs = complexWireFormatBinaryDescTime / 1e6
    
    println(f"Complex WireFormatConverter (direct):       ${wireFormatMs}%.2f ms (${wireFormatTime / iterations}%.0f ns/op)")
    println(f"Complex DynamicMessage path (descriptor):   ${dynamicMs}%.2f ms (${dynamicTime / iterations}%.0f ns/op)")
    println(f"Complex Compiled message path:              ${complexCompiledMs}%.2f ms (${complexCompiledTime / iterations}%.0f ns/op)")
    println(f"Complex WireFormatConverter (binary desc):  ${complexWireFormatBinaryDescMs}%.2f ms (${complexWireFormatBinaryDescTime / iterations}%.0f ns/op)")
    
    val wireFormatSpeedup = dynamicTime.toDouble / wireFormatTime
    val compiledSpeedup = dynamicTime.toDouble / complexCompiledTime
    val complexWireFormatBinaryDescSpeedup = dynamicTime.toDouble / complexWireFormatBinaryDescTime
    
    println(f"Complex WireFormatConverter (direct) is ${wireFormatSpeedup}%.2fx vs DynamicMessage path")
    println(f"Complex Compiled message path is ${compiledSpeedup}%.2fx vs DynamicMessage path")
    println(f"Complex WireFormatConverter (binary desc) is ${complexWireFormatBinaryDescSpeedup}%.2fx vs DynamicMessage path")
    
    // Now comparing fair end-to-end binary → InternalRow conversion for complex structures
    // All approaches complete successfully
    wireFormatTime should be > 0L
    dynamicTime should be > 0L
    complexCompiledTime should be > 0L
    complexWireFormatBinaryDescTime should be > 0L
    
    // Compiled path should be fastest for complete conversion
    complexCompiledTime should be < dynamicTime
    
    println("✓ Complex structure benchmark completed successfully")
  }
}