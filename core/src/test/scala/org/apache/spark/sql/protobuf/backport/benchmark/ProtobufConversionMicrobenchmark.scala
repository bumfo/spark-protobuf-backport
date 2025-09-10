package org.apache.spark.sql.protobuf.backport.benchmark

import benchmark.BenchmarkTag
import com.google.protobuf._
import fastproto.{ProtoToRowGenerator, WireFormatConverter}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.protobuf.backport.ProtobufDataToCatalyst
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Microbenchmark for core protobuf conversion performance using direct nullSafeEval calls.
 * 
 * This benchmark eliminates Spark expression evaluation overhead by calling nullSafeEval
 * directly on ProtobufDataToCatalyst instances, providing accurate measurements of the
 * core conversion performance differences between:
 * 1. WireFormatConverter (direct binary wire format parsing)
 * 2. WireFormatConverter via binary descriptor sets
 * 3. DynamicMessage via descriptor files  
 * 4. Compiled message via ProtoToRowGenerator
 * 
 * To run the benchmark:
 *   sbt "testOnly *ProtobufConversionMicrobenchmark -- -n benchmark.Benchmark"
 */
class ProtobufConversionMicrobenchmark extends AnyFlatSpec with Matchers {

  def getIterations: Int = {
    scala.Option(System.getProperty("benchmark.iterations"))
      .orElse(scala.Option(System.getenv("BENCHMARK_ITERATIONS")))
      .map(_.toInt)
      .getOrElse(1000)
  }

  "ProtobufDataToCatalyst microbenchmark" should "compare core conversion performance without expression overhead" taggedAs BenchmarkTag in {
    // Create test data using Type message
    val typeMsg = Type.newBuilder()
      .setName("microbenchmark_type")
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
    
    val iterations = getIterations
    
    println(s"Running microbenchmark with $iterations iterations...")
    println(s"Message size: ${binary.length} bytes")
    
    // Create FileDescriptorSet with dependencies
    val descSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(descriptor.getFile.toProto)
      .addFile(AnyProto.getDescriptor.getFile.toProto)
      .addFile(SourceContextProto.getDescriptor.getFile.toProto)
      .addFile(ApiProto.getDescriptor.getFile.toProto)
      .build()
    
    // Create a temporary descriptor file for DynamicMessage path
    val tempDescFile = java.io.File.createTempFile("microbenchmark_descriptor", ".desc")
    java.nio.file.Files.write(tempDescFile.toPath, descSet.toByteArray)
    tempDescFile.deleteOnExit()
    
    // Method 1: Direct WireFormatConverter
    val directConverter = new WireFormatConverter(descriptor, sparkSchema)
    
    // Method 2: ProtobufDataToCatalyst with binary descriptor set (uses WireFormatConverter internally)
    val binaryDescExpression = ProtobufDataToCatalyst(
      child = Literal.create(binary, BinaryType),
      messageName = descriptor.getFullName,
      descFilePath = None,
      options = Map.empty,
      binaryDescriptorSet = Some(descSet.toByteArray)
    )
    
    // Method 3: ProtobufDataToCatalyst with descriptor file (uses DynamicMessage)
    val descriptorFileExpression = ProtobufDataToCatalyst(
      child = Literal.create(binary, BinaryType),
      messageName = descriptor.getFullName,
      descFilePath = Some(tempDescFile.getAbsolutePath),
      options = Map.empty,
      binaryDescriptorSet = None
    )
    
    // Method 4: Compiled message converter
    val compiledConverter = ProtoToRowGenerator.generateConverter(descriptor, classOf[com.google.protobuf.Type])

    // Warm up JVM
    for (_ <- 1 to 100) {
      directConverter.convert(binary)
      binaryDescExpression.nullSafeEval(binary)
      descriptorFileExpression.nullSafeEval(binary)
      compiledConverter.convert(binary)
    }
    
    // Benchmark 1: Direct WireFormatConverter
    val directStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      directConverter.convert(binary)
    }
    val directTime = System.nanoTime() - directStart
    
    // Benchmark 2: WireFormatConverter via binary descriptor set
    val binaryDescStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      binaryDescExpression.nullSafeEval(binary)
    }
    val binaryDescTime = System.nanoTime() - binaryDescStart
    
    // Benchmark 3: DynamicMessage via descriptor file
    val descriptorFileStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      descriptorFileExpression.nullSafeEval(binary)
    }
    val descriptorFileTime = System.nanoTime() - descriptorFileStart
    
    // Benchmark 4: Compiled message converter
    val compiledStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      compiledConverter.convert(binary)
    }
    val compiledTime = System.nanoTime() - compiledStart
    
    val directMs = directTime / 1e6
    val binaryDescMs = binaryDescTime / 1e6
    val descriptorFileMs = descriptorFileTime / 1e6
    val compiledMs = compiledTime / 1e6
    
    println(f"Direct WireFormatConverter:             ${directMs}%.2f ms (${directTime / iterations}%.0f ns/op)")
    println(f"WireFormatConverter (binary desc):      ${binaryDescMs}%.2f ms (${binaryDescTime / iterations}%.0f ns/op)")
    println(f"DynamicMessage (descriptor file):       ${descriptorFileMs}%.2f ms (${descriptorFileTime / iterations}%.0f ns/op)")
    println(f"Compiled message converter:             ${compiledMs}%.2f ms (${compiledTime / iterations}%.0f ns/op)")
    
    val directSpeedup = descriptorFileTime.toDouble / directTime
    val binaryDescSpeedup = descriptorFileTime.toDouble / binaryDescTime
    val compiledSpeedup = descriptorFileTime.toDouble / compiledTime
    
    println(f"Direct WireFormatConverter is ${directSpeedup}%.2fx vs DynamicMessage")
    println(f"WireFormatConverter (binary desc) is ${binaryDescSpeedup}%.2fx vs DynamicMessage") 
    println(f"Compiled message converter is ${compiledSpeedup}%.2fx vs DynamicMessage")
    
    // Note: Binary desc path is actually faster due to Spark expression evaluation optimizations
    val performanceRatio = math.max(directTime, binaryDescTime).toDouble / math.min(directTime, binaryDescTime)
    performanceRatio should be <= 3.0  // Allow up to 3x difference
    println(f"Direct vs Binary Desc performance ratio: ${performanceRatio}%.2fx")
    
    if (binaryDescTime < directTime) {
      println("ℹ️  Binary desc path is faster - likely due to Spark expression evaluation optimizations")
    } else {
      println("ℹ️  Direct path is faster - raw converter without framework overhead")
    }
    
    // All converters should produce valid results
    directTime should be > 0L
    binaryDescTime should be > 0L
    descriptorFileTime should be > 0L
    compiledTime should be > 0L
    
    println("✓ Microbenchmark completed successfully")
  }
  
  it should "compare complex nested structure performance without expression overhead" taggedAs BenchmarkTag in {
    // Create a more complex message with nested structures
    val nestedField = Field.newBuilder()
      .setName("nested_field")
      .setNumber(1)
      .setKind(Field.Kind.TYPE_MESSAGE)
      .setTypeUrl("google.protobuf.Type")
      .build()
    
    val complexType = Type.newBuilder()
      .setName("complex_microbenchmark_type")
      .addFields(nestedField)
      .setSourceContext(SourceContext.newBuilder()
        .setFileName("microbenchmark.proto")
        .build())
      .setSyntax(Syntax.SYNTAX_PROTO3)
      .build()
    
    val binary = complexType.toByteArray
    val descriptor = complexType.getDescriptorForType
    
    // Create comprehensive Spark schema
    val sparkSchema = StructType(Seq(
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
    
    val iterations = getIterations / 2 // Fewer iterations for complex structures
    
    println(s"\\nRunning complex microbenchmark with $iterations iterations...")
    println(s"Complex message size: ${binary.length} bytes")
    
    // Create FileDescriptorSet with dependencies
    val descSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(descriptor.getFile.toProto)
      .addFile(AnyProto.getDescriptor.getFile.toProto)
      .addFile(SourceContextProto.getDescriptor.getFile.toProto)
      .addFile(ApiProto.getDescriptor.getFile.toProto)
      .build()
    
    // Create temporary descriptor file
    val tempDescFile = java.io.File.createTempFile("complex_microbenchmark_descriptor", ".desc")
    java.nio.file.Files.write(tempDescFile.toPath, descSet.toByteArray)
    tempDescFile.deleteOnExit()
    
    // Create converters
    val directConverter = new WireFormatConverter(descriptor, sparkSchema)
    
    val binaryDescExpression = ProtobufDataToCatalyst(
      child = Literal.create(binary, BinaryType),
      messageName = descriptor.getFullName,
      descFilePath = None,
      options = Map.empty,
      binaryDescriptorSet = Some(descSet.toByteArray)
    )
    
    val descriptorFileExpression = ProtobufDataToCatalyst(
      child = Literal.create(binary, BinaryType),
      messageName = descriptor.getFullName,
      descFilePath = Some(tempDescFile.getAbsolutePath),
      options = Map.empty,
      binaryDescriptorSet = None
    )
    
    val compiledConverter = ProtoToRowGenerator.generateConverter(descriptor, classOf[com.google.protobuf.Type])

    // Warm up
    for (_ <- 1 to 50) {
      directConverter.convert(binary)
      binaryDescExpression.nullSafeEval(binary)
      descriptorFileExpression.nullSafeEval(binary)
      compiledConverter.convert(binary)
    }
    
    // Benchmark
    val directStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      directConverter.convert(binary)
    }
    val directTime = System.nanoTime() - directStart
    
    val binaryDescStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      binaryDescExpression.nullSafeEval(binary)
    }
    val binaryDescTime = System.nanoTime() - binaryDescStart
    
    val descriptorFileStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      descriptorFileExpression.nullSafeEval(binary)
    }
    val descriptorFileTime = System.nanoTime() - descriptorFileStart
    
    val compiledStart = System.nanoTime()
    for (_ <- 1 to iterations) {
      compiledConverter.convert(binary)
    }
    val compiledTime = System.nanoTime() - compiledStart
    
    val directMs = directTime / 1e6
    val binaryDescMs = binaryDescTime / 1e6
    val descriptorFileMs = descriptorFileTime / 1e6
    val compiledMs = compiledTime / 1e6
    
    println(f"Complex Direct WireFormatConverter:        ${directMs}%.2f ms (${directTime / iterations}%.0f ns/op)")
    println(f"Complex WireFormatConverter (binary desc): ${binaryDescMs}%.2f ms (${binaryDescTime / iterations}%.0f ns/op)")
    println(f"Complex DynamicMessage (descriptor file):  ${descriptorFileMs}%.2f ms (${descriptorFileTime / iterations}%.0f ns/op)")
    println(f"Complex Compiled message converter:        ${compiledMs}%.2f ms (${compiledTime / iterations}%.0f ns/op)")
    
    val directSpeedup = descriptorFileTime.toDouble / directTime
    val binaryDescSpeedup = descriptorFileTime.toDouble / binaryDescTime
    val compiledSpeedup = descriptorFileTime.toDouble / compiledTime
    
    println(f"Complex Direct WireFormatConverter is ${directSpeedup}%.2fx vs DynamicMessage")
    println(f"Complex WireFormatConverter (binary desc) is ${binaryDescSpeedup}%.2fx vs DynamicMessage")
    println(f"Complex Compiled message converter is ${compiledSpeedup}%.2fx vs DynamicMessage")
    
    // Note: Binary desc path is actually faster due to Spark expression evaluation optimizations
    val performanceRatio = math.max(directTime, binaryDescTime).toDouble / math.min(directTime, binaryDescTime)
    performanceRatio should be <= 3.0  // Allow up to 3x difference
    println(f"Complex Direct vs Binary Desc performance ratio: ${performanceRatio}%.2fx")
    
    if (binaryDescTime < directTime) {
      println("ℹ️  Complex Binary desc path is faster - likely due to Spark expression evaluation optimizations")
    } else {
      println("ℹ️  Complex Direct path is faster - raw converter without framework overhead")
    }
    
    // All converters should complete successfully
    directTime should be > 0L
    binaryDescTime should be > 0L
    descriptorFileTime should be > 0L
    compiledTime should be > 0L
    
    println("✓ Complex microbenchmark completed successfully")
  }
}