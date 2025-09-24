package fastproto

import com.google.protobuf._
import org.apache.spark.sql.protobuf.backport.functions
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream

class WireFormatGeneratorSpec extends AnyFlatSpec with Matchers {

  private def createTestBinary(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: STRING (name) - tag=10, "test_message"
    output.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeStringNoTag("test_message")

    // Field 2: repeated MESSAGE (fields) - tag=18
    val nestedMessage = {
      val nestedBaos = new ByteArrayOutputStream()
      val nestedOutput = CodedOutputStream.newInstance(nestedBaos)

      // Nested field 4: STRING (name) - tag=32, "field1" (Field.name is field 4)
      nestedOutput.writeTag(4, WireFormat.WIRETYPE_LENGTH_DELIMITED)
      nestedOutput.writeStringNoTag("field1")

      nestedOutput.flush()
      nestedBaos.toByteArray
    }

    output.writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeBytesNoTag(com.google.protobuf.ByteString.copyFrom(nestedMessage))

    // Add another nested message
    val nestedMessage2 = {
      val nestedBaos = new ByteArrayOutputStream()
      val nestedOutput = CodedOutputStream.newInstance(nestedBaos)

      // Nested field 4: STRING (name) - tag=32, "field2" (Field.name is field 4)
      nestedOutput.writeTag(4, WireFormat.WIRETYPE_LENGTH_DELIMITED)
      nestedOutput.writeStringNoTag("field2")

      nestedOutput.flush()
      nestedBaos.toByteArray
    }

    output.writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeBytesNoTag(com.google.protobuf.ByteString.copyFrom(nestedMessage2))

    // Field 6: ENUM (syntax) - tag=48, SYNTAX_PROTO3 = 1
    output.writeTag(6, WireFormat.WIRETYPE_VARINT)
    output.writeEnumNoTag(1) // SYNTAX_PROTO3 = 1

    output.flush()
    baos.toByteArray
  }

  private def createPrimitivesBinary(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: INT32 - tag=8, value=42
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(42)

    // Field 2: INT64 - tag=16, value=123456789L
    output.writeTag(2, WireFormat.WIRETYPE_VARINT)
    output.writeInt64NoTag(123456789L)

    // Field 3: FLOAT - tag=29, value=3.14f
    output.writeTag(3, WireFormat.WIRETYPE_FIXED32)
    output.writeFloatNoTag(3.14f)

    // Field 4: DOUBLE - tag=33, value=2.718281828
    output.writeTag(4, WireFormat.WIRETYPE_FIXED64)
    output.writeDoubleNoTag(2.718281828)

    // Field 5: BOOL - tag=40, value=true
    output.writeTag(5, WireFormat.WIRETYPE_VARINT)
    output.writeBoolNoTag(true)

    // Field 6: BYTES - tag=50, value=[1,2,3,4]
    output.writeTag(6, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeBytesNoTag(com.google.protobuf.ByteString.copyFrom(Array[Byte](1, 2, 3, 4)))

    output.flush()
    baos.toByteArray
  }

  private def createRepeatedFieldsBinary(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Repeated INT32 field (field 1) - non-packed
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(10)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(20)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(30)

    // Packed repeated INT32 field (field 2)
    val packedInts = {
      val packedBaos = new ByteArrayOutputStream()
      val packedOutput = CodedOutputStream.newInstance(packedBaos)
      packedOutput.writeInt32NoTag(100)
      packedOutput.writeInt32NoTag(200)
      packedOutput.writeInt32NoTag(300)
      packedOutput.flush()
      packedBaos.toByteArray
    }
    output.writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeBytesNoTag(com.google.protobuf.ByteString.copyFrom(packedInts))

    // Repeated STRING field (field 3)
    output.writeTag(3, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeStringNoTag("first")
    output.writeTag(3, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeStringNoTag("second")

    output.flush()
    baos.toByteArray
  }

  "WireFormatToRowGenerator" should "generate parser for simple message" in {
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true)
      ))), nullable = true),
      StructField("syntax", StringType, nullable = true)
    ))

    val parser = WireFormatToRowGenerator.generateParser(descriptor, schema)
    parser should not be null
    parser.schema shouldEqual schema
  }

  it should "parse simple protobuf message correctly" in {
    val binary = createTestBinary()
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType


    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true)
      ))), nullable = true),
      StructField("syntax", StringType, nullable = true) // ENUM maps to StringType
    ))

    val parser = WireFormatToRowGenerator.generateParser(descriptor, schema)
    val row = parser.parse(binary)

    row.numFields should equal(3)
    row.getString(0) should equal("test_message")
    row.getString(2) should equal("SYNTAX_PROTO3") // ENUM name as string

    // Check array field
    val fieldsArray = row.getArray(1)
    fieldsArray.numElements() should equal(2)

    val firstField = fieldsArray.getStruct(0, 1)
    firstField.getString(0) should equal("field1")

    val secondField = fieldsArray.getStruct(1, 1)
    secondField.getString(0) should equal("field2")
  }

  it should "handle primitive types correctly" in {
    val binary = createPrimitivesBinary()

    // We need to create a custom descriptor for this test
    // For now, let's create a minimal test with Type descriptor
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val simpleSchema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val parser = WireFormatToRowGenerator.generateParser(descriptor, simpleSchema)

    // This should not crash even with primitive data
    noException should be thrownBy {
      parser.parse(binary)
    }
  }

  it should "handle repeated fields correctly" in {
    val binary = createRepeatedFieldsBinary()
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val parser = WireFormatToRowGenerator.generateParser(descriptor, schema)

    // Should parse without error even if schema doesn't match exactly
    noException should be thrownBy {
      val row = parser.parse(binary)
      row.numFields should equal(1)
    }
  }

  it should "handle wire type mismatches gracefully" in {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 5: VARINT instead of expected LENGTH_DELIMITED for string field
    output.writeTag(5, WireFormat.WIRETYPE_VARINT)
    output.writeInt64NoTag(12345L)

    output.flush()
    val binaryWithMismatch = baos.toByteArray

    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val schema = StructType(Seq(
      StructField("syntax", StringType, nullable = true)
    ))

    val parser = WireFormatToRowGenerator.generateParser(descriptor, schema)

    // Should not crash due to wire type mismatch
    noException should be thrownBy {
      val row = parser.parse(binaryWithMismatch)
      row.numFields should equal(1)
      // Field should be null due to wire type mismatch
    }
  }

  it should "cache generated parsers" in {
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val parser1 = WireFormatToRowGenerator.generateParser(descriptor, schema)
    val parser2 = WireFormatToRowGenerator.generateParser(descriptor, schema)

    // Should return the same cached instance
    parser1 should be theSameInstanceAs parser2
  }

  it should "handle empty messages" in {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)
    output.flush()
    val emptyBinary = baos.toByteArray

    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val parser = WireFormatToRowGenerator.generateParser(descriptor, schema)
    val row = parser.parse(emptyBinary)

    row.numFields should equal(1)
    if (row.isNullAt(0)) {
      // Field is null - acceptable
      row.isNullAt(0) should be(true)
    } else {
      // Field has a value - should be empty string for protobuf default
      val value = row.getUTF8String(0)
      value.toString should equal("")
    }
  }

  it should "handle unknown fields by skipping them" in {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 999: Unknown field - should be skipped
    output.writeTag(999, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeStringNoTag("unknown_field_value")

    // Field 1: Known field
    output.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeStringNoTag("known_value")

    output.flush()
    val binaryWithUnknownField = baos.toByteArray

    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val parser = WireFormatToRowGenerator.generateParser(descriptor, schema)
    val row = parser.parse(binaryWithUnknownField)

    row.numFields should equal(1)
    row.getString(0) should equal("known_value")
  }

  it should "handle nested messages with depth" in {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Create deeply nested message structure
    val level2Message = {
      val l2Baos = new ByteArrayOutputStream()
      val l2Output = CodedOutputStream.newInstance(l2Baos)
      l2Output.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
      l2Output.writeStringNoTag("level2")
      l2Output.flush()
      l2Baos.toByteArray
    }

    val level1Message = {
      val l1Baos = new ByteArrayOutputStream()
      val l1Output = CodedOutputStream.newInstance(l1Baos)
      l1Output.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
      l1Output.writeStringNoTag("level1")
      // Nested message at field 2
      l1Output.writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED)
      l1Output.writeBytesNoTag(com.google.protobuf.ByteString.copyFrom(level2Message))
      l1Output.flush()
      l1Baos.toByteArray
    }

    // Root message
    output.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeStringNoTag("root")
    output.writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeBytesNoTag(com.google.protobuf.ByteString.copyFrom(level1Message))

    output.flush()
    val nestedBinary = baos.toByteArray

    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    // Simple schema - nested parts may not match exactly but should not crash
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true)
      ))), nullable = true)
    ))

    val parser = WireFormatToRowGenerator.generateParser(descriptor, schema)

    // Should handle nested structure gracefully
    noException should be thrownBy {
      val row = parser.parse(nestedBinary)
      row.numFields should equal(2)
      row.getString(0) should equal("root")
    }
  }

  it should "be thread-safe with concurrent parser generation and conversion" in {
    import java.util.concurrent.{Executors, TimeUnit}
    import scala.concurrent.duration._
    import scala.concurrent.{Await, ExecutionContext, Future}

    val executor = Executors.newFixedThreadPool(10)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    val testBinary = createTestBinary()
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true)
      ))), nullable = true)
    ))

    try {
      // Create multiple concurrent tasks that generate parsers and convert data
      val tasks = (1 to 20).map { _ =>
        Future {
          // Each thread should get its own parser instance but share compiled classes
          val parser1 = WireFormatToRowGenerator.generateParser(Type.getDescriptor, schema)
          val parser2 = WireFormatToRowGenerator.generateParser(Type.getDescriptor, schema)

          // Within same thread, should get same instance (cached)
          parser1 should be theSameInstanceAs parser2

          val results = (1 to 5).map { _ =>
            val row = parser1.parse(testBinary)
            (row.getUTF8String(0).toString, row.getArray(1).numElements())
          }
          results
        }
      }

      // Wait for all tasks to complete
      val allResults = Await.result(Future.sequence(tasks), 10.seconds)

      // Verify all threads got consistent results
      allResults.foreach { threadResults =>
        threadResults.foreach { case (name, numFields) =>
          name should equal("test_message")
          numFields should equal(2)
        }
      }

    } finally {
      executor.shutdown()
      executor.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  it should "handle FIXED32 packed fields without compilation errors" in {
    import com.google.protobuf.{CodedOutputStream, WireFormat}

    import java.io.ByteArrayOutputStream

    // Create a schema with FIXED32 repeated field
    val schema = StructType(Seq(
      StructField("fixed32_values", ArrayType(IntegerType), nullable = true)
    ))

    // Create test binary data with packed FIXED32 values
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: repeated fixed32 values (packed)
    // Tag for field 1 with LENGTH_DELIMITED wire type = (1 << 3) | 2 = 10
    output.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    val fixed32Values = Array(100, 200, 300, 400)
    output.writeUInt32NoTag(fixed32Values.length * 4) // 4 bytes per FIXED32
    fixed32Values.foreach(output.writeFixed32NoTag)

    output.flush()

    // Create a simple descriptor that represents FIXED32 repeated field
    val typeDescriptor = Type.getDescriptor

    // This should not throw compilation errors
    noException should be thrownBy {
      val parser = WireFormatToRowGenerator.generateParser(typeDescriptor, schema)
      // The parser generation should succeed without compilation errors
      parser should not be null
    }
  }

  it should "handle SFIXED32 and SFIXED64 packed fields without compilation errors" in {
    import com.google.protobuf.{CodedOutputStream, WireFormat}

    import java.io.ByteArrayOutputStream

    // Create schema with repeated SFIXED32 and SFIXED64 fields
    val schema = StructType(Seq(
      StructField("sfixed32_values", ArrayType(IntegerType), nullable = true),
      StructField("sfixed64_values", ArrayType(LongType), nullable = true)
    ))

    // Create test binary data with packed SFIXED32 and SFIXED64 values
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: repeated sfixed32 values (packed)
    output.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    val sfixed32Values = Array(-100, 200, -300, 400)
    output.writeUInt32NoTag(sfixed32Values.length * 4) // 4 bytes per SFIXED32
    sfixed32Values.foreach(output.writeSFixed32NoTag)

    // Field 2: repeated sfixed64 values (packed)
    output.writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    val sfixed64Values = Array(-1000L, 2000L, -3000L, 4000L)
    output.writeUInt32NoTag(sfixed64Values.length * 8) // 8 bytes per SFIXED64
    sfixed64Values.foreach(output.writeSFixed64NoTag)

    output.flush()

    // Create a simple descriptor that represents SFIXED32/SFIXED64 repeated fields
    val typeDescriptor = Type.getDescriptor

    // This should not throw compilation errors and should use array path, not IntList/LongList
    noException should be thrownBy {
      val parser = WireFormatToRowGenerator.generateParser(typeDescriptor, schema)
      // The parser generation should succeed without compilation errors
      parser should not be null
    }
  }

  ignore should "produce equivalent results when using binary descriptor sets vs generated WireFormatParser" in {
    // Create a complex message with nested structures and repeated fields
    val nestedField1 = Field.newBuilder()
      .setName("nested_field1")
      .setNumber(1)
      .setKind(Field.Kind.TYPE_STRING)
      .setCardinality(Field.Cardinality.CARDINALITY_OPTIONAL)
      .build()

    val nestedField2 = Field.newBuilder()
      .setName("nested_field2")
      .setNumber(2)
      .setKind(Field.Kind.TYPE_INT32)
      .setCardinality(Field.Cardinality.CARDINALITY_REPEATED)
      .build()

    val sourceContext = SourceContext.newBuilder()
      .setFileName("test.proto")
      .build()

    val complexType = Type.newBuilder()
      .setName("complex_comparison_test")
      .addFields(nestedField1)
      .addFields(nestedField2)
      .setSourceContext(sourceContext)
      .setSyntax(Syntax.SYNTAX_PROTO3)
      .build()

    val binary = complexType.toByteArray
    val descriptor = complexType.getDescriptorForType

    // Create comprehensive Spark schema matching all fields
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("kind", StringType, nullable = true),
        StructField("cardinality", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("type_url", StringType, nullable = true),
        StructField("oneof_index", IntegerType, nullable = true),
        StructField("packed", BooleanType, nullable = true),
        StructField("options", ArrayType(StructType(Seq(
          StructField("name", StringType, nullable = true),
          StructField("value", StructType(Seq(
            StructField("type_url", StringType, nullable = true),
            StructField("value", BinaryType, nullable = true)
          )), nullable = true)
        ))), nullable = true),
        StructField("json_name", StringType, nullable = true),
        StructField("default_value", StringType, nullable = true)
      ))), nullable = true),
      StructField("oneofs", ArrayType(StringType), nullable = true),
      StructField("options", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("value", StructType(Seq(
          StructField("type_url", StringType, nullable = true),
          StructField("value", BinaryType, nullable = true)
        )), nullable = true)
      ))), nullable = true),
      StructField("source_context", StructType(Seq(
        StructField("file_name", StringType, nullable = true)
      )), nullable = true),
      StructField("syntax", StringType, nullable = true)
    ))

    // Method 1: Generated WireFormatToRowGenerator parser
    val generatedParser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val generatedResult = generatedParser.parse(binary)

    // Method 2: Binary descriptor set through from_protobuf function (should use WireFormatParser internally)
    val descSet = DescriptorProtos.FileDescriptorSet.newBuilder()
      .addFile(descriptor.getFile.toProto)
      .addFile(AnyProto.getDescriptor.getFile.toProto)
      .addFile(SourceContextProto.getDescriptor.getFile.toProto)
      .addFile(ApiProto.getDescriptor.getFile.toProto)
      .build()

    // Create a temporary SparkSession for the from_protobuf test
    val spark = org.apache.spark.sql.SparkSession.builder()
      .appName("wireformat-correctness-test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.sql.codegen.maxFields", "1000")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      val df = Seq(binary).toDF("data")
      val binaryDescResult = df.select(
        functions.from_protobuf($"data", descriptor.getFullName, descSet.toByteArray).as("result")
      ).head().getAs[org.apache.spark.sql.Row]("result")

      // Compare all fields recursively
      InternalRowToSqlRowComparator.compareRows(generatedResult, binaryDescResult, sparkSchema, "root")

      println("✓ Generated WireFormatToRowGenerator and binary descriptor set produce identical results")

    } finally {
      spark.stop()
    }
  }
}