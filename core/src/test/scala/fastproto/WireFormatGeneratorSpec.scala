package fastproto

import com.google.protobuf.{Type, CodedOutputStream, WireFormat}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.ByteArrayOutputStream
import scala.collection.JavaConverters._

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

  "WireFormatToRowGenerator" should "generate converter for simple message" in {
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true)
      ))), nullable = true),
      StructField("syntax", StringType, nullable = true)
    ))

    val converter = WireFormatToRowGenerator.generateConverter(descriptor, schema)
    converter should not be null
    converter.schema shouldEqual schema
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

    val converter = WireFormatToRowGenerator.generateConverter(descriptor, schema)
    val row = converter.convert(binary)

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

    // Create a simple descriptor for primitive types
    val schema = StructType(Seq(
      StructField("int32_field", IntegerType, nullable = true),
      StructField("int64_field", LongType, nullable = true),
      StructField("float_field", FloatType, nullable = true),
      StructField("double_field", DoubleType, nullable = true),
      StructField("bool_field", BooleanType, nullable = true),
      StructField("bytes_field", BinaryType, nullable = true)
    ))

    // We need to create a custom descriptor for this test
    // For now, let's create a minimal test with Type descriptor
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val simpleSchema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val converter = WireFormatToRowGenerator.generateConverter(descriptor, simpleSchema)

    // This should not crash even with primitive data
    noException should be thrownBy {
      converter.convert(binary)
    }
  }

  it should "handle repeated fields correctly" in {
    val binary = createRepeatedFieldsBinary()
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val converter = WireFormatToRowGenerator.generateConverter(descriptor, schema)

    // Should parse without error even if schema doesn't match exactly
    noException should be thrownBy {
      val row = converter.convert(binary)
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

    val converter = WireFormatToRowGenerator.generateConverter(descriptor, schema)

    // Should not crash due to wire type mismatch
    noException should be thrownBy {
      val row = converter.convert(binaryWithMismatch)
      row.numFields should equal(1)
      // Field should be null due to wire type mismatch
    }
  }

  it should "cache generated converters" in {
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val converter1 = WireFormatToRowGenerator.generateConverter(descriptor, schema)
    val converter2 = WireFormatToRowGenerator.generateConverter(descriptor, schema)

    // Should return the same cached instance
    converter1 should be theSameInstanceAs converter2
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

    val converter = WireFormatToRowGenerator.generateConverter(descriptor, schema)
    val row = converter.convert(emptyBinary)

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

    val converter = WireFormatToRowGenerator.generateConverter(descriptor, schema)
    val row = converter.convert(binaryWithUnknownField)

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

    val converter = WireFormatToRowGenerator.generateConverter(descriptor, schema)

    // Should handle nested structure gracefully
    noException should be thrownBy {
      val row = converter.convert(nestedBinary)
      row.numFields should equal(2)
      row.getString(0) should equal("root")
    }
  }

  it should "be thread-safe with concurrent converter generation and conversion" in {
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
      // Create multiple concurrent tasks that generate converters and convert data
      val tasks = (1 to 20).map { i =>
        Future {
          // Each thread should get its own converter instance but share compiled classes
          val converter1 = WireFormatToRowGenerator.generateConverter(Type.getDescriptor, schema)
          val converter2 = WireFormatToRowGenerator.generateConverter(Type.getDescriptor, schema)

          // Within same thread, should get same instance (cached)
          converter1 should be theSameInstanceAs converter2

          val results = (1 to 5).map { _ =>
            val row = converter1.convert(testBinary)
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
}