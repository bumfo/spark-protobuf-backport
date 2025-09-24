package fastproto

import com.google.protobuf._
import org.apache.spark.sql.types._
import org.apache.spark.sql.protobuf.backport.DynamicMessageParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream

/**
 * Comprehensive test suite for null handling in protobuf parsers.
 *
 * This test verifies that absent protobuf fields are correctly marked as null in Spark rows,
 * and that the null bit logic has been fixed to prevent unsafe memory access errors.
 */
class NullabilitySpec extends AnyFlatSpec with Matchers with InternalRowMatchers {

  // Create a test message with some fields present and others absent
  private def createPartialMessage(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: string field - present
    output.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeStringNoTag("present_string")

    // Field 2: int32 field - ABSENT (this should be null in result)
    // (Field 2 is intentionally not written)

    // Field 3: boolean field - present
    output.writeTag(3, WireFormat.WIRETYPE_VARINT)
    output.writeBoolNoTag(true)

    // Field 4: double field - ABSENT (this should be null in result)
    // (Field 4 is intentionally not written)

    // Field 5: nested message field - present but empty
    output.writeTag(5, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    val nestedData = {
      val nestedBaos = new ByteArrayOutputStream()
      val nestedOutput = CodedOutputStream.newInstance(nestedBaos)

      // Write nested field 1 - present
      nestedOutput.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
      nestedOutput.writeStringNoTag("nested_value")

      // Nested field 2 - ABSENT (should be null)
      // (Nested field 2 is intentionally not written)

      nestedOutput.flush()
      nestedBaos.toByteArray
    }
    output.writeBytesNoTag(ByteString.copyFrom(nestedData))

    // Field 6: repeated field - ABSENT (should be empty array, not null)
    // (Field 6 is intentionally not written)

    output.flush()
    baos.toByteArray
  }

  // Create a completely empty message (all fields absent)
  private def createEmptyMessage(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)
    // Write no fields at all
    output.flush()
    baos.toByteArray
  }

  "WireFormatParser" should "correctly handle null values for absent fields" in {
    val binary = createPartialMessage()

    // Define schema matching our test message structure
    val nestedSchema = StructType(Seq(
      StructField("nested_field1", StringType, nullable = true),   // present
      StructField("nested_field2", IntegerType, nullable = true)   // absent -> null
    ))

    val mainSchema = StructType(Seq(
      StructField("string_field", StringType, nullable = true),    // present
      StructField("int_field", IntegerType, nullable = true),      // absent -> null
      StructField("bool_field", BooleanType, nullable = true),     // present
      StructField("double_field", DoubleType, nullable = true),    // absent -> null
      StructField("nested_field", nestedSchema, nullable = true),  // present with partial data
      StructField("repeated_field", ArrayType(StringType), nullable = true) // absent -> empty array
    ))

    // Create descriptor for our test message structure
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder()
      .setName("TestMessage")
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("string_field")
        .setNumber(1)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("int_field")
        .setNumber(2)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("bool_field")
        .setNumber(3)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("double_field")
        .setNumber(4)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("nested_field")
        .setNumber(5)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
        .setTypeName(".NestedMessage")
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("repeated_field")
        .setNumber(6)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
      .build()

    val nestedDescriptorProto = DescriptorProtos.DescriptorProto.newBuilder()
      .setName("NestedMessage")
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("nested_field1")
        .setNumber(1)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("nested_field2")
        .setNumber(2)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .build()

    val fileDescriptorProto = DescriptorProtos.FileDescriptorProto.newBuilder()
      .setName("test.proto")
      .addMessageType(descriptorProto)
      .addMessageType(nestedDescriptorProto)
      .build()

    val fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, Array.empty)
    val descriptor = fileDescriptor.findMessageTypeByName("TestMessage")

    val parser = new WireFormatParser(descriptor, mainSchema)

    // This should not crash with unsafe memory access errors
    val row = parser.parse(binary)

    // Verify structure
    row.numFields should equal(6)

    // Field 1: present string field
    row.field(0, "string_field") shouldEqual "present_string"

    // Field 2: absent int field - should be null
    row.isNullAt(1) should be(true)

    // Field 3: present boolean field
    row.isNullAt(2) should be(false)
    row.getBoolean(2) should be(true)

    // Field 4: absent double field - should be null
    row.isNullAt(3) should be(true)

    // Field 5: present nested message (but with some absent nested fields)
    row.isNullAt(4) should be(false)
    val nestedRow = row.getStruct(4, 2)

    // Nested field 1: present
    nestedRow.field(0, "nested_field1") shouldEqual "nested_value"

    // Nested field 2: absent - should be null
    nestedRow.isNullAt(1) should be(true)

    // Field 6: absent repeated field - should be null (empty array would be non-null with 0 elements)
    row.isNullAt(5) should be(true)
  }

  "WireFormatParser" should "handle completely empty messages without crashing" in {
    val binary = createEmptyMessage()

    val schema = StructType(Seq(
      StructField("field1", StringType, nullable = true),
      StructField("field2", IntegerType, nullable = true),
      StructField("field3", BooleanType, nullable = true)
    ))

    // Create a simple descriptor with 3 optional fields
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder()
      .setName("EmptyTestMessage")
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("field1")
        .setNumber(1)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("field2")
        .setNumber(2)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("field3")
        .setNumber(3)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .build()

    val fileDescriptorProto = DescriptorProtos.FileDescriptorProto.newBuilder()
      .setName("empty_test.proto")
      .addMessageType(descriptorProto)
      .build()

    val fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, Array.empty)
    val descriptor = fileDescriptor.findMessageTypeByName("EmptyTestMessage")

    val parser = new WireFormatParser(descriptor, schema)

    // This should not crash with unsafe memory access errors
    val row = parser.parse(binary)

    // All fields should be null since none were present in the protobuf
    row.numFields should equal(3)
    row.isNullAt(0) should be(true)
    row.isNullAt(1) should be(true)
    row.isNullAt(2) should be(true)
  }

  ignore should "ProtoToRowGenerator handle absent fields correctly for compiled messages" in {
    // Test with google.protobuf.Type which is a well-known message
    val partialTypeMsg = Type.newBuilder()
      .setName("test_type")
      // Intentionally omit other fields like sourceContext, syntax, etc.
      .build()

    val binary = partialTypeMsg.toByteArray
    val descriptor = partialTypeMsg.getDescriptorForType

    // Create schema with more fields than what's present in the message
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),          // present
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true)
      ))), nullable = true),                                     // absent
      StructField("oneofs", ArrayType(StringType), nullable = true), // absent
      StructField("syntax", StringType, nullable = true)        // absent
    ))

    val parser = ProtoToRowGenerator.generateParser(descriptor, classOf[Type], sparkSchema)

    // This should not crash with unsafe memory access errors
    val row = parser.parse(binary)

    // Verify structure
    row.numFields should equal(4)

    // Field 0: name - present
    row.field(0, "name") shouldEqual "test_type"

    // Field 1: fields - absent, should be null (empty array would be non-null)
    row.isNullAt(1) should be(true)

    // Field 2: oneofs - absent, should be null
    row.isNullAt(2) should be(true)

    // Field 3: syntax - absent, should be null
    row.isNullAt(3) should be(true)
  }

  "DynamicMessageParser" should "handle absent fields correctly as baseline" in {
    // Test the same scenario with DynamicMessageParser to ensure consistency
    val binary = createPartialMessage()

    val schema = StructType(Seq(
      StructField("string_field", StringType, nullable = true),
      StructField("int_field", IntegerType, nullable = true),
      StructField("bool_field", BooleanType, nullable = true)
    ))

    // Create descriptor
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder()
      .setName("TestMessage")
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("string_field")
        .setNumber(1)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("int_field")
        .setNumber(2)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("bool_field")
        .setNumber(3)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
      .build()

    val fileDescriptorProto = DescriptorProtos.FileDescriptorProto.newBuilder()
      .setName("baseline_test.proto")
      .addMessageType(descriptorProto)
      .build()

    val fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, Array.empty)
    val descriptor = fileDescriptor.findMessageTypeByName("TestMessage")

    val parser = new DynamicMessageParser(descriptor, schema)

    val row = parser.parse(binary)

    // Verify same behavior as optimized parsers
    row.numFields should equal(3)

    // Field 1: present
    row.field(0, "string_field") shouldEqual "present_string"

    // Field 2: absent - should be null
    row.isNullAt(1) should be(true)

    // Field 3: present
    row.isNullAt(2) should be(false)
    row.getBoolean(2) should be(true)
  }
}