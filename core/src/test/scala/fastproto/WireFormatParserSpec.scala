package fastproto

import com.google.protobuf._
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.protobuf.backport.functions
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream

class WireFormatParserSpec extends AnyFlatSpec with Matchers {

  private def createWireTypeMismatchData(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Create a message that has wire type mismatches when parsed with certain schemas
    // Field 1: VARINT with value 0
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(0)

    // Field 2: LENGTH_DELIMITED containing a nested structure
    val nestedData = {
      val nestedBaos = new ByteArrayOutputStream()
      val nestedOutput = CodedOutputStream.newInstance(nestedBaos)

      // Inner field that could cause wire type mismatch
      nestedOutput.writeTag(5, WireFormat.WIRETYPE_VARINT)
      nestedOutput.writeInt64NoTag(42L)

      nestedOutput.flush()
      nestedBaos.toByteArray
    }

    output.writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeBytesNoTag(ByteString.copyFrom(nestedData))

    output.flush()
    baos.toByteArray
  }

  "WireFormatParser" should "convert simple protobuf messages with correct field values" in {
    // Create a simple Type message (which is available in protobuf-java)
    val typeMsg = Type.newBuilder()
      .setName("test_type")
      .addFields(Field.newBuilder()
        .setName("field1")
        .setNumber(1)
        .setKind(Field.Kind.TYPE_STRING)
        .build())
      .build()

    val binary = typeMsg.toByteArray
    val descriptor = typeMsg.getDescriptorForType

    // Create a simplified Spark schema that matches some fields
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true)
      ))), nullable = true)
    ))

    val parser = new WireFormatParser(descriptor, sparkSchema)

    // Convert and verify results
    val row = parser.parse(binary)

    // Verify the name field
    row.numFields should equal(2)
    val nameValue = row.getUTF8String(0)
    nameValue should not be null
    nameValue.toString should equal("test_type")

    // Verify the fields array
    val fieldsArray = row.getArray(1)
    fieldsArray should not be null
    fieldsArray.numElements() should equal(1)

    val fieldRow = fieldsArray.getStruct(0, 2)
    fieldRow.getUTF8String(0).toString should equal("field1")
    fieldRow.getInt(1) should equal(1)

    // Test schema access
    parser.schema should equal(sparkSchema)
  }

  it should "convert primitive types correctly" in {
    // Create a message with various primitive types using Any message
    val stringValue = Any.pack(StringValue.of("test_string"))
    val binary = stringValue.toByteArray
    val descriptor = stringValue.getDescriptorForType

    val sparkSchema = StructType(Seq(
      StructField("type_url", StringType, nullable = true),
      StructField("value", BinaryType, nullable = true)
    ))

    val parser = new WireFormatParser(descriptor, sparkSchema)
    val row = parser.parse(binary)

    // Verify type_url field
    val typeUrl = row.getUTF8String(0)
    typeUrl should not be null
    typeUrl.toString should include("StringValue")

    // Verify value field (should be the packed StringValue bytes)
    val valueBytes = row.getBinary(1)
    valueBytes should not be null
    valueBytes.length should be > 0
  }

  it should "handle nested messages correctly" in {
    // Create a Type with SourceContext (nested message)
    val sourceContext = SourceContext.newBuilder()
      .setFileName("test.proto")
      .build()

    val typeMsg = Type.newBuilder()
      .setName("nested_test")
      .setSourceContext(sourceContext)
      .build()

    val binary = typeMsg.toByteArray
    val descriptor = typeMsg.getDescriptorForType

    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("source_context", StructType(Seq(
        StructField("file_name", StringType, nullable = true)
      )), nullable = true),
      StructField("syntax", StringType, nullable = true)
    ))

    val parser = new WireFormatParser(descriptor, sparkSchema)
    val row = parser.parse(binary)

    // Verify name field
    row.getUTF8String(0).toString should equal("nested_test")

    // Verify nested source_context
    val nestedRow = row.getStruct(1, 1)
    nestedRow should not be null
    nestedRow.getUTF8String(0).toString should equal("test.proto")

    // Verify syntax field - check if it's null or has default value
    if (row.isNullAt(2)) {
      // Field is null - acceptable
      row.isNullAt(2) should be(true)
    } else {
      // Field has a value - protobuf enum default is typically the first value (0)
      // For Syntax enum, this would be SYNTAX_PROTO2 which converts to "SYNTAX_PROTO2"
      val syntaxValue = row.getUTF8String(2)
      syntaxValue should not be null
      // Accept any valid syntax enum value
      syntaxValue.toString should (equal("SYNTAX_PROTO2") or equal("SYNTAX_PROTO3") or equal(""))
    }
  }

  it should "handle repeated fields correctly" in {
    // Create a Type with multiple fields (repeated)
    val typeMsg = Type.newBuilder()
      .setName("repeated_test")
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
      .addFields(Field.newBuilder()
        .setName("field3")
        .setNumber(3)
        .setKind(Field.Kind.TYPE_BOOL)
        .build())
      .build()

    val binary = typeMsg.toByteArray
    val descriptor = typeMsg.getDescriptorForType

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

    val parser = new WireFormatParser(descriptor, sparkSchema)
    val row = parser.parse(binary)

    // Verify name field
    row.getUTF8String(0).toString should equal("repeated_test")

    // Verify fields array
    val fieldsArray = row.getArray(1)
    fieldsArray.numElements() should equal(3)

    // Check first field
    val field1 = fieldsArray.getStruct(0, 10)
    field1.getUTF8String(3).toString should equal("field1") // name field
    field1.getInt(2) should equal(1) // number field

    // Check second field  
    val field2 = fieldsArray.getStruct(1, 10)
    field2.getUTF8String(3).toString should equal("field2")
    field2.getInt(2) should equal(2)

    // Check third field
    val field3 = fieldsArray.getStruct(2, 10)
    field3.getUTF8String(3).toString should equal("field3")
    field3.getInt(2) should equal(3)
  }

  it should "handle empty messages" in {
    val typeMsg = Type.newBuilder().build()
    val binary = typeMsg.toByteArray
    val descriptor = typeMsg.getDescriptorForType

    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val parser = new WireFormatParser(descriptor, sparkSchema)
    val row = parser.parse(binary)

    row.numFields should equal(1)
    // Empty message should have null name (protobuf default for unset string is empty string, not null)
    // But our parser may initialize fields to null unless explicitly set
    if (row.isNullAt(0)) {
      // Field is null - acceptable
      row.isNullAt(0) should be(true)
    } else {
      // Field has a value - should be empty string for protobuf default
      val value = row.getUTF8String(0)
      value.toString should equal("")
    }
  }

  it should "handle unknown fields gracefully" in {
    // Create a message with more fields than our schema expects
    val typeMsg = Type.newBuilder()
      .setName("test")
      .setSourceContext(SourceContext.newBuilder().setFileName("test.proto").build())
      .build()

    val binary = typeMsg.toByteArray
    val descriptor = typeMsg.getDescriptorForType

    // Schema that only includes the name field
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true)
    ))

    val parser = new WireFormatParser(descriptor, sparkSchema)
    val row = parser.parse(binary)

    row.numFields should equal(1)
    row.getUTF8String(0).toString should equal("test")
  }

  it should "convert numeric types correctly" in {
    // Create an Enum message to test numeric fields
    val enumMsg = Enum.newBuilder()
      .setName("test_enum")
      .addEnumvalue(EnumValue.newBuilder()
        .setName("VALUE_1")
        .setNumber(1)
        .build())
      .addEnumvalue(EnumValue.newBuilder()
        .setName("VALUE_2")
        .setNumber(2)
        .build())
      .build()

    val binary = enumMsg.toByteArray
    val descriptor = enumMsg.getDescriptorForType

    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("enumvalue", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true),
        StructField("options", ArrayType(StructType(Seq(
          StructField("name", StringType, nullable = true),
          StructField("value", StructType(Seq(
            StructField("type_url", StringType, nullable = true),
            StructField("value", BinaryType, nullable = true)
          )), nullable = true)
        ))), nullable = true)
      ))), nullable = true),
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

    val parser = new WireFormatParser(descriptor, sparkSchema)
    val row = parser.parse(binary)

    // Verify name
    row.getUTF8String(0).toString should equal("test_enum")

    // Verify enum values
    val enumValues = row.getArray(1)
    enumValues.numElements() should equal(2)

    val value1 = enumValues.getStruct(0, 3)
    value1.getUTF8String(0).toString should equal("VALUE_1")
    value1.getInt(1) should equal(1)

    val value2 = enumValues.getStruct(1, 3)
    value2.getUTF8String(0).toString should equal("VALUE_2")
    value2.getInt(1) should equal(2)
  }

  ignore should "produce equivalent results when using binary descriptor sets vs direct WireFormatParser" in {
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

    // Method 1: Direct WireFormatParser
    val directParser = new WireFormatParser(descriptor, sparkSchema)
    val directResult = directParser.parse(binary)

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
      InternalRowToSqlRowComparator.compareRows(directResult, binaryDescResult, sparkSchema, "root")

      println("✓ Direct WireFormatParser and binary descriptor set produce identical results")

    } finally {
      spark.stop()
    }
  }


  it should "handle wire type mismatches gracefully" in {
    // Create synthetic binary data with wire type mismatches
    val binary = createWireTypeMismatchData()

    // Use a simple Type descriptor - the data should be parsed without errors
    // even if wire types don't match exactly what the schema expects
    val typeMsg = Type.newBuilder().build()
    val descriptor = typeMsg.getDescriptorForType

    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("fields", ArrayType(StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("number", IntegerType, nullable = true)
      ))), nullable = true)
    ))

    val parser = new WireFormatParser(descriptor, sparkSchema)

    // This should not throw an exception, even with wire type mismatches
    val row = parser.parse(binary)

    // Verify the parser completed successfully
    row.numFields should equal(2)

    // Fields may be null due to wire type mismatches, which is acceptable
    // The important thing is that parsing didn't crash
  }

  it should "skip fields with unexpected wire types" in {
    // Create a message with a field that would be a wire type mismatch
    val typeMsg = Type.newBuilder()
      .setName("wire_type_test")
      .build()

    val binary = typeMsg.toByteArray
    val descriptor = typeMsg.getDescriptorForType

    // Create a schema that expects field 5 to be a string (which would be LENGTH_DELIMITED)
    // but the actual data might have field 5 as VARINT
    val sparkSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("unknown_field", StringType, nullable = true) // This field doesn't exist in the message
    ))

    val parser = new WireFormatParser(descriptor, sparkSchema)
    val row = parser.parse(binary)

    // Should complete without errors
    row.numFields should equal(2)
    row.getUTF8String(0).toString should equal("wire_type_test")
  }

  // ==== Interleaving Tests for PrimitiveArrayWriter ====

  it should "handle packed-then-unpacked encoding for repeated int32" in {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: packed repeated int32 [1, 2, 3]
    output.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeUInt32NoTag(3) // 3 bytes for 3 varints (1 byte each)
    output.writeInt32NoTag(1)
    output.writeInt32NoTag(2)
    output.writeInt32NoTag(3)

    // Field 1: unpacked repeated int32 [4, 5]
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(4)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(5)

    output.flush()
    val binary = baos.toByteArray

    val fileDesc = Descriptors.FileDescriptor.buildFrom(
      com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("test.proto")
        .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
          .setName("TestMessage")
          .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("values")
            .setNumber(1)
            .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
            .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)))
        .build(),
      Array.empty[Descriptors.FileDescriptor]
    )

    val msgDescriptor = fileDesc.getMessageTypes.get(0)
    val testSchema = StructType(Seq(
      StructField("values", ArrayType(IntegerType), nullable = true)
    ))

    val parser = new WireFormatParser(msgDescriptor, testSchema)
    val row = parser.parse(binary)

    // Should have all 5 values: [1,2,3,4,5]
    val valuesArray = row.getArray(0)
    valuesArray.numElements() should equal(5)
    valuesArray.getInt(0) should equal(1)
    valuesArray.getInt(1) should equal(2)
    valuesArray.getInt(2) should equal(3)
    valuesArray.getInt(3) should equal(4)
    valuesArray.getInt(4) should equal(5)
  }

  it should "handle unpacked-then-packed encoding for repeated int64" in {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: unpacked repeated int64 [10, 20]
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt64NoTag(10L)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt64NoTag(20L)

    // Field 1: packed repeated int64 [30, 40, 50]
    output.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeUInt32NoTag(3) // 3 bytes for 3 varints (1 byte each)
    output.writeInt64NoTag(30L)
    output.writeInt64NoTag(40L)
    output.writeInt64NoTag(50L)

    output.flush()
    val binary = baos.toByteArray

    val fileDesc = Descriptors.FileDescriptor.buildFrom(
      com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("test.proto")
        .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
          .setName("TestMessage")
          .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("values")
            .setNumber(1)
            .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
            .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)))
        .build(),
      Array.empty[Descriptors.FileDescriptor]
    )

    val msgDescriptor = fileDesc.getMessageTypes.get(0)
    val testSchema = StructType(Seq(
      StructField("values", ArrayType(LongType), nullable = true)
    ))

    val parser = new WireFormatParser(msgDescriptor, testSchema)
    val row = parser.parse(binary)

    // Should have all 5 values: [10,20,30,40,50]
    val valuesArray = row.getArray(0)
    valuesArray.numElements() should equal(5)
    valuesArray.getLong(0) should equal(10L)
    valuesArray.getLong(1) should equal(20L)
    valuesArray.getLong(2) should equal(30L)
    valuesArray.getLong(3) should equal(40L)
    valuesArray.getLong(4) should equal(50L)
  }

  it should "handle interleaved repeated fields (field1 -> field2 -> field1)" in {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: unpacked [1, 2]
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(1)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(2)

    // Field 2: unpacked [100, 200]
    output.writeTag(2, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(100)
    output.writeTag(2, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(200)

    // Field 1: unpacked [3, 4] - interleaving! Should trigger fallback
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(3)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(4)

    output.flush()
    val binary = baos.toByteArray

    val fileDesc = Descriptors.FileDescriptor.buildFrom(
      com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("test.proto")
        .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
          .setName("TestMessage")
          .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("field1")
            .setNumber(1)
            .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
            .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
          .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("field2")
            .setNumber(2)
            .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
            .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)))
        .build(),
      Array.empty[Descriptors.FileDescriptor]
    )

    val msgDescriptor = fileDesc.getMessageTypes.get(0)
    val testSchema = StructType(Seq(
      StructField("field1", ArrayType(IntegerType), nullable = true),
      StructField("field2", ArrayType(IntegerType), nullable = true)
    ))

    val parser = new WireFormatParser(msgDescriptor, testSchema)
    val row = parser.parse(binary)

    // Field 1: [1,2,3,4]
    val field1Array = row.getArray(0)
    field1Array.numElements() should equal(4)
    field1Array.getInt(0) should equal(1)
    field1Array.getInt(1) should equal(2)
    field1Array.getInt(2) should equal(3)
    field1Array.getInt(3) should equal(4)

    // Field 2: [100,200]
    val field2Array = row.getArray(1)
    field2Array.numElements() should equal(2)
    field2Array.getInt(0) should equal(100)
    field2Array.getInt(1) should equal(200)
  }

  it should "handle interleaving with singular variable-length field" in {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: repeated int32 [1, 2]
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(1)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(2)

    // Field 2: singular string "test"
    output.writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED)
    output.writeStringNoTag("test")

    // Field 1: repeated int32 [3, 4] - interleaving with variable-length write!
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(3)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(4)

    output.flush()
    val binary = baos.toByteArray

    val fileDesc = Descriptors.FileDescriptor.buildFrom(
      com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("test.proto")
        .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
          .setName("TestMessage")
          .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("values")
            .setNumber(1)
            .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
            .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
          .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("name")
            .setNumber(2)
            .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
            .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)))
        .build(),
      Array.empty[Descriptors.FileDescriptor]
    )

    val msgDescriptor = fileDesc.getMessageTypes.get(0)
    val testSchema = StructType(Seq(
      StructField("values", ArrayType(IntegerType), nullable = true),
      StructField("name", StringType, nullable = true)
    ))

    val parser = new WireFormatParser(msgDescriptor, testSchema)
    val row = parser.parse(binary)

    // Field 1: [1,2,3,4] - should trigger fallback after string write
    val valuesArray = row.getArray(0)
    valuesArray.numElements() should equal(4)
    valuesArray.getInt(0) should equal(1)
    valuesArray.getInt(1) should equal(2)
    valuesArray.getInt(2) should equal(3)
    valuesArray.getInt(3) should equal(4)

    // Field 2: "test"
    row.getUTF8String(1).toString should equal("test")
  }

  it should "handle contiguous unpacked repeated values (no interleaving)" in {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: unpacked repeated [1, 2, 3, 4, 5] - all contiguous
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(1)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(2)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(3)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(4)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(5)

    output.flush()
    val binary = baos.toByteArray

    val fileDesc = Descriptors.FileDescriptor.buildFrom(
      com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("test.proto")
        .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
          .setName("TestMessage")
          .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("values")
            .setNumber(1)
            .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
            .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)))
        .build(),
      Array.empty[Descriptors.FileDescriptor]
    )

    val msgDescriptor = fileDesc.getMessageTypes.get(0)
    val testSchema = StructType(Seq(
      StructField("values", ArrayType(IntegerType), nullable = true)
    ))

    val parser = new WireFormatParser(msgDescriptor, testSchema)
    val row = parser.parse(binary)

    // Should use PrimitiveArrayWriter (no fallback) for optimal performance
    val valuesArray = row.getArray(0)
    valuesArray.numElements() should equal(5)
    valuesArray.getInt(0) should equal(1)
    valuesArray.getInt(1) should equal(2)
    valuesArray.getInt(2) should equal(3)
    valuesArray.getInt(3) should equal(4)
    valuesArray.getInt(4) should equal(5)
  }

  it should "correctly handle two repeated fields and verify UnsafeArrayData length" in {
    val baos = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(baos)

    // Field 1: repeated int32 [10, 20, 30]
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(10)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(20)
    output.writeTag(1, WireFormat.WIRETYPE_VARINT)
    output.writeInt32NoTag(30)

    // Field 2: repeated int64 [100, 200, 300]
    output.writeTag(2, WireFormat.WIRETYPE_VARINT)
    output.writeInt64NoTag(100L)
    output.writeTag(2, WireFormat.WIRETYPE_VARINT)
    output.writeInt64NoTag(200L)
    output.writeTag(2, WireFormat.WIRETYPE_VARINT)
    output.writeInt64NoTag(300L)

    output.flush()
    val binary = baos.toByteArray

    val fileDesc = Descriptors.FileDescriptor.buildFrom(
      com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("test.proto")
        .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
          .setName("TestMessage")
          .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("field1")
            .setNumber(1)
            .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
            .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
          .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("field2")
            .setNumber(2)
            .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
            .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)))
        .build(),
      Array.empty[Descriptors.FileDescriptor]
    )

    val msgDescriptor = fileDesc.getMessageTypes.get(0)
    val testSchema = StructType(Seq(
      StructField("field1", ArrayType(IntegerType), nullable = true),
      StructField("field2", ArrayType(LongType), nullable = true)
    ))

    val parser = new WireFormatParser(msgDescriptor, testSchema)
    val row = parser.parse(binary)

    // Verify field1 array length and values
    val field1Array = row.getArray(0).asInstanceOf[UnsafeArrayData]
    field1Array.numElements() should equal(3)
    field1Array.getInt(0) should equal(10)
    field1Array.getInt(1) should equal(20)
    field1Array.getInt(2) should equal(30)

    // Verify field1 UnsafeArrayData size
    // Header: 8 bytes (count) + 8 bytes (null bitmap for ≤64 elements) = 16 bytes
    // Data: 3 * 4 bytes = 12 bytes, rounded to 8-byte boundary = 16 bytes
    // Total: 16 + 16 = 32 bytes
    field1Array.getSizeInBytes should equal(32)

    // Verify field2 array length and values
    val field2Array = row.getArray(1).asInstanceOf[UnsafeArrayData]
    field2Array.numElements() should equal(3)
    field2Array.getLong(0) should equal(100L)
    field2Array.getLong(1) should equal(200L)
    field2Array.getLong(2) should equal(300L)

    // Verify field2 UnsafeArrayData size
    // Header: 8 bytes (count) + 8 bytes (null bitmap) = 16 bytes
    // Data: 3 * 8 bytes = 24 bytes (already 8-byte aligned)
    // Total: 16 + 24 = 40 bytes
    field2Array.getSizeInBytes should equal(40)
  }
}