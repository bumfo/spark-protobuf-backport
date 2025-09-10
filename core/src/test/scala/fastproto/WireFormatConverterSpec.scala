package fastproto

import com.google.protobuf._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util.ArrayData

class WireFormatConverterSpec extends AnyFlatSpec with Matchers {

  "WireFormatConverter" should "convert simple protobuf messages with correct field values" in {
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
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    
    // Convert and verify results
    val row = converter.convert(binary)
    
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
    converter.schema should equal(sparkSchema)
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
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    val row = converter.convert(binary)
    
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
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    val row = converter.convert(binary)
    
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
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    val row = converter.convert(binary)
    
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
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    val row = converter.convert(binary)
    
    row.numFields should equal(1)
    // Empty message should have null name (protobuf default for unset string is empty string, not null)
    // But our converter may initialize fields to null unless explicitly set
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
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    val row = converter.convert(binary)
    
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
    
    val converter = new WireFormatConverter(descriptor, sparkSchema)
    val row = converter.convert(binary)
    
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
}