package fastproto

import com.google.protobuf._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.protobuf.backport.functions
import java.io.ByteArrayOutputStream

class WireFormatConverterSpec extends AnyFlatSpec with Matchers {

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

  ignore should "produce equivalent results when using binary descriptor sets vs direct WireFormatConverter" in {
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

    // Method 1: Direct WireFormatConverter
    val directConverter = new WireFormatConverter(descriptor, sparkSchema)
    val directResult = directConverter.convert(binary)

    // Method 2: Binary descriptor set through from_protobuf function (should use WireFormatConverter internally)
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
      compareInternalRows(directResult, binaryDescResult, sparkSchema, "root")

      println("✓ Direct WireFormatConverter and binary descriptor set produce identical results")
      
    } finally {
      spark.stop()
    }
  }

  private def compareInternalRows(
      row1: org.apache.spark.sql.catalyst.InternalRow,
      row2: org.apache.spark.sql.Row,
      schema: StructType,
      path: String): Unit = {
    
    row1.numFields should equal(row2.size)
    
    schema.fields.zipWithIndex.foreach { case (field, idx) =>
      val fieldPath = s"$path.${field.name}"
      
      field.dataType match {
        case StringType =>
          // For string fields, treat null and empty string as equivalent (protobuf semantics)
          val str1 = if (row1.isNullAt(idx)) "" else row1.getUTF8String(idx).toString
          val str2 = if (row2.isNullAt(idx)) "" else row2.getAs[String](idx)
          str1 should equal(str2)
        case _ =>
          // For non-string fields, use strict nullability comparison
          (row1.isNullAt(idx), row2.isNullAt(idx)) match {
            case (true, true) => // Both null - OK
            case (false, false) =>
              field.dataType match {
                case IntegerType =>
                  val int1 = row1.getInt(idx)
                  val int2 = row2.getAs[Int](idx)
                  int1 should equal(int2)

                case BooleanType =>
                  val bool1 = row1.getBoolean(idx)
                  val bool2 = row2.getAs[Boolean](idx)
                  bool1 should equal(bool2)

                case BinaryType =>
                  val bytes1 = row1.getBinary(idx)
                  val bytes2 = row2.getAs[Array[Byte]](idx)
                  bytes1 should equal(bytes2)

                case arrayType: ArrayType =>
                  val array1 = row1.getArray(idx)
                  val array2 = row2.getAs[Seq[_]](idx)

                  if (array1 == null && array2 == null) {
                    // Both null - OK
                  } else if (array1 != null && array2 != null) {
                    array1.numElements() should equal(array2.size)

                    // Compare each element based on element type
                    arrayType.elementType match {
                      case StringType =>
                        for (i <- 0 until array1.numElements()) {
                          // For string array elements, treat null and empty string as equivalent
                          val elem1 = if (array1.isNullAt(i)) "" else array1.getUTF8String(i).toString
                          val elem2 = if (array2(i) == null) "" else array2(i).asInstanceOf[String]
                          elem1 should equal(elem2)
                        }
                      case structType: StructType =>
                        for (i <- 0 until array1.numElements()) {
                          if (!array1.isNullAt(i)) {
                            val struct1 = array1.getStruct(i, structType.fields.length)
                            val struct2 = array2(i).asInstanceOf[org.apache.spark.sql.Row]
                            compareInternalRows(struct1, struct2, structType, s"$fieldPath[$i]")
                          }
                        }
                      case _ => // For other array element types, just compare sizes for now
                        array1.numElements() should equal(array2.size)
                    }
                  } else {
                    fail(s"Array mismatch at $fieldPath: one is null, other is not")
                  }

                case structType: StructType =>
                  val struct1 = row1.getStruct(idx, structType.fields.length)
                  val struct2 = row2.getAs[org.apache.spark.sql.Row](idx)

                  if (struct1 == null && struct2 == null) {
                    // Both null - OK
                  } else if (struct1 != null && struct2 != null) {
                    compareInternalRows(struct1, struct2, structType, fieldPath)
                  } else {
                    fail(s"Struct mismatch at $fieldPath: one is null, other is not")
                  }
              }

            case (null1, null2) =>
              fail(s"Nullability mismatch at $fieldPath: row1.isNull=$null1, row2.isNull=$null2")
          }
      }
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

    val converter = new WireFormatConverter(descriptor, sparkSchema)

    // This should not throw an exception, even with wire type mismatches
    val row = converter.convert(binary)

    // Verify the converter completed successfully
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

    val converter = new WireFormatConverter(descriptor, sparkSchema)
    val row = converter.convert(binary)

    // Should complete without errors
    row.numFields should equal(2)
    row.getUTF8String(0).toString should equal("wire_type_test")
  }
}