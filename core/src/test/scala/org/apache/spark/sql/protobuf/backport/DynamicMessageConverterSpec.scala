package org.apache.spark.sql.protobuf.backport

import com.google.protobuf.{Field, Type}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class DynamicMessageConverterSpec extends AnyFunSpec with Matchers {

  describe("DynamicMessageConverter") {

    it("should convert protobuf binary data to InternalRow") {
      // Create a Type message directly
      val typeMsg = Type.newBuilder()
        .setName("TestType")
        .addFields(Field.newBuilder()
          .setName("field1")
          .setNumber(1)
          .setKind(Field.Kind.TYPE_STRING)
          .build())
        .build()

      val descriptor = typeMsg.getDescriptorForType
      val dataType = SchemaConverters.toSqlType(descriptor).dataType

      val converter = new DynamicMessageConverter(descriptor, dataType)

      val binary = typeMsg.toByteArray

      // Convert binary to InternalRow
      val result = converter.convert(binary)

      result should not be null
      result.isInstanceOf[InternalRow] shouldBe true

      // Check that string fields are converted to UTF8String
      val nameOrdinal = converter.schema.fieldIndex("name")
      val nameValue = result.get(nameOrdinal, StringType)
      nameValue.isInstanceOf[UTF8String] shouldBe true
      nameValue.toString shouldBe "TestType"
    }


    it("should throw exception in fail-fast mode") {
      val typeMsg = Type.newBuilder().setName("TestType").build()
      val descriptor = typeMsg.getDescriptorForType
      val dataType = SchemaConverters.toSqlType(descriptor).dataType

      val converter = new DynamicMessageConverter(descriptor, dataType)

      // Invalid protobuf binary
      val invalidBinary = Array[Byte](0xFF.toByte, 0xFF.toByte, 0xFF.toByte)

      intercept[Exception] {
        converter.convert(invalidBinary)
      }
    }

    it("should return correct schema") {
      val typeMsg = Type.newBuilder().setName("TestType").build()
      val descriptor = typeMsg.getDescriptorForType
      val dataType = SchemaConverters.toSqlType(descriptor).dataType

      val converter = new DynamicMessageConverter(descriptor, dataType)

      converter.schema shouldBe dataType
      converter.schema.isInstanceOf[StructType] shouldBe true
    }

    it("should convert nested structures correctly") {
      // Create a message with repeated fields
      val typeMsg = Type.newBuilder()
        .setName("TestType")
        .addFields(Field.newBuilder()
          .setName("field1")
          .setNumber(1)
          .setKind(Field.Kind.TYPE_STRING)
          .build())
        .build()

      val descriptor = typeMsg.getDescriptorForType
      val dataType = SchemaConverters.toSqlType(descriptor).dataType

      val converter = new DynamicMessageConverter(descriptor, dataType)

      val binary = typeMsg.toByteArray

      val result = converter.convert(binary)
      result should not be null

      // Verify the structure was converted correctly
      val fieldsOrdinal = converter.schema.fieldIndex("fields")
      val fieldsValue = result.get(fieldsOrdinal, converter.schema("fields").dataType)
      fieldsValue should not be null
    }
  }
}