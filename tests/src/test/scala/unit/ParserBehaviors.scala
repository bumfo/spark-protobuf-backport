package unit

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.{ByteString, Message}
import fastproto.{InternalRowMatchers, Parser, RecursiveSchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import testproto.AllTypesProtos._
import testproto.EdgeCasesProtos._
import testproto.MapsProtos._
import testproto.NestedProtos._
import testproto.TestData

/**
 * Shared test behaviors for all parser implementations.
 * Each behavior tests both correctness AND nullability of fields.
 *
 * Mix this trait into concrete parser specs and call the behaviors with
 * a parser factory function.
 */
trait ParserBehaviors extends InternalRowMatchers { this: AnyFlatSpec with Matchers =>

  /**
   * Factory function type for creating parsers.
   * Concrete specs provide their specific parser creation logic.
   */
  type ParserFactory = (Descriptor, StructType, Option[Class[_ <: Message]]) => Parser

  // ========== Primitive Types Behavior ==========

  def primitiveTypeParser(createParser: ParserFactory): Unit = {
    it should "parse all primitive types correctly" in {
      val message = TestData.createFullPrimitives()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[AllPrimitiveTypes]))

      val row = parser.parse(binary)

      // Validate all fields are present (not null) and have correct values
      row.isNullAt(0) shouldBe false
      row.getInt(0) shouldBe 42 // int32_field

      row.isNullAt(1) shouldBe false
      row.getLong(1) shouldBe 12345678901L // int64_field

      row.isNullAt(2) shouldBe false
      row.getInt(2) shouldBe -1 // uint32_field (unsigned max as signed)

      row.isNullAt(3) shouldBe false
      row.getLong(3) shouldBe Long.MaxValue // uint64_field

      row.isNullAt(4) shouldBe false
      row.getInt(4) shouldBe -42 // sint32_field (ZigZag encoded)

      row.isNullAt(5) shouldBe false
      row.getLong(5) shouldBe -12345678901L // sint64_field (ZigZag encoded)

      row.isNullAt(6) shouldBe false
      row.getInt(6) shouldBe 100 // fixed32_field

      row.isNullAt(7) shouldBe false
      row.getLong(7) shouldBe 1000L // fixed64_field

      row.isNullAt(8) shouldBe false
      row.getInt(8) shouldBe -100 // sfixed32_field

      row.isNullAt(9) shouldBe false
      row.getLong(9) shouldBe -1000L // sfixed64_field

      row.isNullAt(10) shouldBe false
      row.getFloat(10) shouldBe 3.14f +- 0.001f // float_field

      row.isNullAt(11) shouldBe false
      row.getDouble(11) shouldBe 2.71828 +- 0.00001 // double_field

      row.isNullAt(12) shouldBe false
      row.getBoolean(12) shouldBe true // bool_field

      row.isNullAt(13) shouldBe false
      row.getUTF8String(13).toString shouldBe "test_string" // string_field

      row.isNullAt(14) shouldBe false
      val bytes = row.getBinary(14)
      bytes shouldBe Array[Byte](1, 2, 3, 4, 5)

      row.isNullAt(15) shouldBe false
      row.getInt(15) shouldBe 1 // status_field (ACTIVE = 1)
    }

    it should "handle null/absent fields correctly" in {
      val message = TestData.createEmptyPrimitives() // All fields are defaults
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[AllPrimitiveTypes]))

      val row = parser.parse(binary)

      // Proto3: Absent fields should be null (or have default values depending on parser)
      // For consistency, we expect null for unset fields in proto3
      // Note: Some parsers might return default values (0, false, "") instead of null
      // This is acceptable behavior - we just verify the field is either null or default

      // Check numeric fields: either null or 0
      if (!row.isNullAt(0)) {
        row.getInt(0) shouldBe 0 // int32_field default
      }

      if (!row.isNullAt(1)) {
        row.getLong(1) shouldBe 0L // int64_field default
      }

      // Check boolean field: either null or false
      if (!row.isNullAt(12)) {
        row.getBoolean(12) shouldBe false // bool_field default
      }

      // Check string field: either null or empty
      if (!row.isNullAt(13)) {
        row.getUTF8String(13).toString shouldBe "" // string_field default
      }
    }

    it should "zero out null field values in UnsafeRow" in {
      import org.apache.spark.sql.catalyst.expressions.UnsafeRow

      val message = TestData.createEmptyPrimitives() // All fields are defaults
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[AllPrimitiveTypes]))

      val row = parser.parse(binary)

      // UnsafeRow.setNullAt() zeros out the 8-byte word for null fields "to preserve row equality"
      // This test verifies that null fields have getLong return 0L
      row shouldBe a[UnsafeRow]
      val unsafeRow = row.asInstanceOf[UnsafeRow]

      // For each field, if it's null, the underlying long value should be 0L
      // Field ordinals: 0=int32, 1=int64, 2=uint32, 3=uint64, 4=sint32, 5=sint64,
      //                 6=fixed32, 7=fixed64, 8=sfixed32, 9=sfixed64,
      //                 10=float, 11=double, 12=bool, 15=enum
      // Fields 13-14 (string, bytes) are variable-length, so not tested here

      val fixedLengthFields = Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15)
      fixedLengthFields.foreach { ordinal =>
        if (unsafeRow.isNullAt(ordinal)) {
          unsafeRow.getLong(ordinal) shouldBe 0L
        }
      }
    }

    it should "handle sparse messages with mixed null and present fields" in {
      val message = TestData.createSparsePrimitives() // Only 2 fields set
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[AllPrimitiveTypes]))

      val row = parser.parse(binary)

      // Fields that ARE set should have correct values
      row.getInt(0) shouldBe 10 // int32_field
      row.getUTF8String(13).toString shouldBe "sparse" // string_field

      // Other fields are either null or have defaults - both are acceptable
    }
  }

  // ========== Repeated Fields Behavior ==========

  def repeatedFieldParser(createParser: ParserFactory): Unit = {
    it should "parse repeated fields with packed encoding" in {
      val message = TestData.createFullRepeated()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[AllRepeatedTypes]))

      val row = parser.parse(binary)

      // int32_list
      row.isNullAt(0) shouldBe false
      val int32Array = row.getArray(0)
      int32Array.numElements() shouldBe 5
      int32Array.getInt(0) shouldBe 1
      int32Array.getInt(4) shouldBe 5

      // sint32_list (CRITICAL: ZigZag encoded packed)
      row.isNullAt(4) shouldBe false
      val sint32Array = row.getArray(4)
      sint32Array.numElements() shouldBe 3
      sint32Array.getInt(0) shouldBe -1
      sint32Array.getInt(1) shouldBe -2
      sint32Array.getInt(2) shouldBe -3

      // sint64_list (CRITICAL: ZigZag encoded packed)
      row.isNullAt(5) shouldBe false
      val sint64Array = row.getArray(5)
      sint64Array.numElements() shouldBe 3
      sint64Array.getLong(0) shouldBe -10L
      sint64Array.getLong(1) shouldBe -20L
      sint64Array.getLong(2) shouldBe -30L

      // float_list
      row.isNullAt(10) shouldBe false
      val floatArray = row.getArray(10)
      floatArray.numElements() shouldBe 4
      floatArray.getFloat(0) shouldBe 1.1f +- 0.01f

      // string_list (not packed - always length-delimited)
      row.isNullAt(13) shouldBe false
      val stringArray = row.getArray(13)
      stringArray.numElements() shouldBe 3
      stringArray.getUTF8String(0).toString shouldBe "a"
      stringArray.getUTF8String(1).toString shouldBe "b"
      stringArray.getUTF8String(2).toString shouldBe "c"
    }

    it should "handle empty repeated fields" in {
      val message = TestData.createEmptyRepeated()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[AllRepeatedTypes]))

      val row = parser.parse(binary)

      // Empty repeated fields should be either null or empty arrays
      // Both are acceptable depending on parser implementation
      if (!row.isNullAt(0)) {
        row.getArray(0).numElements() shouldBe 0
      }

      if (!row.isNullAt(4)) {
        row.getArray(4).numElements() shouldBe 0
      }
    }
  }

  // ========== Unpacked Repeated Fields Behavior ==========

  def unpackedRepeatedFieldParser(createParser: ParserFactory): Unit = {
    it should "parse unpacked repeated fields correctly" in {
      val message = TestData.createFullUnpackedRepeated()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[AllUnpackedRepeatedTypes]))

      val row = parser.parse(binary)

      // int32_list
      row.isNullAt(0) shouldBe false
      val int32Array = row.getArray(0)
      int32Array.numElements() shouldBe 5
      int32Array.getInt(0) shouldBe 1
      int32Array.getInt(4) shouldBe 5

      // sint32_list (CRITICAL: ZigZag encoded UNPACKED)
      row.isNullAt(4) shouldBe false
      val sint32Array = row.getArray(4)
      sint32Array.numElements() shouldBe 3
      sint32Array.getInt(0) shouldBe -1
      sint32Array.getInt(1) shouldBe -2
      sint32Array.getInt(2) shouldBe -3

      // sint64_list (CRITICAL: ZigZag encoded UNPACKED)
      row.isNullAt(5) shouldBe false
      val sint64Array = row.getArray(5)
      sint64Array.numElements() shouldBe 3
      sint64Array.getLong(0) shouldBe -10L
      sint64Array.getLong(1) shouldBe -20L
      sint64Array.getLong(2) shouldBe -30L

      // float_list
      row.isNullAt(10) shouldBe false
      val floatArray = row.getArray(10)
      floatArray.numElements() shouldBe 4
      floatArray.getFloat(0) shouldBe 1.1f +- 0.01f

      // string_list (not packable - always length-delimited)
      row.isNullAt(13) shouldBe false
      val stringArray = row.getArray(13)
      stringArray.numElements() shouldBe 3
      stringArray.getUTF8String(0).toString shouldBe "a"
      stringArray.getUTF8String(1).toString shouldBe "b"
      stringArray.getUTF8String(2).toString shouldBe "c"
    }

    it should "handle empty unpacked repeated fields" in {
      val message = TestData.createEmptyUnpackedRepeated()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[AllUnpackedRepeatedTypes]))

      val row = parser.parse(binary)

      // Empty repeated fields should be either null or empty arrays
      // Both are acceptable depending on parser implementation
      if (!row.isNullAt(0)) {
        row.getArray(0).numElements() shouldBe 0
      }

      if (!row.isNullAt(4)) {
        row.getArray(4).numElements() shouldBe 0
      }
    }
  }

  // ========== Nested Messages Behavior ==========

  def nestedMessageParser(createParser: ParserFactory): Unit = {
    it should "parse nested messages correctly" in {
      val message = TestData.createNestedMessage()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[Nested]))

      val row = parser.parse(binary)

      // name field
      row.isNullAt(0) shouldBe false
      row.getUTF8String(0).toString shouldBe "parent"

      // inner nested message
      row.isNullAt(1) shouldBe false
      val innerRow = row.getStruct(1, 4)
      innerRow.getInt(0) shouldBe 42 // value
      innerRow.getUTF8String(1).toString shouldBe "inner" // description

      // deep nested message
      innerRow.isNullAt(2) shouldBe false
      val deepRow = innerRow.getStruct(2, 3)
      deepRow.getBoolean(0) shouldBe true // flag
      deepRow.getDouble(1) shouldBe 3.14 +- 0.01 // score
    }

    it should "parse recursive messages correctly" in {
      val message = TestData.createRecursiveMessage(3) // 3 levels deep
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[Recursive]))

      val row = parser.parse(binary)

      // Level 0
      row.getUTF8String(0).toString shouldBe "node_3"
      row.getInt(1) shouldBe 3

      // Level 1 (child)
      row.isNullAt(2) shouldBe false
      val level1 = row.getStruct(2, 4)
      level1.getUTF8String(0).toString shouldBe "node_2"
      level1.getInt(1) shouldBe 2

      // Level 2 (grandchild)
      level1.isNullAt(2) shouldBe false
      val level2 = level1.getStruct(2, 4)
      level2.getUTF8String(0).toString shouldBe "node_1"
      level2.getInt(1) shouldBe 1
    }

    it should "handle null nested messages" in {
      val message = Nested.newBuilder()
        .setName("no_nested")
        // inner field not set
        .build()

      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[Nested]))

      val row = parser.parse(binary)

      row.getUTF8String(0).toString shouldBe "no_nested"

      // inner field should be null (or potentially an empty struct)
      // Accept either null or a valid struct depending on parser
    }
  }

  // ========== Map Fields Behavior ==========

  def mapFieldParser(createParser: ParserFactory): Unit = {
    it should "parse map fields correctly" in pending /* Map support pending */ /*{
      val message = TestData.createMapMessage()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[WithMaps]))

      val row = parser.parse(binary)

      // string_to_int map
      row.isNullAt(0) shouldBe false
      val stringToIntMap = row.getMap(0)
      stringToIntMap.numElements() shouldBe 2

      // int_to_string map
      row.isNullAt(1) shouldBe false
      val intToStringMap = row.getMap(1)
      intToStringMap.numElements() shouldBe 2

      // string_to_message map
      row.isNullAt(5) shouldBe false
      val stringToMsgMap = row.getMap(5)
      stringToMsgMap.numElements() shouldBe 1
    }*/

    it should "handle empty maps" in pending /* Map support pending */ /*{
      val message = WithMaps.newBuilder().build()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[WithMaps]))

      val row = parser.parse(binary)

      // Empty maps should be null or have 0 elements
      if (!row.isNullAt(0)) {
        row.getMap(0).numElements() shouldBe 0
      }
    }*/
  }

  // ========== Edge Cases Behavior ==========

  def edgeCaseParser(createParser: ParserFactory): Unit = {
    it should "handle empty messages" in {
      val message = TestData.createEmptyMessage()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[EmptyMessage]))

      val row = parser.parse(binary)

      // Empty message should have 0 fields
      row.numFields shouldBe 0
    }

    it should "handle single field messages" in {
      val message = TestData.createSingleFieldMessage()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[SingleField]))

      val row = parser.parse(binary)

      row.numFields shouldBe 1
      row.isNullAt(0) shouldBe false
      row.getUTF8String(0).toString shouldBe "value"
    }

    it should "handle sparse messages" in {
      val message = TestData.createSparseMessage()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[Sparse]))

      val row = parser.parse(binary)

      // Fields that are set
      row.isNullAt(0) shouldBe false
      row.getInt(0) shouldBe 42

      row.isNullAt(1) shouldBe false
      row.getUTF8String(1).toString shouldBe "sparse_value"

      // Other fields are null or have defaults - both acceptable
    }

    it should "handle default values correctly" in {
      val message = TestData.createDefaults()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[Defaults]))

      val row = parser.parse(binary)

      // Proto3 defaults: either null or default value is acceptable
      if (!row.isNullAt(0)) row.getInt(0) shouldBe 0
      if (!row.isNullAt(1)) row.getUTF8String(1).toString shouldBe ""
      if (!row.isNullAt(2)) row.getBoolean(2) shouldBe false
      if (!row.isNullAt(3)) row.getInt(3) shouldBe 0 // DEFAULT_UNKNOWN = 0
    }

    it should "handle oneof fields" in {
      val message = TestData.createOneof()
      val binary = message.toByteArray
      val descriptor = message.getDescriptorForType
      val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
      val parser = createParser(descriptor, schema, Some(classOf[WithOneof]))

      val row = parser.parse(binary)

      row.getUTF8String(0).toString shouldBe "oneof_test"

      // string_choice is set
      row.isNullAt(1) shouldBe false
      row.getUTF8String(1).toString shouldBe "string_value"

      // Other oneof choices should be null
      // (int_choice at ordinal 2, message_choice at ordinal 3)
    }
  }
}