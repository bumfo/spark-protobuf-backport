package benchmark

import fastproto.{ProtoToRowGenerator, WireFormatToRowGenerator}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for SimpleMessage wireformat code generation path.
 *
 * Tests the optimized wireformat converter generation for simple messages
 * with 120 scalar and repeated scalar fields.
 */
class SimpleMessageTest extends AnyFunSuite with Matchers {

  private val testMessage = TestDataGenerator.createSimpleMessage()
  private val binaryData = testMessage.toByteArray
  private val descriptor = testMessage.getDescriptorForType
  private val sparkSchema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

  private val generatedWireFormatParser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)

  test("SimpleMessage should have 120 fields") {
    sparkSchema.fields.length shouldBe 120
  }

  test("Generated WireFormat converter should handle SimpleMessage 1") {
    val b = SimpleBenchmarkProtos.SimpleMessage.newBuilder()
    b.setFieldInt32001(100)
    b.setFieldInt64021(200)
    b.addRepeatedInt32086(300)
    val msg = b.build().toByteArray

    val converter = generatedWireFormatParser
    val row = converter.parse(msg)

    row should not be null
    row.numFields shouldBe 120
  }

  test("Generated WireFormat converter should handle SimpleMessage") {
    val converter = generatedWireFormatParser
    val row = converter.parse(binaryData)

    row should not be null
    row.numFields shouldBe 120

    // Verify some known field values
    row.getInt(0) shouldBe 100 // field_int32_001
    row.getInt(1) shouldBe 200 // field_int32_002
    row.getInt(19) shouldBe 2000 // field_int32_020

    // Verify int64 fields
    row.getLong(20) shouldBe 21000000L // field_int64_021
    row.getLong(39) shouldBe 40000000L // field_int64_040

    // Verify float fields
    row.getFloat(40) shouldBe 41.5f // field_float_041
    row.getFloat(49) shouldBe 50.5f // field_float_050

    // Verify double fields
    row.getDouble(50) shouldBe 51.25 // field_double_051
    row.getDouble(59) shouldBe 60.25 // field_double_060

    // Verify boolean fields (alternating pattern)
    row.getBoolean(60) shouldBe false // field_bool_061 (61 % 2 == 0 -> false)
    row.getBoolean(61) shouldBe true // field_bool_062 (62 % 2 == 0 -> true)

    // Verify string fields
    row.getUTF8String(70).toString shouldBe "test_string_field_71" // field_string_071
    row.getUTF8String(79).toString shouldBe "test_string_field_80" // field_string_080

    // Verify bytes fields
    val bytesField = row.getBinary(80) // field_bytes_081
    new String(bytesField, "UTF-8") shouldBe "test_bytes_81"
  }

  test("Generated WireFormat converter should handle repeated fields correctly") {
    val converter = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val row = converter.parse(binaryData)

    // Verify repeated int32 field
    val repeatedInt32Array = row.getArray(85) // repeated_int32_086
    repeatedInt32Array.numElements() shouldBe 120
    (0 until 120).foreach { i =>
      repeatedInt32Array.getInt(i) shouldBe (i + 1) * 10
    }

    // Verify repeated string field
    val repeatedStringArray = row.getArray(110) // repeated_string_111
    repeatedStringArray.numElements() shouldBe 12
    (0 until 12).foreach { i =>
      repeatedStringArray.getUTF8String(i).toString shouldBe s"repeated_string_${i + 1}"
    }
  }

  test("Compiled message converter should produce same results as WireFormat") {
    val WireFormatParser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val compiledParser = ProtoToRowGenerator.generateParser(descriptor, classOf[SimpleBenchmarkProtos.SimpleMessage], sparkSchema)

    val wireFormatRow = WireFormatParser.parse(binaryData)
    val compiledRow = compiledParser.parse(binaryData)

    // Both should produce identical results
    wireFormatRow.numFields shouldBe compiledRow.numFields

    // Compare some key fields
    (0 until 20).foreach { i =>
      wireFormatRow.getInt(i) shouldBe compiledRow.getInt(i)
    }

    (20 until 40).foreach { i =>
      wireFormatRow.getLong(i) shouldBe compiledRow.getLong(i)
    }

    (70 until 80).foreach { i =>
      wireFormatRow.getUTF8String(i).toString shouldBe compiledRow.getUTF8String(i).toString
    }
  }

  test("WireFormat converter should be reusable") {
    val converter = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)

    // Convert the same data multiple times
    val row1 = converter.parse(binaryData)
    val row2 = converter.parse(binaryData)
    val row3 = converter.parse(binaryData)

    // All results should be identical
    row1.numFields shouldBe row2.numFields
    row2.numFields shouldBe row3.numFields

    // Spot check some values
    row1.getInt(0) shouldBe row2.getInt(0)
    row2.getInt(0) shouldBe row3.getInt(0)

    row1.getUTF8String(70).toString shouldBe row2.getUTF8String(70).toString
    row2.getUTF8String(70).toString shouldBe row3.getUTF8String(70).toString
  }

  test("WireFormat converter should handle deterministic binary data") {
    // Generate the same message multiple times
    val message1 = TestDataGenerator.createSimpleMessage()
    val message2 = TestDataGenerator.createSimpleMessage()

    // Binary data should be identical
    message1.toByteArray shouldBe message2.toByteArray

    val converter = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val row1 = converter.parse(message1.toByteArray)
    val row2 = converter.parse(message2.toByteArray)

    // Results should be identical
    row1.getInt(0) shouldBe row2.getInt(0)
    row1.getLong(20) shouldBe row2.getLong(20)
    row1.getUTF8String(70).toString shouldBe row2.getUTF8String(70).toString
  }

  test("Schema should correctly represent all field types") {
    import org.apache.spark.sql.types._

    // Verify the schema structure
    sparkSchema.fields.length shouldBe 120

    // Check int32 fields (0-19)
    (0 until 20).foreach { i =>
      sparkSchema.fields(i).dataType shouldBe IntegerType
    }

    // Check int64 fields (20-39)
    (20 until 40).foreach { i =>
      sparkSchema.fields(i).dataType shouldBe LongType
    }

    // Check float fields (40-49)
    (40 until 50).foreach { i =>
      sparkSchema.fields(i).dataType shouldBe FloatType
    }

    // Check double fields (50-59)
    (50 until 60).foreach { i =>
      sparkSchema.fields(i).dataType shouldBe DoubleType
    }

    // Check boolean fields (60-69)
    (60 until 70).foreach { i =>
      sparkSchema.fields(i).dataType shouldBe BooleanType
    }

    // Check string fields (70-79)
    (70 until 80).foreach { i =>
      sparkSchema.fields(i).dataType shouldBe StringType
    }

    // Check bytes fields (80-84)
    (80 until 85).foreach { i =>
      sparkSchema.fields(i).dataType shouldBe BinaryType
    }

    // Check repeated fields are ArrayType
    sparkSchema.fields(85).dataType shouldBe ArrayType(IntegerType, containsNull = false) // repeated_int32_086
    sparkSchema.fields(110).dataType shouldBe ArrayType(StringType, containsNull = false) // repeated_string_111
  }
}