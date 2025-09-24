package benchmark

import fastproto.{ProtoToRowGenerator, WireFormatToRowGenerator}
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for ComplexMessage wireformat code generation path.
 *
 * Tests the optimized wireformat parser generation for complex messages
 * with nested structures and recursive relationships (A ↔ B).
 */
class ComplexMessageTest extends AnyFunSuite with Matchers {

  private val testMessage = TestDataGenerator.createComplexMessage()
  private val binaryData = testMessage.toByteArray
  private val descriptor = testMessage.getDescriptorForType
  private val sparkSchema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

  test("ComplexMessage should have nested structure fields") {
    // ComplexMessageA has these main fields: id, name, value, active, data, numbers, tags, measurements, flags,
    // message_b, timestamp, ratio, description, metadata, timestamps, ratios, chunks, nested_messages
    sparkSchema.fields.length should be >= 15

    // Find the message_b field (should be a struct)
    val messageBField = sparkSchema.fields.find(_.name == "message_b")
    messageBField shouldBe defined
    messageBField.get.dataType shouldBe a[StructType]
  }

  test("Generated WireFormat parser should handle ComplexMessage") {
    val parser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val row = parser.parse(binaryData)

    row should not be null

    // Verify scalar fields
    row.getInt(0) shouldBe 42  // id
    row.getUTF8String(1).toString shouldBe "test_message_a"  // name
    row.getDouble(2) shouldBe 123.456  // value
    row.getBoolean(3) shouldBe true  // active

    // Verify bytes field
    val dataField = row.getBinary(4)  // data
    new String(dataField, "UTF-8") shouldBe "deterministic_data_a"

    // Verify repeated numbers field
    val numbersArray = row.getArray(5)  // numbers
    numbersArray.numElements() shouldBe 3
    (0 until 3).foreach { i =>
      numbersArray.getInt(i) shouldBe (i + 1) * 100
    }

    // Verify repeated tags field
    val tagsArray = row.getArray(6)  // tags
    tagsArray.numElements() shouldBe 3
    tagsArray.getUTF8String(0).toString shouldBe "performance"
    tagsArray.getUTF8String(1).toString shouldBe "test"
    tagsArray.getUTF8String(2).toString shouldBe "protobuf"
  }

  test("Generated WireFormat parser should handle nested MessageB") {
    val parser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val row = parser.parse(binaryData)

    // Find the message_b field index
    val messageBFieldIndex = sparkSchema.fieldIndex("message_b")
    val messageBRow = row.getStruct(messageBFieldIndex, sparkSchema.fields(messageBFieldIndex).dataType.asInstanceOf[StructType].size)

    messageBRow should not be null

    // Verify MessageB fields
    messageBRow.getLong(0) shouldBe 9876543210L  // identifier
    messageBRow.getUTF8String(1).toString shouldBe "test_message_b"  // label
    messageBRow.getFloat(2) shouldBe 87.65f  // score
    messageBRow.getBoolean(3) shouldBe true  // enabled

    // Verify MessageB payload field
    val payloadField = messageBRow.getBinary(4)  // payload
    new String(payloadField, "UTF-8") shouldBe "deterministic_payload_b"
  }

  test("Generated WireFormat parser should handle nested NestedData") {
    val parser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val row = parser.parse(binaryData)

    // Navigate to message_b -> nested_data
    val messageBFieldIndex = sparkSchema.fieldIndex("message_b")
    val messageBRow = row.getStruct(messageBFieldIndex, sparkSchema.fields(messageBFieldIndex).dataType.asInstanceOf[StructType].size)

    val messageBSchema = sparkSchema.fields(messageBFieldIndex).dataType.asInstanceOf[StructType]
    val nestedDataFieldIndex = messageBSchema.fieldIndex("nested_data")
    val nestedDataRow = messageBRow.getStruct(nestedDataFieldIndex, messageBSchema.fields(nestedDataFieldIndex).dataType.asInstanceOf[StructType].size)

    nestedDataRow should not be null

    // Verify NestedData fields
    nestedDataRow.getUTF8String(0).toString shouldBe "deterministic_key"  // key
    nestedDataRow.getUTF8String(1).toString shouldBe "deterministic_value"  // value
    nestedDataRow.getInt(2) shouldBe 777  // count
    nestedDataRow.getDouble(3) shouldBe 555.333  // average
    nestedDataRow.getBoolean(4) shouldBe true  // valid

    // Verify NestedData repeated fields
    val keysArray = nestedDataRow.getArray(5)  // keys
    keysArray.numElements() shouldBe 2
    keysArray.getUTF8String(0).toString shouldBe "key1"
    keysArray.getUTF8String(1).toString shouldBe "key2"
  }

  ignore("Compiled message parser should produce same results as WireFormat") {
    val WireFormatParser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val compiledParser = ProtoToRowGenerator.generateParser(descriptor, classOf[ComplexBenchmarkProtos.ComplexMessageA], sparkSchema)

    val wireFormatRow = WireFormatParser.parse(binaryData)
    val compiledRow = compiledParser.parse(binaryData)

    // Both should produce identical results for scalar fields
    wireFormatRow.numFields shouldBe compiledRow.numFields

    // Compare scalar fields
    wireFormatRow.getInt(0) shouldBe compiledRow.getInt(0)  // id
    wireFormatRow.getUTF8String(1).toString shouldBe compiledRow.getUTF8String(1).toString  // name
    wireFormatRow.getDouble(2) shouldBe compiledRow.getDouble(2)  // value
    wireFormatRow.getBoolean(3) shouldBe compiledRow.getBoolean(3)  // active

    // Compare binary fields
    wireFormatRow.getBinary(4) shouldBe compiledRow.getBinary(4)  // data
  }

  test("WireFormat parser should handle repeated nested messages") {
    val parser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val row = parser.parse(binaryData)

    // Find the nested_messages field (repeated MessageB)
    val nestedMessagesFieldIndex = sparkSchema.fieldIndex("nested_messages")
    val nestedMessagesArray = row.getArray(nestedMessagesFieldIndex)

    nestedMessagesArray.numElements() shouldBe 1  // We add one MessageB to the repeated field

    // Verify the first (and only) nested message
    val messageBSchema = sparkSchema.fields(nestedMessagesFieldIndex).dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType]
      .elementType.asInstanceOf[StructType]
    val nestedMessageRow = nestedMessagesArray.getStruct(0, messageBSchema.size)

    nestedMessageRow.getLong(0) shouldBe 9876543210L  // identifier
    nestedMessageRow.getUTF8String(1).toString shouldBe "test_message_b"  // label
  }

  test("WireFormat parser should be reusable for complex messages") {
    val parser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)

    // Convert the same data multiple times
    val row1 = parser.parse(binaryData)
    val row2 = parser.parse(binaryData)
    val row3 = parser.parse(binaryData)

    // All results should be identical
    row1.numFields shouldBe row2.numFields
    row2.numFields shouldBe row3.numFields

    // Spot check some values
    row1.getInt(0) shouldBe row2.getInt(0)
    row2.getInt(0) shouldBe row3.getInt(0)

    row1.getUTF8String(1).toString shouldBe row2.getUTF8String(1).toString
    row2.getUTF8String(1).toString shouldBe row3.getUTF8String(1).toString
  }

  test("WireFormat parser should handle deterministic complex binary data") {
    // Generate the same message multiple times
    val message1 = TestDataGenerator.createComplexMessage()
    val message2 = TestDataGenerator.createComplexMessage()

    // Binary data should be identical
    message1.toByteArray shouldBe message2.toByteArray

    val parser = WireFormatToRowGenerator.generateParser(descriptor, sparkSchema)
    val row1 = parser.parse(message1.toByteArray)
    val row2 = parser.parse(message2.toByteArray)

    // Results should be identical
    row1.getInt(0) shouldBe row2.getInt(0)  // id
    row1.getUTF8String(1).toString shouldBe row2.getUTF8String(1).toString  // name
    row1.getDouble(2) shouldBe row2.getDouble(2)  // value
  }

  test("Complex schema should correctly represent nested structures") {
    import org.apache.spark.sql.types._

    // Verify message_b field is a StructType
    val messageBField = sparkSchema.fields.find(_.name == "message_b")
    messageBField shouldBe defined
    messageBField.get.dataType shouldBe a[StructType]

    val messageBSchema = messageBField.get.dataType.asInstanceOf[StructType]
    messageBSchema.fields.length should be >= 10

    // Verify nested_data field within message_b is also a StructType
    val nestedDataField = messageBSchema.fields.find(_.name == "nested_data")
    nestedDataField shouldBe defined
    nestedDataField.get.dataType shouldBe a[StructType]

    // Verify repeated fields are ArrayType
    val numbersField = sparkSchema.fields.find(_.name == "numbers")
    numbersField shouldBe defined
    numbersField.get.dataType shouldBe ArrayType(IntegerType, containsNull = false)

    val tagsField = sparkSchema.fields.find(_.name == "tags")
    tagsField shouldBe defined
    tagsField.get.dataType shouldBe ArrayType(StringType, containsNull = false)
  }

  test("Schema field order should be consistent") {
    // The schema should have predictable field ordering for reliable access
    sparkSchema.fieldNames should contain allOf("id", "name", "value", "active", "data", "numbers", "tags", "message_b")

    // First few fields should be in expected order
    sparkSchema.fields(0).name shouldBe "id"
    sparkSchema.fields(1).name shouldBe "name"
    sparkSchema.fields(2).name shouldBe "value"
    sparkSchema.fields(3).name shouldBe "active"
    sparkSchema.fields(4).name shouldBe "data"
  }
}