package benchmark

import fastproto.ProtoToRowGenerator
import org.apache.spark.sql.protobuf.backport.utils.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Debug test to isolate the wireformat parsing issue.
 */
class SimpleMessageDebugTest extends AnyFunSuite with Matchers {

  test("TestDataGenerator should create valid protobuf binary") {
    val message = TestDataGenerator.createSimpleMessage()
    message should not be null

    val binary = message.toByteArray
    binary.length should be > 0

    println(s"Generated binary length: ${binary.length}")
    println(s"First few bytes: ${binary.take(10).map(b => f"$b%02x").mkString(" ")}")

    // Verify we can parse it back
    val reparsed = SimpleBenchmarkProtos.SimpleMessage.parseFrom(binary)
    reparsed.getFieldInt32001 shouldBe 100
    reparsed.getFieldInt32002 shouldBe 200
  }

  test("Compiled converter should work with generated data") {
    val message = TestDataGenerator.createSimpleMessage()
    val binary = message.toByteArray
    val descriptor = message.getDescriptorForType
    val sparkSchema = SchemaConverters.toSqlType(descriptor).dataType.asInstanceOf[StructType]

    val parser = ProtoToRowGenerator.generateParser(descriptor, classOf[SimpleBenchmarkProtos.SimpleMessage], sparkSchema)
    val row = parser.parse(binary)

    row should not be null
    row.numFields shouldBe 120
    row.getInt(0) shouldBe 100    // field_int32_001
    row.getInt(1) shouldBe 200    // field_int32_002
  }
}