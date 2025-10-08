package fastproto

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test suite for GrowableArrayWriter to verify dynamic capacity expansion
 * and data preservation during growth.
 */
class GrowableArrayWriterSpec extends AnyFlatSpec with Matchers {

  "GrowableArrayWriter" should "initialize with given capacity" in {
    val rowWriter = new UnsafeRowWriter(1, 64)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(10)
    writer.getCapacity shouldBe 10
    writer.getCount shouldBe 0
  }

  it should "track count as highest ordinal + 1" in {
    val rowWriter = new UnsafeRowWriter(1, 64)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(10)
    writer.write(0, 100L)
    writer.getCount shouldBe 1

    writer.write(5, 200L)
    writer.getCount shouldBe 6  // Highest ordinal (5) + 1

    writer.write(3, 150L)
    writer.getCount shouldBe 6  // Still 6, didn't write beyond ordinal 5
  }

  it should "auto-grow when writing beyond capacity" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(5)  // Start with capacity of 5
    writer.getCapacity shouldBe 5

    // Write within capacity
    writer.write(0, 10)
    writer.write(4, 50)
    writer.getCapacity shouldBe 5

    // Write beyond capacity - should trigger growth
    writer.write(10, 100)
    writer.getCapacity should be >= 11  // Should have grown to at least 11
    writer.getCount shouldBe 11
  }

  it should "preserve existing data when growing" in {
    val rowWriter = new UnsafeRowWriter(1, 512)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(3)

    // Write some initial data
    writer.write(0, 100L)
    writer.write(1, 200L)
    writer.write(2, 300L)

    // Trigger growth by writing beyond capacity
    writer.write(10, 999L)

    // Complete to finalize the array
    val count = writer.complete()
    count shouldBe 11

    // Read back the array data and verify preservation
    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, writer.getStartingOffset, rowWriter.totalSize())

    arrayData.numElements() shouldBe 11
    arrayData.getLong(0) shouldBe 100L
    arrayData.getLong(1) shouldBe 200L
    arrayData.getLong(2) shouldBe 300L
    arrayData.getLong(10) shouldBe 999L
  }

  it should "support multiple growth cycles" in {
    val rowWriter = new UnsafeRowWriter(1, 512)
    val writer = new GrowableArrayWriter(rowWriter, 4)  // 4-byte elements (int)

    writer.initialize(2)
    writer.getCapacity shouldBe 2

    // First growth
    writer.write(5, 50)
    val capacity1 = writer.getCapacity
    capacity1 should be >= 6

    // Second growth
    writer.write(15, 150)
    val capacity2 = writer.getCapacity
    capacity2 should be >= 16
    capacity2 should be > capacity1

    // Verify all data preserved
    val count = writer.complete()
    count shouldBe 16

    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, writer.getStartingOffset, rowWriter.totalSize())

    arrayData.getInt(5) shouldBe 50
    arrayData.getInt(15) shouldBe 150
  }

  it should "handle primitive types correctly" in {
    // Test with different element sizes
    val longRowWriter = new UnsafeRowWriter(1, 256)
    val longWriter = new GrowableArrayWriter(longRowWriter, 8)
    longWriter.initialize(5)
    longWriter.write(0, 100L)
    longWriter.write(10, 999L)  // Trigger growth
    longWriter.complete() shouldBe 11

    // Test with int (4 bytes)
    val intRowWriter = new UnsafeRowWriter(1, 256)
    val intWriter = new GrowableArrayWriter(intRowWriter, 4)
    intWriter.initialize(3)
    intWriter.write(0, 42)
    intWriter.write(5, 84)  // Trigger growth
    intWriter.complete() shouldBe 6

    // Test with double (8 bytes)
    val doubleRowWriter = new UnsafeRowWriter(1, 256)
    val doubleWriter = new GrowableArrayWriter(doubleRowWriter, 8)
    doubleWriter.initialize(2)
    doubleWriter.write(0, 3.14)
    doubleWriter.write(4, 2.71)  // Trigger growth
    doubleWriter.complete() shouldBe 5
  }

  it should "handle null values correctly" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(3)
    writer.write(0, 100L)
    writer.setNull8Bytes(1)  // Set ordinal 1 to null
    writer.write(2, 300L)

    val count = writer.complete()
    count shouldBe 3

    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, writer.getStartingOffset, rowWriter.totalSize())

    arrayData.isNullAt(0) shouldBe false
    arrayData.isNullAt(1) shouldBe true
    arrayData.isNullAt(2) shouldBe false
    arrayData.getLong(0) shouldBe 100L
    arrayData.getLong(2) shouldBe 300L
  }

  it should "forbid double finalization" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(5)
    writer.write(0, 100L)
    writer.complete()

    // Second complete should throw
    assertThrows[IllegalStateException] {
      writer.complete()
    }
  }

  it should "support sparse arrays" in {
    val rowWriter = new UnsafeRowWriter(1, 2048)  // Need more space for sparse array
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(3)

    // Write only to ordinals 0 and 100, skipping everything in between
    writer.write(0, 1L)
    writer.write(100, 101L)

    val count = writer.complete()
    count shouldBe 101  // Array size is max ordinal + 1

    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, writer.getStartingOffset, rowWriter.totalSize())

    arrayData.numElements() shouldBe 101
    arrayData.getLong(0) shouldBe 1L
    arrayData.getLong(100) shouldBe 101L
    // Middle elements should be null/zero (default initialized)
  }

  it should "forbid variable-length data writes" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(5)

    // UTF8String writes should throw UnsupportedOperationException
    val utf8 = UTF8String.fromString("test")

    assertThrows[UnsupportedOperationException] {
      writer.write(0, utf8)
    }

    // Byte array writes should also throw
    assertThrows[UnsupportedOperationException] {
      writer.write(0, Array[Byte](1, 2, 3))
    }

    // Count should still be 0 since no writes succeeded
    writer.getCount shouldBe 0
  }

  it should "handle Decimal values within capacity" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(5)

    // Small decimal (fits in long)
    val smallDecimal = Decimal(12345, 10, 2)
    writer.write(0, smallDecimal, 10, 2)

    val count = writer.complete()
    count shouldBe 1

    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, writer.getStartingOffset, rowWriter.totalSize())

    val result = arrayData.getDecimal(0, 10, 2)
    result.toJavaBigDecimal shouldBe smallDecimal.toJavaBigDecimal
  }

  it should "double capacity when growing by default" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(4)
    writer.getCapacity shouldBe 4

    // Write to ordinal 5 (needs capacity of at least 6)
    writer.write(5, 100L)

    // Should double: 4 * 2 = 8
    writer.getCapacity shouldBe 8

    // Write to ordinal 15 (needs capacity of at least 16)
    writer.write(15, 200L)

    // Should double: 8 * 2 = 16
    writer.getCapacity shouldBe 16
  }

  it should "use minCapacity when doubling is insufficient" in {
    val rowWriter = new UnsafeRowWriter(1, 512)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.initialize(4)
    writer.getCapacity shouldBe 4

    // Jump way beyond double capacity (4 * 2 = 8)
    // Writing to ordinal 20 needs capacity of 21
    writer.write(20, 100L)

    // Should use max(4*2, 21) = 21
    writer.getCapacity should be >= 21
  }
}
