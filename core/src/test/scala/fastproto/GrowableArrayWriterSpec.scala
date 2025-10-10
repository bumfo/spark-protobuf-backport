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

  "GrowableArrayWriter" should "accept capacity hint via sizeHint" in {
    val rowWriter = new UnsafeRowWriter(1, 64)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.sizeHint(10)
    writer.getCapacity shouldBe 10
    writer.getCount shouldBe 0
  }

  it should "track count as highest ordinal + 1" in {
    val rowWriter = new UnsafeRowWriter(1, 64)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.sizeHint(10)
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

    writer.sizeHint(5)  // Start with capacity of 5
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

    writer.sizeHint(3)

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

    writer.sizeHint(2)
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
    longWriter.sizeHint(5)
    longWriter.write(0, 100L)
    longWriter.write(10, 999L)  // Trigger growth
    longWriter.complete() shouldBe 11

    // Test with int (4 bytes)
    val intRowWriter = new UnsafeRowWriter(1, 256)
    val intWriter = new GrowableArrayWriter(intRowWriter, 4)
    intWriter.sizeHint(3)
    intWriter.write(0, 42)
    intWriter.write(5, 84)  // Trigger growth
    intWriter.complete() shouldBe 6

    // Test with double (8 bytes)
    val doubleRowWriter = new UnsafeRowWriter(1, 256)
    val doubleWriter = new GrowableArrayWriter(doubleRowWriter, 8)
    doubleWriter.sizeHint(2)
    doubleWriter.write(0, 3.14)
    doubleWriter.write(4, 2.71)  // Trigger growth
    doubleWriter.complete() shouldBe 5
  }

  it should "handle null values correctly" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.sizeHint(3)
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

    writer.sizeHint(5)
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

    writer.sizeHint(3)

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

    writer.sizeHint(5)

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

    writer.sizeHint(5)

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

  it should "use hybrid growth strategy (linear < 768, 1.5x >= 768)" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.sizeHint(4)
    writer.getCapacity shouldBe 4

    // Write to ordinal 5 (needs capacity of at least 6)
    writer.write(5, 100L)

    // Small array: linear growth (exactly what's needed)
    writer.getCapacity shouldBe 6

    // Write to ordinal 15 (needs capacity of at least 16)
    writer.write(15, 200L)

    // Still small array: linear growth
    writer.getCapacity shouldBe 16
  }

  it should "handle large jumps in ordinal efficiently" in {
    val rowWriter = new UnsafeRowWriter(1, 512)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.sizeHint(4)
    writer.getCapacity shouldBe 4

    // Jump to ordinal 20 (needs capacity of 21)
    writer.write(20, 100L)

    // Small array: linear growth, allocates exactly what's needed
    writer.getCapacity shouldBe 21
  }

  it should "allocate on sizeHint" in {
    val rowWriter = new UnsafeRowWriter(1, 256)

    // Get cursor position after row writer construction
    val cursorAfterRowWriter = rowWriter.cursor()

    val writer = new GrowableArrayWriter(rowWriter, 8)

    // sizeHint now allocates immediately
    writer.sizeHint(10)
    val cursorAfterHint = rowWriter.cursor()

    // Cursor should have moved (allocation happened)
    cursorAfterHint should be > cursorAfterRowWriter

    // Write data
    writer.write(0, 100L)
    val cursorAfterWrite = rowWriter.cursor()

    // Cursor should not have moved significantly (no reallocation)
    cursorAfterWrite shouldBe cursorAfterHint

    // Verify capacity is correct
    writer.getCapacity shouldBe 10
    writer.getCount shouldBe 1
  }

  it should "handle empty arrays without wasting space" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    // Don't call sizeHint or write anything
    val cursorBefore = rowWriter.cursor()

    // Complete without any writes - allocates minimal capacity (0)
    val count = writer.complete()
    count shouldBe 0

    // Space should have been allocated for empty array
    val cursorAfter = rowWriter.cursor()
    cursorAfter should be > cursorBefore

    // Verify it's a valid empty array with minimal capacity
    writer.getCapacity shouldBe 0

    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, writer.getStartingOffset, rowWriter.totalSize())
    arrayData.numElements() shouldBe 0
  }

  it should "work without calling sizeHint()" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    // Don't call sizeHint() - allocates exactly what's needed
    // Write some values
    writer.write(0, 100L)
    writer.write(5, 500L)

    // After growth, capacity should be 6 (linear growth for small arrays)
    writer.getCapacity shouldBe 6

    val count = writer.complete()
    count shouldBe 6

    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, writer.getStartingOffset, rowWriter.totalSize())
    arrayData.numElements() shouldBe 6
    arrayData.getLong(0) shouldBe 100L
    arrayData.getLong(5) shouldBe 500L
  }

  it should "allow multiple sizeHint calls" in {
    val rowWriter = new UnsafeRowWriter(1, 512)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    // Multiple hints before allocation - takes max
    writer.sizeHint(5)
    writer.sizeHint(3)   // Smaller hint is ignored
    writer.sizeHint(10)  // Larger hint is used
    writer.getCapacity shouldBe 10

    // Write to trigger allocation
    writer.write(0, 100L)
    writer.write(5, 500L)

    // Can still call sizeHint after allocation - grows if needed
    writer.sizeHint(20)
    writer.getCapacity shouldBe 20

    // Data should be preserved
    writer.write(15, 1500L)
    val count = writer.complete()
    count shouldBe 16

    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, writer.getStartingOffset, rowWriter.totalSize())
    arrayData.getLong(0) shouldBe 100L
    arrayData.getLong(5) shouldBe 500L
    arrayData.getLong(15) shouldBe 1500L
  }

  it should "use 1.5x growth for large arrays (>= 768 elements)" in {
    val rowWriter = new UnsafeRowWriter(1, 32768)  // Need large buffer for large array
    val writer = new GrowableArrayWriter(rowWriter, 8)

    // Build up to capacity just above threshold
    writer.sizeHint(800)
    writer.write(0, 1L)  // Trigger allocation
    writer.getCapacity shouldBe 832  // Aligned to next 64-element boundary (13 * 64)

    // Write to trigger 1.5x growth (since capacity >= 768)
    writer.write(832, 832L)  // Triggers growth

    // Should use 1.5x growth: max(832 + 416, 833) = 1248, aligned to 1280 (20 * 64)
    writer.getCapacity shouldBe 1280

    // Verify growth strategy used
    val count = writer.complete()
    count shouldBe 833
  }
}
