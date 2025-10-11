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
    writer.getCapacity shouldBe 64  // Header capacity rounds up to next power of 2 (min 64)
    writer.getCount shouldBe 10  // Allocated space for 10 elements
  }

  it should "track count as allocated space (grows to highest ordinal + 1)" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    // No sizeHint - count grows on demand
    writer.write(0, 100L)
    writer.getCount shouldBe 1  // Allocated 1 slot for ordinal 0

    writer.write(5, 200L)
    writer.getCount shouldBe 6  // Grew to 6 slots for ordinal 5

    writer.write(3, 150L)
    writer.getCount shouldBe 6  // Still 6, ordinal 3 within capacity

    writer.write(10, 250L)
    writer.getCount shouldBe 11  // Grew to 11 slots for ordinal 10
  }

  it should "auto-grow when writing beyond capacity" in {
    val rowWriter = new UnsafeRowWriter(1, 512)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.sizeHint(5)  // Allocate 5 slots, header capacity=64
    writer.getCapacity shouldBe 64
    writer.getCount shouldBe 5

    // Write within allocated capacity
    writer.write(0, 10)
    writer.write(4, 50)
    writer.getCount shouldBe 5  // Still 5, within capacity

    // Write beyond allocated capacity - should trigger growth
    writer.write(100, 100)
    writer.getCapacity shouldBe 128  // Header capacity doubled to accommodate
    writer.getCount shouldBe 101  // Allocated 101 slots for ordinal 100
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
    writer.getCapacity shouldBe 64  // Min capacity

    // First growth - stays at 64
    writer.write(5, 50)
    val capacity1 = writer.getCapacity
    capacity1 shouldBe 64

    // Second growth - doubles to 128
    writer.write(100, 150)
    val capacity2 = writer.getCapacity
    capacity2 shouldBe 128
    capacity2 should be > capacity1

    // Verify all data preserved
    val count = writer.complete()
    count shouldBe 101

    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, writer.getStartingOffset, rowWriter.totalSize())

    arrayData.getInt(5) shouldBe 50
    arrayData.getInt(100) shouldBe 150
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

    // Don't call sizeHint - count starts at 0

    // UTF8String writes should throw UnsupportedOperationException
    val utf8 = UTF8String.fromString("test")

    assertThrows[UnsupportedOperationException] {
      writer.write(0, utf8)
    }

    // Byte array writes should also throw
    assertThrows[UnsupportedOperationException] {
      writer.write(0, Array[Byte](1, 2, 3))
    }

    // Count should still be 0 since no successful writes
    writer.getCount shouldBe 0
  }

  it should "handle Decimal values within capacity" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

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

  it should "use exponential growth strategy (double capacity)" in {
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.sizeHint(4)
    writer.getCapacity shouldBe 64  // Min capacity

    // Write within capacity - no growth
    writer.write(5, 100L)
    writer.getCapacity shouldBe 64

    // Write within capacity - no growth
    writer.write(15, 200L)
    writer.getCapacity shouldBe 64

    // Write beyond capacity - doubles to 128
    writer.write(100, 300L)
    writer.getCapacity shouldBe 128
  }

  it should "handle large jumps in ordinal efficiently" in {
    val rowWriter = new UnsafeRowWriter(1, 512)
    val writer = new GrowableArrayWriter(rowWriter, 8)

    writer.sizeHint(4)
    writer.getCapacity shouldBe 64  // Min capacity

    // Jump to ordinal 20 - within capacity
    writer.write(20, 100L)
    writer.getCapacity shouldBe 64  // No growth needed

    // Jump to ordinal 200 - exceeds capacity, doubles to 256
    writer.write(200, 200L)
    writer.getCapacity shouldBe 256
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

    // Verify capacity is correct (rounds up to 64)
    writer.getCapacity shouldBe 64
    writer.getCount shouldBe 10
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

    // Don't call sizeHint() - allocates on first write
    // Write some values
    writer.write(0, 100L)
    writer.write(5, 500L)

    // After growth, capacity should be 64 (min capacity)
    writer.getCapacity shouldBe 64

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

    // Multiple hints - rounds to min capacity (64)
    writer.sizeHint(5)
    writer.sizeHint(3)   // Smaller hint is ignored (already at 64)
    writer.sizeHint(10)  // Still rounds to 64
    writer.getCapacity shouldBe 64

    // Write to trigger allocation
    writer.write(0, 100L)
    writer.write(5, 500L)

    // Can still call sizeHint after allocation - grows if needed (rounds to 128)
    writer.sizeHint(100)
    writer.getCapacity shouldBe 128

    // Data should be preserved
    writer.write(100, 1500L)
    val count = writer.complete()
    count shouldBe 101

    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, writer.getStartingOffset, rowWriter.totalSize())
    arrayData.getLong(0) shouldBe 100L
    arrayData.getLong(5) shouldBe 500L
    arrayData.getLong(100) shouldBe 1500L
  }

  it should "use exponential doubling for large arrays" in {
    val rowWriter = new UnsafeRowWriter(1, 32768)  // Need large buffer for large array
    val writer = new GrowableArrayWriter(rowWriter, 8)

    // Build up to large capacity
    writer.sizeHint(800)
    writer.write(0, 1L)  // Trigger allocation
    writer.getCapacity shouldBe 1024  // Rounds up to next power of 2

    // Write within capacity - no growth
    writer.write(500, 500L)
    writer.getCapacity shouldBe 1024

    // Write beyond capacity - doubles to 2048
    writer.write(1500, 1500L)
    writer.getCapacity shouldBe 2048

    // Verify growth strategy used
    val count = writer.complete()
    count shouldBe 1501
  }
}
