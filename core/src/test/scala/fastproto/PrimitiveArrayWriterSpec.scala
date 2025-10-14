package fastproto

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrimitiveArrayWriterSpec extends AnyFlatSpec with Matchers {

  behavior of "PrimitiveArrayWriter"

  it should "write long array with no size hint" in {
    val rowWriter = new UnsafeRowWriter(1, 8192)
    val writer = new PrimitiveArrayWriter(rowWriter, 8, 0)

    writer.writeLong(100L)
    writer.writeLong(200L)
    writer.writeLong(300L)

    val offset = writer.getStartingOffset
    val count = writer.complete()
    count shouldBe 3

    // Verify the array is valid UnsafeArrayData
    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    arrayData.numElements() shouldBe 3
    arrayData.getLong(0) shouldBe 100L
    arrayData.getLong(1) shouldBe 200L
    arrayData.getLong(2) shouldBe 300L
  }

  it should "write int array with size hint" in {
    val rowWriter = new UnsafeRowWriter(1, 8192)
    val writer = new PrimitiveArrayWriter(rowWriter, 4, 5)

    writer.writeInt(10)
    writer.writeInt(20)
    writer.writeInt(30)
    writer.writeInt(40)
    writer.writeInt(50)

    val offset = writer.getStartingOffset
    val count = writer.complete()
    count shouldBe 5

    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    arrayData.numElements() shouldBe 5
    arrayData.getInt(0) shouldBe 10
    arrayData.getInt(1) shouldBe 20
    arrayData.getInt(2) shouldBe 30
    arrayData.getInt(3) shouldBe 40
    arrayData.getInt(4) shouldBe 50
  }

  it should "handle small arrays without data movement" in {
    // Arrays with ≤64 elements should have zero data movement
    val rowWriter = new UnsafeRowWriter(1, 8192)
    val writer = new PrimitiveArrayWriter(rowWriter, 8, 0)

    // Write exactly 64 elements
    for (i <- 0 until 64) {
      writer.writeLong(i.toLong)
    }

    val offset = writer.getStartingOffset
    val count = writer.complete()
    count shouldBe 64

    // Verify no null bitmap was inserted (header is exactly 8 bytes for ≤64 elements)
    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    arrayData.numElements() shouldBe 64

    // Verify all values
    for (i <- 0 until 64) {
      arrayData.getLong(i) shouldBe i.toLong
    }
  }

  it should "handle large arrays with data movement" in {
    // Arrays with >64 elements need data movement to insert null bitmap
    val rowWriter = new UnsafeRowWriter(1, 8192)
    val writer = new PrimitiveArrayWriter(rowWriter, 8, 0)

    // Write 65 elements (triggers data movement at complete)
    for (i <- 0 until 65) {
      writer.writeLong(i.toLong)
    }

    val offset = writer.getStartingOffset
    val count = writer.complete()
    count shouldBe 65

    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    arrayData.numElements() shouldBe 65

    // Verify all values after data movement
    for (i <- 0 until 65) {
      arrayData.getLong(i) shouldBe i.toLong
    }
  }

  it should "handle arrays at exact boundary sizes" in {
    // Test at boundaries: 128, 192, 256, etc.
    val testSizes = Seq(128, 192, 256)

    for (testSize <- testSizes) {
      val rowWriter = new UnsafeRowWriter(1, 65536)
      val writer = new PrimitiveArrayWriter(rowWriter, 4, testSize)

      for (i <- 0 until testSize) {
        writer.writeInt(i)
      }

      val offset = writer.getStartingOffset
      val count = writer.complete()
      count shouldBe testSize

      val size = rowWriter.cursor() - offset
      val arrayData = new UnsafeArrayData
      arrayData.pointTo(rowWriter.getBuffer, offset, size)
      arrayData.numElements() shouldBe testSize

      // Spot check some values
      arrayData.getInt(0) shouldBe 0
      arrayData.getInt(testSize / 2) shouldBe testSize / 2
      arrayData.getInt(testSize - 1) shouldBe testSize - 1
    }
  }

  it should "handle empty arrays" in {
    val rowWriter = new UnsafeRowWriter(1, 8192)
    val writer = new PrimitiveArrayWriter(rowWriter, 8, 0)

    // Don't write anything
    val offset = writer.getStartingOffset
    val count = writer.complete()
    count shouldBe 0

    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    arrayData.numElements() shouldBe 0
  }

  it should "write all primitive types correctly" in {
    val rowWriter = new UnsafeRowWriter(8, 8192)

    // Test each primitive type
    val longWriter = new PrimitiveArrayWriter(rowWriter, 8, 0)
    longWriter.writeLong(Long.MaxValue)
    longWriter.complete()

    val intWriter = new PrimitiveArrayWriter(rowWriter, 4, 0)
    intWriter.writeInt(Int.MaxValue)
    intWriter.complete()

    val doubleWriter = new PrimitiveArrayWriter(rowWriter, 8, 0)
    doubleWriter.writeDouble(3.14159)
    doubleWriter.complete()

    val floatWriter = new PrimitiveArrayWriter(rowWriter, 4, 0)
    floatWriter.writeFloat(2.71828f)
    floatWriter.complete()

    val shortWriter = new PrimitiveArrayWriter(rowWriter, 2, 0)
    shortWriter.writeShort(Short.MaxValue)
    shortWriter.complete()

    val byteWriter = new PrimitiveArrayWriter(rowWriter, 1, 0)
    byteWriter.writeByte(Byte.MaxValue)
    byteWriter.complete()

    val boolWriter = new PrimitiveArrayWriter(rowWriter, 1, 0)
    boolWriter.writeBoolean(true)
    boolWriter.writeBoolean(false)
    boolWriter.complete()

    // Verify boolean array
    val boolOffset = boolWriter.getStartingOffset
    val boolSize = rowWriter.cursor() - boolOffset
    val boolArray = new UnsafeArrayData
    boolArray.pointTo(rowWriter.getBuffer, boolOffset, boolSize)
    boolArray.numElements() shouldBe 2
    boolArray.getBoolean(0) shouldBe true
    boolArray.getBoolean(1) shouldBe false
  }

  it should "automatically grow buffer when needed" in {
    val rowWriter = new UnsafeRowWriter(1, 256) // Start with small buffer
    val writer = new PrimitiveArrayWriter(rowWriter, 8, 100) // Hint for 100 elements

    // Write 100 long values
    for (i <- 0 until 100) {
      writer.writeLong(i * 1000L)
    }

    val offset = writer.getStartingOffset
    val count = writer.complete()
    count shouldBe 100

    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    arrayData.numElements() shouldBe 100

    // Verify some values
    arrayData.getLong(0) shouldBe 0L
    arrayData.getLong(50) shouldBe 50000L
    arrayData.getLong(99) shouldBe 99000L
  }

  it should "handle large initial capacity correctly" in {
    val rowWriter = new UnsafeRowWriter(1, 16384)
    val writer = new PrimitiveArrayWriter(rowWriter, 8, 1000) // Large initial capacity

    // Only write a few elements
    writer.writeLong(1L)
    writer.writeLong(2L)
    writer.writeLong(3L)

    val offset = writer.getStartingOffset
    val count = writer.complete()
    count shouldBe 3

    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    arrayData.numElements() shouldBe 3
    arrayData.getLong(0) shouldBe 1L
    arrayData.getLong(1) shouldBe 2L
    arrayData.getLong(2) shouldBe 3L
  }

  it should "correctly track size" in {
    val rowWriter = new UnsafeRowWriter(1, 8192)
    val writer = new PrimitiveArrayWriter(rowWriter, 4, 0)

    writer.size() shouldBe 0

    writer.writeInt(1)
    writer.size() shouldBe 1

    writer.writeInt(2)
    writer.size() shouldBe 2

    writer.writeInt(3)
    writer.writeInt(4)
    writer.writeInt(5)
    writer.size() shouldBe 5

    writer.complete() shouldBe 5
  }

  it should "handle 1000+ elements with small initial buffer" in {
    // Start with small buffer (256 bytes) to force multiple buffer growth operations
    val rowWriter = new UnsafeRowWriter(1, 256)
    val writer = new PrimitiveArrayWriter(rowWriter, 8, 0)

    // Write 1000 long values
    for (i <- 0 until 1000) {
      writer.writeLong(i * 100L)
    }

    val offset = writer.getStartingOffset
    val count = writer.complete()
    count shouldBe 1000

    // Verify output is valid UnsafeArrayData
    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    arrayData.numElements() shouldBe 1000

    // Spot check some values
    arrayData.getLong(0) shouldBe 0L
    arrayData.getLong(100) shouldBe 10000L
    arrayData.getLong(500) shouldBe 50000L
    arrayData.getLong(999) shouldBe 99900L
  }
}