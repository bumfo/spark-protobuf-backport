package benchmark

import com.google.protobuf.CodedInputStream
import fastproto._
import org.scalatest.flatspec.AnyFlatSpec

/**
 * Debug test to understand packed array performance differences between parsers.
 */
class PackedArrayDebugTest extends AnyFlatSpec {

  "Packed array parsing" should "show performance characteristics" in {
    val sizes = Seq(1, 100, 10000)

    for (size <- sizes) {
      val message = TestDataGenerator.createScalarArrayMessage(size)
      val binary = message.toByteArray

      println(s"\n=== Size: $size, Binary size: ${binary.length} bytes ===")

      // Analyze the wire format
      val input = CodedInputStream.newInstance(binary)
      var fieldCount = 0
      while (!input.isAtEnd) {
        val tag = input.readTag()
        val fieldNum = tag >>> 3
        val wireType = tag & 0x7

        if (wireType == 2) { // LENGTH_DELIMITED
          val length = input.readRawVarint32()
          println(s"Field $fieldNum: packed, length=$length bytes")
          input.skipRawBytes(length)
        } else {
          println(s"Field $fieldNum: unpacked, wireType=$wireType")
          input.skipField(tag)
        }
        fieldCount += 1
      }
      println(s"Total field tags in message: $fieldCount")
    }

    // Now look at the key difference
    println("\n=== Key Differences ===")
    println("WireFormatParser:")
    println("  - Uses FastList accumulators (IntList, LongList, etc)")
    println("  - parsePackedVarint32s() manages list growth internally")
    println("  - Single UnsafeArrayWriter created after all parsing")
    println("  - Two-phase: accumulate then write")

    println("\nInlineParser (ProtoRuntime):")
    println("  - Uses single PrimitiveArrayWriter with ArrayContext")
    println("  - collectPackedInt32() gets/creates writer, then loops")
    println("  - PrimitiveArrayWriter grows during parsing")
    println("  - Single-phase: write directly during parsing")

    println("\n=== Performance Impact ===")
    println("PrimitiveArrayWriter overhead:")
    println("  - getOrCreate() check on every packed field")
    println("  - 16-byte header reservation on creation")
    println("  - Growth during parsing may cause buffer moves")
    println("  - completeIfActive() called for each variable-length field")

    println("\nFastList advantages:")
    println("  - Pre-sized based on packed length hint")
    println("  - No header overhead during accumulation")
    println("  - Single UnsafeArrayWriter.initialize() with exact size")
    println("  - Batch write at end with known size")
  }

  "ProtoRuntime.collectPackedInt32" should "show implementation" in {
    println("\n=== ProtoRuntime.collectPackedInt32 Implementation ===")
    println("""
    |public static void collectPackedInt32(CodedInputStream in, ArrayContext ctx,
    |                                       NullDefaultRowWriter parent, int ordinal) {
    |  PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    |  int length = in.readRawVarint32();
    |  int limit = in.pushLimit(length);
    |  while (in.getBytesUntilLimit() > 0) {
    |    w.writeInt(in.readRawVarint32());
    |  }
    |  in.popLimit(limit);
    |}
    """.stripMargin)

    println("\n=== WireFormatParser.parsePackedVarint32s Implementation ===")
    println("""
    |protected static void parsePackedVarint32s(CodedInputStream input, IntList list) {
    |  int packedLength = input.readRawVarint32();
    |  int oldLimit = input.pushLimit(packedLength);
    |
    |  int[] array = list.array;
    |  int count = list.count;
    |
    |  while (input.getBytesUntilLimit() > 0) {
    |    if (count >= array.length) {
    |      list.grow(count);
    |      array = list.array;
    |    }
    |    array[count++] = input.readRawVarint32();
    |  }
    |
    |  list.count = count;
    |  input.popLimit(oldLimit);
    |}
    """.stripMargin)
  }
}