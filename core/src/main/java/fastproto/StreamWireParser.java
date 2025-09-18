package fastproto;

import com.google.protobuf.CodedInputStream;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

/**
 * Base class for stream-based wire format parsers that use CodedInputStream to parse protobuf wire format
 * directly into Spark SQL UnsafeRow structures.
 * <p>
 * Provides optimized array writing methods with size parameters to eliminate
 * array slicing and reduce memory allocations, plus CodedInputStream-specific parsing utilities.
 */
@SuppressWarnings("unused")
public abstract class StreamWireParser extends BufferSharingParser {

    public StreamWireParser(StructType schema) {
        super(schema);
    }

    /**
     * Convenience method that creates a CodedInputStream from binary data and delegates
     * to the abstract parseInto(CodedInputStream, UnsafeRowWriter) method.
     */
    @Override
    public final void parseInto(byte[] binary, UnsafeRowWriter writer) {
        CodedInputStream input = CodedInputStream.newInstance(binary);

        parseInto(input, writer);
    }

    /**
     * Abstract method for subclasses to implement CodedInputStream-based parsing logic.
     * Parses protobuf fields from the CodedInputStream and writes them to the UnsafeRowWriter.
     *
     * @param input  the CodedInputStream to read protobuf data from
     * @param writer the UnsafeRowWriter to populate with parsed field data
     */
    protected abstract void parseInto(CodedInputStream input, UnsafeRowWriter writer);

    // ========== Array Resizing Utilities ==========

    /**
     * Resize primitive int array when capacity is exceeded.
     */
    protected static int[] resizeIntArray(int[] array, int currentCount, int minSize) {
        int newSize = Math.max(array.length * 2, minSize);
        int[] newArray = new int[newSize];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        return newArray;
    }

    /**
     * Resize primitive long array when capacity is exceeded.
     */
    protected static long[] resizeLongArray(long[] array, int currentCount, int minSize) {
        int newSize = Math.max(array.length * 2, minSize);
        long[] newArray = new long[newSize];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        return newArray;
    }

    /**
     * Resize primitive float array when capacity is exceeded.
     */
    protected static float[] resizeFloatArray(float[] array, int currentCount, int minSize) {
        int newSize = Math.max(array.length * 2, minSize);
        float[] newArray = new float[newSize];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        return newArray;
    }

    /**
     * Resize primitive double array when capacity is exceeded.
     */
    protected static double[] resizeDoubleArray(double[] array, int currentCount, int minSize) {
        int newSize = Math.max(array.length * 2, minSize);
        double[] newArray = new double[newSize];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        return newArray;
    }

    /**
     * Resize primitive boolean array when capacity is exceeded.
     */
    protected static boolean[] resizeBooleanArray(boolean[] array, int currentCount, int minSize) {
        int newSize = Math.max(array.length * 2, minSize);
        boolean[] newArray = new boolean[newSize];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        return newArray;
    }

    /**
     * Resize byte array array when capacity is exceeded.
     */
    protected static byte[][] resizeByteArrayArray(byte[][] array, int currentCount, int minSize) {
        int newSize = Math.max(array.length * 2, minSize);
        byte[][] newArray = new byte[newSize][];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        return newArray;
    }

    // ========== String Array Methods ==========

    /**
     * Write a repeated string field as an array to the UnsafeRow.
     * Optimized for direct byte array writing without UTF8String allocation per element.
     */
    protected void writeStringArray(byte[][] values, int ordinal, UnsafeRowWriter writer) {
        writeStringArray(values, values.length, ordinal, writer);
    }

    /**
     * Write a repeated string field as an array to the UnsafeRow with size parameter.
     * Eliminates array slicing by using size parameter instead of values.length.
     * Writes bytes directly without UTF8String wrapper allocation.
     */
    protected void writeStringArray(byte[][] values, int size, int ordinal, UnsafeRowWriter writer) {
        assert size <= values.length;

        int offset = writer.cursor();
        UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8); // Variable-length strings use 8-byte offset/size
        arrayWriter.initialize(size);

        for (int i = 0; i < size; i++) {
            // Write bytes directly without UTF8String wrapper
            arrayWriter.write(i, values[i]);
        }

        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    // ========== Bytes Array Methods ==========

    /**
     * Write a repeated byte array field as an array to the UnsafeRow.
     */
    protected void writeBytesArray(byte[][] values, int ordinal, UnsafeRowWriter writer) {
        writeBytesArray(values, values.length, ordinal, writer);
    }

    /**
     * Write a repeated byte array field as an array to the UnsafeRow with size parameter.
     * Eliminates array slicing by using size parameter instead of values.length.
     */
    protected void writeBytesArray(byte[][] values, int size, int ordinal, UnsafeRowWriter writer) {
        assert size <= values.length;

        int offset = writer.cursor();
        UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);
        arrayWriter.initialize(size);

        for (int i = 0; i < size; i++) {
            arrayWriter.write(i, values[i]);
        }

        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    // ========== Primitive Array Methods ==========

    /**
     * Write a repeated int field as an array to the UnsafeRow.
     */
    protected void writeIntArray(int[] values, int ordinal, UnsafeRowWriter writer) {
        writeIntArray(values, values.length, ordinal, writer);
    }

    /**
     * Write a repeated int field as an array to the UnsafeRow with size parameter.
     * Eliminates array slicing by using size parameter instead of values.length.
     */
    protected void writeIntArray(int[] values, int size, int ordinal, UnsafeRowWriter writer) {
        assert size <= values.length;

        int offset = writer.cursor();
        UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 4);
        arrayWriter.initialize(size);

        for (int i = 0; i < size; i++) {
            arrayWriter.write(i, values[i]);
        }

        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    /**
     * Write a repeated long field as an array to the UnsafeRow.
     */
    protected void writeLongArray(long[] values, int ordinal, UnsafeRowWriter writer) {
        writeLongArray(values, values.length, ordinal, writer);
    }

    /**
     * Write a repeated long field as an array to the UnsafeRow with size parameter.
     * Eliminates array slicing by using size parameter instead of values.length.
     */
    protected void writeLongArray(long[] values, int size, int ordinal, UnsafeRowWriter writer) {
        assert size <= values.length;

        int offset = writer.cursor();
        UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);
        arrayWriter.initialize(size);

        for (int i = 0; i < size; i++) {
            arrayWriter.write(i, values[i]);
        }

        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    /**
     * Write a repeated float field as an array to the UnsafeRow.
     */
    protected void writeFloatArray(float[] values, int ordinal, UnsafeRowWriter writer) {
        writeFloatArray(values, values.length, ordinal, writer);
    }

    /**
     * Write a repeated float field as an array to the UnsafeRow with size parameter.
     * Eliminates array slicing by using size parameter instead of values.length.
     */
    protected void writeFloatArray(float[] values, int size, int ordinal, UnsafeRowWriter writer) {
        assert size <= values.length;

        int offset = writer.cursor();
        UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 4);
        arrayWriter.initialize(size);

        for (int i = 0; i < size; i++) {
            arrayWriter.write(i, values[i]);
        }

        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    /**
     * Write a repeated double field as an array to the UnsafeRow.
     */
    protected void writeDoubleArray(double[] values, int ordinal, UnsafeRowWriter writer) {
        writeDoubleArray(values, values.length, ordinal, writer);
    }

    /**
     * Write a repeated double field as an array to the UnsafeRow with size parameter.
     * Eliminates array slicing by using size parameter instead of values.length.
     */
    protected void writeDoubleArray(double[] values, int size, int ordinal, UnsafeRowWriter writer) {
        assert size <= values.length;

        int offset = writer.cursor();
        UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);
        arrayWriter.initialize(size);

        for (int i = 0; i < size; i++) {
            arrayWriter.write(i, values[i]);
        }

        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    /**
     * Write a repeated boolean field as an array to the UnsafeRow.
     */
    protected void writeBooleanArray(boolean[] values, int ordinal, UnsafeRowWriter writer) {
        writeBooleanArray(values, values.length, ordinal, writer);
    }

    /**
     * Write a repeated boolean field as an array to the UnsafeRow with size parameter.
     * Eliminates array slicing by using size parameter instead of values.length.
     */
    protected void writeBooleanArray(boolean[] values, int size, int ordinal, UnsafeRowWriter writer) {
        assert size <= values.length;

        int offset = writer.cursor();
        UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 1);
        arrayWriter.initialize(size);

        for (int i = 0; i < size; i++) {
            arrayWriter.write(i, values[i]);
        }

        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    // ========== Message Array Methods ==========

    /**
     * Write a repeated message field as an array to the UnsafeRow.
     * Uses nested parser with writer sharing for optimal performance.
     */
    protected void writeMessageArray(byte[][] messageBytes, int ordinal, StreamWireParser parser, UnsafeRowWriter writer) {
        writeMessageArray(messageBytes, messageBytes.length, ordinal, parser, writer);
    }

    /**
     * Write a repeated message field as an array to the UnsafeRow with size parameter.
     * Eliminates array slicing by using size parameter instead of messageBytes.length.
     */
    protected void writeMessageArray(byte[][] messageBytes, int size, int ordinal, StreamWireParser parser, UnsafeRowWriter writer) {
        assert size <= messageBytes.length;

        int offset = writer.cursor();
        UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);
        arrayWriter.initialize(size);

        for (int i = 0; i < size; i++) {
            int elemOffset = arrayWriter.cursor();
            parser.parseWithSharedBuffer(messageBytes[i], writer);
            arrayWriter.setOffsetAndSizeFromPreviousCursor(i, elemOffset);
        }

        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    // ========== Single Message Methods ==========

    /**
     * Write a single nested message field to the UnsafeRow.
     * Uses nested parser with writer sharing.
     */
    protected void writeMessage(byte[] messageBytes, int ordinal, StreamWireParser parser, UnsafeRowWriter writer) {
        int offset = writer.cursor();
        parser.parseWithSharedBuffer(messageBytes, writer);
        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    // ========== Packed Field Parsing Methods ==========

    /**
     * Parse packed repeated fixed32 values (FIXED32, SFIXED32) from a LENGTH_DELIMITED wire format.
     * Resizes array internally if needed and returns the (potentially new) array.
     */
    protected static int[] parsePackedFixed32s(CodedInputStream input, int[] buffer, int currentCount, int packedLength) throws IOException {
        int oldLimit = input.pushLimit(packedLength);

        // Fixed32 values are always 4 bytes each
        int newValueCount = packedLength / 4;
        if (buffer.length < currentCount + newValueCount) {
            buffer = resizeIntArray(buffer, currentCount, currentCount + newValueCount);
        }

        int index = currentCount;
        while (input.getBytesUntilLimit() > 0) {
            buffer[index++] = input.readRawLittleEndian32();
        }

        input.popLimit(oldLimit);
        return buffer;
    }

    /**
     * Parse packed repeated fixed64 values (FIXED64, SFIXED64) from a LENGTH_DELIMITED wire format.
     * Resizes array internally if needed and returns the (potentially new) array.
     */
    protected static long[] parsePackedFixed64s(CodedInputStream input, long[] buffer, int currentCount, int packedLength) throws IOException {
        int oldLimit = input.pushLimit(packedLength);

        // Fixed64 values are always 8 bytes each
        int newValueCount = packedLength / 8;
        if (buffer.length < currentCount + newValueCount) {
            buffer = resizeLongArray(buffer, currentCount, currentCount + newValueCount);
        }

        int index = currentCount;
        while (input.getBytesUntilLimit() > 0) {
            buffer[index++] = input.readRawLittleEndian64();
        }

        input.popLimit(oldLimit);
        return buffer;
    }

    /**
     * Parse packed repeated floats from a LENGTH_DELIMITED wire format.
     * Resizes array internally if needed and returns the (potentially new) array.
     */
    protected static float[] parsePackedFloats(CodedInputStream input, float[] buffer, int currentCount, int packedLength) throws IOException {
        int oldLimit = input.pushLimit(packedLength);

        // Calculate required capacity (floats are 4 bytes each)
        int maxNewValues = packedLength / 4;
        if (buffer.length < currentCount + maxNewValues) {
            buffer = resizeFloatArray(buffer, currentCount, currentCount + maxNewValues);
        }

        int index = currentCount;
        while (input.getBytesUntilLimit() > 0) {
            buffer[index++] = input.readFloat();
        }

        input.popLimit(oldLimit);
        return buffer;
    }

    /**
     * Parse packed repeated doubles from a LENGTH_DELIMITED wire format.
     * Resizes array internally if needed and returns the (potentially new) array.
     */
    protected static double[] parsePackedDoubles(CodedInputStream input, double[] buffer, int currentCount, int packedLength) throws IOException {
        int oldLimit = input.pushLimit(packedLength);

        // Calculate required capacity (doubles are 8 bytes each)
        int maxNewValues = packedLength / 8;
        if (buffer.length < currentCount + maxNewValues) {
            buffer = resizeDoubleArray(buffer, currentCount, currentCount + maxNewValues);
        }

        int index = currentCount;
        while (input.getBytesUntilLimit() > 0) {
            buffer[index++] = input.readDouble();
        }

        input.popLimit(oldLimit);
        return buffer;
    }

    /**
     * Parse packed repeated booleans from a LENGTH_DELIMITED wire format.
     * Resizes array internally if needed and returns the (potentially new) array.
     */
    protected static boolean[] parsePackedBooleans(CodedInputStream input, boolean[] buffer, int currentCount, int packedLength) throws IOException {
        int oldLimit = input.pushLimit(packedLength);

        // Calculate required capacity (booleans are 1 byte each)
        int maxNewValues = packedLength;
        if (buffer.length < currentCount + maxNewValues) {
            buffer = resizeBooleanArray(buffer, currentCount, currentCount + maxNewValues);
        }

        int index = currentCount;
        while (input.getBytesUntilLimit() > 0) {
            buffer[index++] = input.readBool();
        }

        input.popLimit(oldLimit);
        return buffer;
    }

    // ========== Packed Field Parsing with IntList/LongList ==========

    /**
     * Parse packed repeated varint32 values (INT32, UINT32, ENUM) using IntList for efficient storage.
     * Reads the packed length from input and updates the list's count and may resize the array.
     */
    protected static void parsePackedVarint32s(CodedInputStream input, IntList list) throws IOException {
        int packedLength = input.readRawVarint32();
        int oldLimit = input.pushLimit(packedLength);

        // Local variables for performance
        int[] array = list.array;
        int count = list.count;

        // Parse all values in the packed field
        while (input.getBytesUntilLimit() > 0) {
            // Check capacity and grow if needed
            if (count >= array.length) {
                list.grow(count);
                array = list.array;  // Update local reference after resize
            }
            array[count++] = input.readRawVarint32();
        }

        // Write back the count
        list.count = count;

        input.popLimit(oldLimit);
    }

    /**
     * Parse packed repeated SINT32 values using IntList for efficient storage.
     * Reads the packed length from input and updates the list's count and may resize the array.
     */
    protected static void parsePackedSInt32s(CodedInputStream input, IntList list) throws IOException {
        int packedLength = input.readRawVarint32();
        int oldLimit = input.pushLimit(packedLength);

        // Local variables for performance
        int[] array = list.array;
        int count = list.count;

        // Parse all values in the packed field
        while (input.getBytesUntilLimit() > 0) {
            // Check capacity and grow if needed
            if (count >= array.length) {
                list.grow(count);
                array = list.array;  // Update local reference after resize
            }
            array[count++] = input.readSInt32();
        }

        // Write back the count
        list.count = count;

        input.popLimit(oldLimit);
    }

    /**
     * Parse packed repeated varint64 values (INT64, UINT64) using LongList for efficient storage.
     * Reads the packed length from input and updates the list's count and may resize the array.
     */
    protected static void parsePackedVarint64s(CodedInputStream input, LongList list) throws IOException {
        int packedLength = input.readRawVarint32();
        int oldLimit = input.pushLimit(packedLength);

        // Local variables for performance
        long[] array = list.array;
        int count = list.count;

        // Parse all values in the packed field
        while (input.getBytesUntilLimit() > 0) {
            // Check capacity and grow if needed
            if (count >= array.length) {
                list.grow(count);
                array = list.array;  // Update local reference after resize
            }
            array[count++] = input.readRawVarint64();
        }

        // Write back the count
        list.count = count;

        input.popLimit(oldLimit);
    }

    /**
     * Parse packed repeated SINT64 values using LongList for efficient storage.
     * Reads the packed length from input and updates the list's count and may resize the array.
     */
    protected static void parsePackedSInt64s(CodedInputStream input, LongList list) throws IOException {
        int packedLength = input.readRawVarint32();
        int oldLimit = input.pushLimit(packedLength);

        // Local variables for performance
        long[] array = list.array;
        int count = list.count;

        // Parse all values in the packed field
        while (input.getBytesUntilLimit() > 0) {
            // Check capacity and grow if needed
            if (count >= array.length) {
                list.grow(count);
                array = list.array;  // Update local reference after resize
            }
            array[count++] = input.readSInt64();
        }

        // Write back the count
        list.count = count;

        input.popLimit(oldLimit);
    }
}