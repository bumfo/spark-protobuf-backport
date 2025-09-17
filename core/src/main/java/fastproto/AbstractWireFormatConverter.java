package fastproto;

import com.google.protobuf.CodedInputStream;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

/**
 * Base class for generated WireFormat converters that parse protobuf wire format
 * directly into Spark SQL UnsafeRow structures.
 * <p>
 * Provides optimized array writing methods with size parameters to eliminate
 * array slicing and reduce memory allocations.
 */
public abstract class AbstractWireFormatConverter extends AbstractRowConverter {

    public AbstractWireFormatConverter(StructType schema) {
        super(schema);
    }

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
     * Uses nested converter with writer sharing for optimal performance.
     */
    protected void writeMessageArray(byte[][] messageBytes, int ordinal, AbstractWireFormatConverter converter, UnsafeRowWriter writer) {
        writeMessageArray(messageBytes, messageBytes.length, ordinal, converter, writer);
    }

    /**
     * Write a repeated message field as an array to the UnsafeRow with size parameter.
     * Eliminates array slicing by using size parameter instead of messageBytes.length.
     */
    protected void writeMessageArray(byte[][] messageBytes, int size, int ordinal, AbstractWireFormatConverter converter, UnsafeRowWriter writer) {
        assert size <= messageBytes.length;

        int offset = writer.cursor();
        UnsafeArrayWriter arrayWriter = new UnsafeArrayWriter(writer, 8);
        arrayWriter.initialize(size);

        for (int i = 0; i < size; i++) {
            int elemOffset = arrayWriter.cursor();
            converter.convert(messageBytes[i], writer);
            arrayWriter.setOffsetAndSizeFromPreviousCursor(i, elemOffset);
        }

        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    // ========== Single Message Methods ==========

    /**
     * Write a single nested message field to the UnsafeRow.
     * Uses nested converter with writer sharing.
     */
    protected void writeMessage(byte[] messageBytes, int ordinal, AbstractWireFormatConverter converter, UnsafeRowWriter writer) {
        int offset = writer.cursor();
        converter.convert(messageBytes, writer);
        writer.setOffsetAndSizeFromPreviousCursor(ordinal, offset);
    }

    // ========== Packed Field Parsing Methods ==========

    /**
     * Parse packed repeated ints from a LENGTH_DELIMITED wire format.
     * Resizes array internally if needed and returns the (potentially new) array.
     */
    protected static int[] parsePackedInts(CodedInputStream input, int[] buffer, int currentCount, int packedLength) throws IOException {
        int oldLimit = input.pushLimit(packedLength);

        // Calculate required capacity (ints are variable length, estimate conservatively)
        int maxNewValues = packedLength; // Worst case: 1 byte per varint
        if (buffer.length < currentCount + maxNewValues) {
            buffer = resizeIntArray(buffer, currentCount, currentCount + maxNewValues);
        }

        int index = currentCount;
        while (input.getBytesUntilLimit() > 0) {
            buffer[index++] = input.readInt32();
        }

        input.popLimit(oldLimit);
        return buffer;
    }

    /**
     * Parse packed repeated longs from a LENGTH_DELIMITED wire format.
     * Resizes array internally if needed and returns the (potentially new) array.
     */
    protected static long[] parsePackedLongs(CodedInputStream input, long[] buffer, int currentCount, int packedLength) throws IOException {
        int oldLimit = input.pushLimit(packedLength);

        // Calculate required capacity (longs are variable length, estimate conservatively)
        int maxNewValues = packedLength; // Worst case: 1 byte per varint
        if (buffer.length < currentCount + maxNewValues) {
            buffer = resizeLongArray(buffer, currentCount, currentCount + maxNewValues);
        }

        int index = currentCount;
        while (input.getBytesUntilLimit() > 0) {
            buffer[index++] = input.readInt64();
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
     * Parse packed repeated ints using IntList for efficient storage.
     * Reads the packed length from input and updates the list's count and may resize the array.
     */
    protected static void parsePackedInts(CodedInputStream input, IntList list) throws IOException {
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
            array[count++] = input.readInt32();
        }

        // Write back the count
        list.count = count;

        input.popLimit(oldLimit);
    }

    /**
     * Parse packed repeated longs using LongList for efficient storage.
     * Reads the packed length from input and updates the list's count and may resize the array.
     */
    protected static void parsePackedLongs(CodedInputStream input, LongList list) throws IOException {
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
            array[count++] = input.readInt64();
        }

        // Write back the count
        list.count = count;

        input.popLimit(oldLimit);
    }
}