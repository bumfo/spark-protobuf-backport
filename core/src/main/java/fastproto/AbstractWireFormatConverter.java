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

    /**
     * Convert protobuf binary data to InternalRow using optimized parsing.
     */
    public InternalRow convert(byte[] binary) {
        UnsafeRowWriter writer = new UnsafeRowWriter(schema().size());
        writer.resetRowWriter();
        writeData(binary, writer);
        return writer.getRow();
    }

    /**
     * Convert protobuf binary data directly to UnsafeRowWriter.
     * This method is implemented by generated converters.
     */
    public abstract void writeData(byte[] binary, UnsafeRowWriter writer);



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
     * Parse packed repeated values from a LENGTH_DELIMITED wire format.
     * Returns the number of values parsed.
     */
    protected int parsePackedInts(CodedInputStream input, int[] buffer, int maxCount) throws IOException {
        int length = input.readRawVarint32();
        int oldLimit = input.pushLimit(length);
        int count = 0;

        while (input.getBytesUntilLimit() > 0 && count < maxCount) {
            buffer[count] = input.readInt32();
            count++;
        }

        input.popLimit(oldLimit);
        return count;
    }

    /**
     * Parse packed repeated longs from a LENGTH_DELIMITED wire format.
     */
    protected int parsePackedLongs(CodedInputStream input, long[] buffer, int maxCount) throws IOException {
        int length = input.readRawVarint32();
        int oldLimit = input.pushLimit(length);
        int count = 0;

        while (input.getBytesUntilLimit() > 0 && count < maxCount) {
            buffer[count] = input.readInt64();
            count++;
        }

        input.popLimit(oldLimit);
        return count;
    }

    /**
     * Parse packed repeated floats from a LENGTH_DELIMITED wire format.
     */
    protected int parsePackedFloats(CodedInputStream input, float[] buffer, int maxCount) throws IOException {
        int length = input.readRawVarint32();
        int oldLimit = input.pushLimit(length);
        int count = 0;

        while (input.getBytesUntilLimit() > 0 && count < maxCount) {
            buffer[count] = input.readFloat();
            count++;
        }

        input.popLimit(oldLimit);
        return count;
    }

    /**
     * Parse packed repeated doubles from a LENGTH_DELIMITED wire format.
     */
    protected int parsePackedDoubles(CodedInputStream input, double[] buffer, int maxCount) throws IOException {
        int length = input.readRawVarint32();
        int oldLimit = input.pushLimit(length);
        int count = 0;

        while (input.getBytesUntilLimit() > 0 && count < maxCount) {
            buffer[count] = input.readDouble();
            count++;
        }

        input.popLimit(oldLimit);
        return count;
    }

    /**
     * Parse packed repeated booleans from a LENGTH_DELIMITED wire format.
     */
    protected int parsePackedBooleans(CodedInputStream input, boolean[] buffer, int maxCount) throws IOException {
        int length = input.readRawVarint32();
        int oldLimit = input.pushLimit(length);
        int count = 0;

        while (input.getBytesUntilLimit() > 0 && count < maxCount) {
            buffer[count] = input.readBool();
            count++;
        }

        input.popLimit(oldLimit);
        return count;
    }
}