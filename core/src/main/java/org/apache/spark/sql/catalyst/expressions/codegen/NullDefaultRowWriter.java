/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.bitset.BitSetMethods;

/**
 * A row writer that defaults all fields to null, optimized for sparse data like protobuf messages.
 * <p>
 * Unlike the standard UnsafeRowWriter which defaults fields to non-null, this writer:
 * - Initializes all fields as null via setAllNullBytes()
 * - Automatically clears the null bit when writing field data
 * - Eliminates the need for explicit clearNullAt() calls
 * <p>
 * This design is safer for protobuf parsing where most fields may be absent, ensuring
 * unwritten fields remain properly null rather than containing uninitialized data.
 * <p>
 * Usage:
 * 1. Call resetRowWriter() to initialize all fields as null
 * 2. Write field data - null bits are automatically cleared
 * 3. No need for explicit null bit management
 */
public final class NullDefaultRowWriter extends UnsafeWriter {

    private final UnsafeRow row;

    private final int nullBitsSize;
    private final int fixedSize;

    public NullDefaultRowWriter(int numFields) {
        this(new UnsafeRow(numFields));
    }

    public NullDefaultRowWriter(int numFields, int initialBufferSize) {
        this(new UnsafeRow(numFields), initialBufferSize);
    }

    public NullDefaultRowWriter(UnsafeWriter writer, int numFields) {
        this(null, writer.getBufferHolder(), numFields);
    }

    private NullDefaultRowWriter(UnsafeRow row) {
        this(row, new BufferHolder(row), row.numFields());
    }

    private NullDefaultRowWriter(UnsafeRow row, int initialBufferSize) {
        this(row, new BufferHolder(row, initialBufferSize), row.numFields());
    }

    private NullDefaultRowWriter(UnsafeRow row, BufferHolder holder, int numFields) {
        super(holder);
        this.row = row;
        this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);
        this.fixedSize = nullBitsSize + 8 * numFields;
        this.startingOffset = cursor();
    }

    /**
     * Updates total size of the UnsafeRow using the size collected by BufferHolder, and returns
     * the UnsafeRow created at a constructor
     */
    public UnsafeRow getRow() {
        row.setTotalSize(totalSize());
        return row;
    }

    /**
     * Resets the `startingOffset` according to the current cursor of row buffer, and sets all
     * fields to null.  This should be called before we write a new nested struct to the row buffer.
     */
    public void resetRowWriter() {
        this.startingOffset = cursor();

        // grow the global buffer to make sure it has enough space to write fixed-length data.
        grow(fixedSize);
        increaseCursor(fixedSize);

        setAllNullBytes();
    }

    /**
     * Sets all fields to null (all null bits = 1). This should be called before writing
     * a new row to initialize all fields as null by default.
     */
    public void setAllNullBytes() {
        for (int i = 0; i < nullBitsSize; i += 8) {
            Platform.putLong(getBuffer(), startingOffset + i, -1L);  // All 1s = all null
        }
    }

    public boolean isNullAt(int ordinal) {
        return BitSetMethods.isSet(getBuffer(), startingOffset, ordinal);
    }

    public void setNullAt(int ordinal) {
        BitSetMethods.set(getBuffer(), startingOffset, ordinal);
        write(ordinal, 0L);
    }

    @Override
    public void setNull1Bytes(int ordinal) {
        setNullAt(ordinal);
    }

    @Override
    public void setNull2Bytes(int ordinal) {
        setNullAt(ordinal);
    }

    @Override
    public void setNull4Bytes(int ordinal) {
        setNullAt(ordinal);
    }

    @Override
    public void setNull8Bytes(int ordinal) {
        setNullAt(ordinal);
    }

    public long getFieldOffset(int ordinal) {
        return startingOffset + nullBitsSize + 8L * ordinal;
    }

    @Override
    public void write(int ordinal, boolean value) {
        final long offset = getFieldOffset(ordinal);
        writeLong(offset, 0L);
        writeBoolean(offset, value);
        // Automatically clear null bit when writing data
        BitSetMethods.unset(getBuffer(), startingOffset, ordinal);
    }

    @Override
    public void write(int ordinal, byte value) {
        final long offset = getFieldOffset(ordinal);
        writeLong(offset, 0L);
        writeByte(offset, value);
        // Automatically clear null bit when writing data
        BitSetMethods.unset(getBuffer(), startingOffset, ordinal);
    }

    @Override
    public void write(int ordinal, short value) {
        final long offset = getFieldOffset(ordinal);
        writeLong(offset, 0L);
        writeShort(offset, value);
        // Automatically clear null bit when writing data
        BitSetMethods.unset(getBuffer(), startingOffset, ordinal);
    }

    @Override
    public void write(int ordinal, int value) {
        final long offset = getFieldOffset(ordinal);
        writeLong(offset, 0L);
        writeInt(offset, value);
        // Automatically clear null bit when writing data
        BitSetMethods.unset(getBuffer(), startingOffset, ordinal);
    }

    @Override
    public void write(int ordinal, long value) {
        writeLong(getFieldOffset(ordinal), value);
        // Automatically clear null bit when writing data
        BitSetMethods.unset(getBuffer(), startingOffset, ordinal);
    }

    @Override
    public void write(int ordinal, float value) {
        final long offset = getFieldOffset(ordinal);
        writeLong(offset, 0);
        writeFloat(offset, value);
        // Automatically clear null bit when writing data
        BitSetMethods.unset(getBuffer(), startingOffset, ordinal);
    }

    @Override
    public void write(int ordinal, double value) {
        writeDouble(getFieldOffset(ordinal), value);
        // Automatically clear null bit when writing data
        BitSetMethods.unset(getBuffer(), startingOffset, ordinal);
    }

    @Override
    public void write(int ordinal, Decimal input, int precision, int scale) {
        if (precision <= Decimal.MAX_LONG_DIGITS()) {
            // make sure Decimal object has the same scale as DecimalType
            if (input != null && input.changePrecision(precision, scale)) {
                write(ordinal, input.toUnscaledLong());
            } else {
                setNullAt(ordinal);
            }
        } else {
            // grow the global buffer before writing data.
            holder.grow(16);

            // always zero-out the 16-byte buffer
            Platform.putLong(getBuffer(), cursor(), 0L);
            Platform.putLong(getBuffer(), cursor() + 8, 0L);

            // Make sure Decimal object has the same scale as DecimalType.
            // Note that we may pass in null Decimal object to set null for it.
            if (input == null || !input.changePrecision(precision, scale)) {
                BitSetMethods.set(getBuffer(), startingOffset, ordinal);
                // keep the offset for future update
                setOffsetAndSize(ordinal, 0);
            } else {
                final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
                final int numBytes = bytes.length;
                assert numBytes <= 16;

                // Write the bytes to the variable length portion.
                Platform.copyMemory(
                        bytes, Platform.BYTE_ARRAY_OFFSET, getBuffer(), cursor(), numBytes);
                setOffsetAndSize(ordinal, bytes.length);
                // Automatically clear null bit when writing valid decimal data
                BitSetMethods.unset(getBuffer(), startingOffset, ordinal);
            }

            // move the cursor forward.
            increaseCursor(16);
        }
    }

    /**
     * Writes offset and size for a variable-length field (arrays, nested messages, etc.)
     * and automatically clears the null bit to mark the field as non-null.
     * This method combines setOffsetAndSizeFromPreviousCursor with automatic null bit clearing.
     *
     * @param ordinal the field ordinal to write
     * @param previousCursor the previous cursor position before the variable-length data was written
     */
    public void writeVariableField(int ordinal, int previousCursor) {
        setOffsetAndSizeFromPreviousCursor(ordinal, previousCursor);
        BitSetMethods.unset(getBuffer(), startingOffset, ordinal);
    }
}
