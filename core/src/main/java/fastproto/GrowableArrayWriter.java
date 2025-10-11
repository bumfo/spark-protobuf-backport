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

package fastproto;

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;

import static org.apache.spark.sql.catalyst.expressions.UnsafeArrayData.calculateHeaderPortionInBytes;

/**
 * A growable version of UnsafeArrayWriter that supports dynamic capacity expansion.
 * <p>
 * Unlike UnsafeArrayWriter which requires the exact element count upfront via initialize(numElements),
 * GrowableArrayWriter starts with capacity 0 and automatically grows as elements are written.
 * <p>
 * Usage pattern:
 * <pre>
 *   GrowableArrayWriter writer = new GrowableArrayWriter(parentWriter, elementSize);
 *   writer.sizeHint(10);    // Optional capacity hint (allocates immediately)
 *   writer.write(0, value1);
 *   writer.write(1, value2);
 *   writer.write(100, value3);  // Automatically grows to accommodate ordinal 100
 *   int actualCount = writer.complete();  // Finalizes with actual count (101)
 * </pre>
 * <p>
 * Growth Strategy:
 * <ul>
 *   <li>Header-focused growth: Capacity doubles (64 → 128 → 256 → 512...) to minimize header resizes</li>
 *   <li>BufferHolder handles buffer allocation with 2x exponential growth automatically</li>
 *   <li>Data movement only occurs when header size changes (logarithmic resizes)</li>
 *   <li>Empty arrays: Fast path allocates only 8 bytes for numElements field</li>
 * </ul>
 * <p>
 * Key Features:
 * <ul>
 *   <li>sizeHint() is optional - allocates immediately to avoid reallocation overhead</li>
 *   <li>Auto-grows when writing beyond capacity</li>
 *   <li>Tracks actual element count as max(ordinal + 1) across all writes</li>
 *   <li>Supports sparse arrays (e.g., writing to ordinals 0 and 100 creates size 101)</li>
 *   <li>Fixed-size elements only (primitives and Decimal)</li>
 *   <li>Requires complete() to finalize the array header with actual count</li>
 * </ul>
 *
 * @see org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter
 */
public final class GrowableArrayWriter extends UnsafeWriter {

    // Header capacity grows exponentially (64, 128, 256...) to minimize header resizes
    private int headerCapacity;

    // Allocated element space (number of slots allocated)
    // Updated only by growCapacity() when allocating space
    // After all writes: count = allocated space = highest ordinal + 1
    private int count;

    // The element size in this array
    private int elementSize;

    private int headerInBytes;

    // Track if array has been finalized
    private boolean finalized;

    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
    }

    public GrowableArrayWriter(UnsafeWriter writer, int elementSize) {
        super(writer.getBufferHolder());
        this.elementSize = elementSize;
        this.headerCapacity = 0;  // 0 indicates not yet allocated
        this.count = 0;
        this.finalized = false;
    }

    /**
     * Provide a capacity hint to optimize space allocation.
     * Can be called multiple times - keeps existing data and grows if needed.
     *
     * @param minCapacity the minimum capacity hint
     */
    public void sizeHint(int minCapacity) {
        if (minCapacity > count) {
            growCapacity(minCapacity);
        }
    }

    /**
     * Grow the array capacity to accommodate at least minCapacity elements.
     * Handles initial allocation when headerCapacity == 0.
     * Header capacity grows exponentially (doubles) to minimize header resizes.
     * Element space allocated exactly for count (current allocated space).
     * BufferHolder.grow() handles actual buffer allocation with its own 2x growth.
     *
     * @param minCapacity the minimum capacity required (for header calculation only)
     */
    private void growCapacity(int minCapacity) {
        // Double headerCapacity until it satisfies minCapacity
        // Start with 64 for initial allocation (first boundary where header includes null bits)
        int newHeaderCapacity = headerCapacity == 0 ? 64 : headerCapacity;
        while (newHeaderCapacity < minCapacity) {
            newHeaderCapacity = newHeaderCapacity << 1;  // Double
        }

        // Allocate space for exactly minCapacity elements
        int newCount = minCapacity;

        // General path: handles all cases (initial allocation, jumps, boundaries, large arrays)
        boolean isInitialAllocation = (headerCapacity == 0);
        int oldHeaderInBytes = headerInBytes;  // 0 when isInitialAllocation
        int newHeaderInBytes = calculateHeaderPortionInBytes(newHeaderCapacity);
        int oldFixedPartInBytes = ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * count);  // 0 when count == 0
        int newFixedPartInBytes = ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * newCount);

        // Set starting offset for initial allocation
        if (isInitialAllocation) {
            this.startingOffset = cursor();
        }

        // Calculate space needed and variable data size
        int oldCursor = cursor();
        int variableDataSize = oldCursor - startingOffset - oldHeaderInBytes - oldFixedPartInBytes;  // 0 when isInitialAllocation (oldCursor == startingOffset)

        // Assert no variable-length data (enforced by setOffsetAndSize throwing UnsupportedOperationException)
        assert variableDataSize == 0 : "GrowableArrayWriter does not support variable-length data";

        // Grow buffer
        int additionalSpace = (newHeaderInBytes - oldHeaderInBytes) + (newFixedPartInBytes - oldFixedPartInBytes);
        grow(additionalSpace);

        // Move data if header size changed (skip for initial allocation)
        if (!isInitialAllocation && newHeaderInBytes > oldHeaderInBytes) {
            // // Move variable-length data first (if any) to avoid overwriting
            // if (variableDataSize > 0) {
            //     int oldVariableStart = startingOffset + oldHeaderInBytes + oldFixedPartInBytes;
            //     int newVariableStart = startingOffset + newHeaderInBytes + newFixedPartInBytes;
            //     Platform.copyMemory(
            //         getBuffer(), oldVariableStart,
            //         getBuffer(), newVariableStart,
            //         variableDataSize
            //     );
            // }

            // Move fixed-length data
            int oldFixedStart = startingOffset + oldHeaderInBytes;
            int newFixedStart = startingOffset + newHeaderInBytes;
            int existingDataBytes = count * elementSize;
            Platform.copyMemory(
                getBuffer(), oldFixedStart,
                getBuffer(), newFixedStart,
                existingDataBytes
            );
        }

        // Initialize header (full initialization for new allocation, only new null bits for growth)
        int headerInitStart = oldHeaderInBytes;  // 0 when isInitialAllocation
        for (int i = headerInitStart; i < newHeaderInBytes; i += 8) {
            Platform.putLong(getBuffer(), startingOffset + i, 0L);
        }

        // // Zero out new fixed region slots
        // int newFixedStart = startingOffset + newHeaderInBytes;
        // int fixedInitStart = count * elementSize;  // 0 when isInitialAllocation (count == 0)
        // for (int i = fixedInitStart; i < newFixedPartInBytes; i += 8) {
        //     Platform.putLong(getBuffer(), newFixedStart + i, 0L);
        // }

        // Update cursor
        int newCursor = startingOffset + newHeaderInBytes + newFixedPartInBytes + variableDataSize;
        increaseCursor(newCursor - oldCursor);

        this.headerCapacity = newHeaderCapacity;
        // Only update count if growing beyond current count (from writes, not sizeHint)
        if (newCount > this.count) {
            this.count = newCount;
        }
        this.headerInBytes = newHeaderInBytes;
    }

    private void ensureCapacity(int ordinal) {
        if (ordinal >= count) {
            growCapacity(ordinal + 1);
        }
    }

    private long getElementOffset(int ordinal) {
        return startingOffset + headerInBytes + ordinal * (long) elementSize;
    }

    private void setNullBit(int ordinal) {
        assertIndexIsValid(ordinal);
        BitSetMethods.set(getBuffer(), startingOffset + 8, ordinal);
    }

    @Override
    public void setNull1Bytes(int ordinal) {
        ensureCapacity(ordinal);
        setNullBit(ordinal);
        // put zero into the corresponding field when set null
        writeByte(getElementOffset(ordinal), (byte) 0);
    }

    @Override
    public void setNull2Bytes(int ordinal) {
        ensureCapacity(ordinal);
        setNullBit(ordinal);
        // put zero into the corresponding field when set null
        writeShort(getElementOffset(ordinal), (short) 0);
    }

    @Override
    public void setNull4Bytes(int ordinal) {
        ensureCapacity(ordinal);
        setNullBit(ordinal);
        // put zero into the corresponding field when set null
        writeInt(getElementOffset(ordinal), 0);
    }

    @Override
    public void setNull8Bytes(int ordinal) {
        ensureCapacity(ordinal);
        setNullBit(ordinal);
        // put zero into the corresponding field when set null
        writeLong(getElementOffset(ordinal), 0);
    }

    public void setNull(int ordinal) {
        setNull8Bytes(ordinal);
    }

    @Override
    public void write(int ordinal, boolean value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeBoolean(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, byte value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeByte(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, short value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeShort(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, int value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeInt(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, long value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeLong(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, float value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeFloat(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, double value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeDouble(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, Decimal input, int precision, int scale) {
        // make sure Decimal object has the same scale as DecimalType
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        if (input != null && input.changePrecision(precision, scale)) {
            if (precision <= Decimal.MAX_LONG_DIGITS()) {
                write(ordinal, input.toUnscaledLong());
            } else {
                final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
                final int numBytes = bytes.length;
                assert numBytes <= 16;
                int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
                grow(roundedSize);

                zeroOutPaddingBytes(numBytes);

                // Write the bytes to the variable length portion.
                Platform.copyMemory(
                        bytes, Platform.BYTE_ARRAY_OFFSET, getBuffer(), cursor(), numBytes);
                setOffsetAndSize(ordinal, numBytes);

                // move the cursor forward with 8-bytes boundary
                increaseCursor(roundedSize);
            }
        } else {
            setNull(ordinal);
        }
    }

    /**
     * Finalize the growable array by updating the header with the actual element count.
     * Must be called after all elements have been written.
     * If no writes occurred, allocates an empty array.
     *
     * @return the actual number of elements written
     * @throws IllegalStateException if already finalized
     */
    public int complete() {
        if (finalized) {
            throw new IllegalStateException("GrowableArrayWriter has already been finalized");
        }

        // Fast path for empty array: just allocate 8 bytes for numElements
        if (headerCapacity == 0) {
            this.startingOffset = cursor();
            grow(8);
            increaseCursor(8);
            this.headerInBytes = 8;
        }

        // Update the numElements field in the header with the actual count
        Platform.putLong(getBuffer(), startingOffset, count);

        finalized = true;
        return count;
    }

    /**
     * Get the current element count without finalizing.
     * Useful for checking progress or debugging.
     *
     * @return the current number of elements written
     */
    public int getCount() {
        return count;
    }

    /**
     * Get the current header capacity (for testing).
     * This represents the header size boundary, not element count.
     *
     * @return the current header capacity
     */
    public int getCapacity() {
        return headerCapacity;
    }

    /**
     * Get the starting offset of this array in the buffer.
     * Useful for tests and debugging.
     *
     * @return the starting offset in bytes
     */
    public int getStartingOffset() {
        return startingOffset;
    }

    // Override variable-length write methods to forbid their use
    // GrowableArrayWriter only supports fixed-size elements

    @Override
    protected final void setOffsetAndSize(int ordinal, int currentCursor, int size) {
        throw new UnsupportedOperationException(
            "GrowableArrayWriter does not support variable-length data. " +
            "Only fixed-size primitive types and Decimal are supported.");
    }
}
