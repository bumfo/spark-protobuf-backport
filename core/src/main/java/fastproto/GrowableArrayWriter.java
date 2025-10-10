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
 * GrowableArrayWriter starts with an initial capacity and automatically grows as elements are written
 * to any ordinal beyond current capacity.
 * <p>
 * Usage pattern:
 * <pre>
 *   GrowableArrayWriter writer = new GrowableArrayWriter(parentWriter, elementSize);
 *   writer.initialize(10);  // Optional capacity hint (allocation happens on first write)
 *   writer.write(0, value1);
 *   writer.write(1, value2);
 *   writer.write(100, value3);  // Automatically grows to accommodate ordinal 100
 *   int actualCount = writer.complete();  // Finalizes with actual count (101)
 * </pre>
 * <p>
 * Key differences from UnsafeArrayWriter:
 * - initialize(capacity) is optional and only sets capacity hint (lazy allocation)
 * - Space allocated on first write, so empty arrays take no extra space
 * - write() methods auto-grow when ordinal >= capacity
 * - Tracks actual element count as max(ordinal + 1) across all writes
 * - Requires complete() call to finalize the array header with actual count
 * <p>
 * The element count is determined by the highest ordinal written to plus one,
 * not by the number of write() calls. Sparse arrays are supported (e.g., writing
 * only to ordinals 0 and 100 creates an array of size 101).
 *
 * @see org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter
 */
public final class GrowableArrayWriter extends UnsafeWriter {

    // The allocated capacity (max elements without reallocation)
    private int capacity;

    // The actual number of elements written (highest ordinal + 1)
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
        this.capacity = 0;  // 0 indicates not yet allocated
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
        if (minCapacity > capacity) {
            growCapacity(minCapacity);
        }
    }

    /**
     * Grow the array capacity to accommodate at least minCapacity elements.
     * Handles initial allocation when capacity == 0.
     * Uses hybrid growth strategy:
     * - Small arrays (< 823): Linear growth (allocate exactly what's needed)
     * - Large arrays (>= 823): Exponential growth (1.5x) to amortize costs
     *
     * Optimization: Fastest path for small mid-block arrays (capacity < 823 && capacity % 64 != 0).
     * Header guaranteed unchanged, just grow buffer space - no calculations or data movement needed.
     *
     * @param minCapacity the minimum capacity required
     */
    private void growCapacity(int minCapacity) {
        // Hybrid growth strategy
        int newCapacity;
        if (capacity < 823) {
            // Small arrays (including initial allocation): allocate exactly what's needed
            newCapacity = minCapacity;
        } else {
            // Large arrays: use 1.5x growth like ArrayList
            newCapacity = Math.max(capacity + (capacity >> 1), minCapacity);
        }

        // Fastest path: incrementing by 1 element without crossing 64-element boundary
        // Header changes when crossing 64, 128, 192... (multiples of 64)
        // Safe to use fast path when capacity is not at boundary: (capacity & 63) != 0
        // No need to zero - new slot will be written or already zero from buffer allocation
        if (capacity < 823 && newCapacity == capacity + 1 && (capacity & 63) != 0) {
            int additionalSpace = ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * newCapacity) -
                                  ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * capacity);
            grow(additionalSpace);
            increaseCursor(additionalSpace);
            this.capacity = newCapacity;
            return;
        }

        // General path: initial allocation, large arrays, or boundary cases
        boolean isInitialAllocation = (capacity == 0);
        int oldHeaderInBytes = isInitialAllocation ? 0 : headerInBytes;
        int newHeaderInBytes = calculateHeaderPortionInBytes(newCapacity);
        int oldFixedPartInBytes = isInitialAllocation ? 0 : ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * capacity);
        int newFixedPartInBytes = ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * newCapacity);

        // Set starting offset for initial allocation
        if (isInitialAllocation) {
            this.startingOffset = cursor();
        }

        // Calculate space needed and variable data size
        int oldCursor = cursor();
        int variableDataSize = isInitialAllocation ? 0 : oldCursor - startingOffset - oldHeaderInBytes - oldFixedPartInBytes;

        // Assert no variable-length data (enforced by setOffsetAndSize throwing UnsupportedOperationException)
        assert variableDataSize == 0 : "GrowableArrayWriter does not support variable-length data";

        // Grow buffer
        int additionalSpace = (newHeaderInBytes - oldHeaderInBytes) + (newFixedPartInBytes - oldFixedPartInBytes);
        grow(additionalSpace);

        // Move data if header size changed (skip for initial allocation)
        if (!isInitialAllocation && newHeaderInBytes > oldHeaderInBytes) {
            // Move variable-length data first (if any) to avoid overwriting
            if (variableDataSize > 0) {
                int oldVariableStart = startingOffset + oldHeaderInBytes + oldFixedPartInBytes;
                int newVariableStart = startingOffset + newHeaderInBytes + newFixedPartInBytes;
                Platform.copyMemory(
                    getBuffer(), oldVariableStart,
                    getBuffer(), newVariableStart,
                    variableDataSize
                );
            }

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
        int headerInitStart = isInitialAllocation ? 0 : oldHeaderInBytes;
        Platform.putLong(getBuffer(), startingOffset, 0L);  // Always write numElements placeholder
        for (int i = Math.max(8, headerInitStart); i < newHeaderInBytes; i += 8) {
            Platform.putLong(getBuffer(), startingOffset + i, 0L);
        }

        // Zero out new fixed region slots
        int newFixedStart = startingOffset + newHeaderInBytes;
        int fixedInitStart = isInitialAllocation ? 0 : count * elementSize;
        for (int i = fixedInitStart; i < newFixedPartInBytes; i += 8) {
            Platform.putLong(getBuffer(), newFixedStart + i, 0L);
        }

        // Update cursor
        int newCursor = startingOffset + newHeaderInBytes + newFixedPartInBytes + variableDataSize;
        increaseCursor(newCursor - oldCursor);

        this.capacity = newCapacity;
        this.headerInBytes = newHeaderInBytes;
    }

    private void ensureCapacity(int ordinal) {
        if (ordinal >= capacity) {
            growCapacity(ordinal + 1);
        }
    }

    private void updateCount(int ordinal) {
        count = Math.max(count, ordinal + 1);
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
        updateCount(ordinal);
    }

    @Override
    public void setNull2Bytes(int ordinal) {
        ensureCapacity(ordinal);
        setNullBit(ordinal);
        // put zero into the corresponding field when set null
        writeShort(getElementOffset(ordinal), (short) 0);
        updateCount(ordinal);
    }

    @Override
    public void setNull4Bytes(int ordinal) {
        ensureCapacity(ordinal);
        setNullBit(ordinal);
        // put zero into the corresponding field when set null
        writeInt(getElementOffset(ordinal), 0);
        updateCount(ordinal);
    }

    @Override
    public void setNull8Bytes(int ordinal) {
        ensureCapacity(ordinal);
        setNullBit(ordinal);
        // put zero into the corresponding field when set null
        writeLong(getElementOffset(ordinal), 0);
        updateCount(ordinal);
    }

    public void setNull(int ordinal) {
        setNull8Bytes(ordinal);
    }

    @Override
    public void write(int ordinal, boolean value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeBoolean(getElementOffset(ordinal), value);
        updateCount(ordinal);
    }

    @Override
    public void write(int ordinal, byte value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeByte(getElementOffset(ordinal), value);
        updateCount(ordinal);
    }

    @Override
    public void write(int ordinal, short value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeShort(getElementOffset(ordinal), value);
        updateCount(ordinal);
    }

    @Override
    public void write(int ordinal, int value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeInt(getElementOffset(ordinal), value);
        updateCount(ordinal);
    }

    @Override
    public void write(int ordinal, long value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeLong(getElementOffset(ordinal), value);
        updateCount(ordinal);
    }

    @Override
    public void write(int ordinal, float value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeFloat(getElementOffset(ordinal), value);
        updateCount(ordinal);
    }

    @Override
    public void write(int ordinal, double value) {
        assertIndexIsValid(ordinal);
        ensureCapacity(ordinal);
        writeDouble(getElementOffset(ordinal), value);
        updateCount(ordinal);
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
                updateCount(ordinal);
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

        // Allocate if no writes occurred (empty array)
        if (capacity == 0) {
            growCapacity(0);
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
     * Get the current allocated capacity.
     *
     * @return the current capacity before next growth would be triggered
     */
    public int getCapacity() {
        return capacity;
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
