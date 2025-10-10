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

    // Default initial capacity when initialize() is not called
    private static final int DEFAULT_CAPACITY = 10;

    // The allocated capacity (max elements without reallocation)
    private int capacity;

    // The actual number of elements written (highest ordinal + 1)
    private int count;

    // The element size in this array
    private int elementSize;

    private int headerInBytes;

    // Track if array has been finalized
    private boolean finalized;

    // Track if space has been allocated yet (for lazy allocation)
    private boolean allocated;

    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
    }

    public GrowableArrayWriter(UnsafeWriter writer, int elementSize) {
        super(writer.getBufferHolder());
        this.elementSize = elementSize;
        this.capacity = 0;  // Will be set on first sizeHint() or first write
        this.count = 0;
        this.finalized = false;
        this.allocated = false;
    }

    /**
     * Provide a capacity hint to optimize space allocation.
     * Can be called multiple times - keeps existing data and grows if needed.
     * Space is allocated lazily on first write, so empty arrays take no space.
     *
     * @param minCapacity the minimum capacity hint
     */
    public void sizeHint(int minCapacity) {
        if (allocated) {
            // Already allocated - grow if needed
            if (minCapacity > capacity) {
                growCapacity(minCapacity);
            }
        } else {
            // Not yet allocated - update capacity hint (take max if already set)
            this.capacity = minCapacity;
        }
    }

    /**
     * Allocate space for the array with the current capacity.
     * Called lazily on first write to avoid allocating space for empty arrays.
     */
    private void allocate() {
        if (allocated) {
            return;
        }

        // Use DEFAULT_CAPACITY if sizeHint was never called
        if (capacity == 0) {
            capacity = DEFAULT_CAPACITY;
        }

        this.headerInBytes = calculateHeaderPortionInBytes(capacity);
        this.startingOffset = cursor();

        // Grows the global buffer ahead for header and fixed size data.
        int fixedPartInBytes =
                ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * capacity);
        grow(headerInBytes + fixedPartInBytes);

        // Write temporary numElements (will be updated in complete()) and clear out null bits to header
        Platform.putLong(getBuffer(), startingOffset, 0L);
        for (int i = 8; i < headerInBytes; i += 8) {
            Platform.putLong(getBuffer(), startingOffset + i, 0L);
        }

        // fill 0 into reminder part of 8-bytes alignment in unsafe array
        for (int i = elementSize * capacity; i < fixedPartInBytes; i++) {
            Platform.putByte(getBuffer(), startingOffset + headerInBytes + i, (byte) 0);
        }
        increaseCursor(headerInBytes + fixedPartInBytes);

        this.allocated = true;
    }

    /**
     * Grow the array capacity to accommodate at least minCapacity elements.
     * Reallocates header and fixed region, preserving existing data.
     *
     * @param minCapacity the minimum capacity required
     */
    private void growCapacity(int minCapacity) {
        int newCapacity = Math.max(capacity * 2, minCapacity);
        int oldHeaderInBytes = headerInBytes;
        int newHeaderInBytes = calculateHeaderPortionInBytes(newCapacity);

        int oldFixedPartInBytes = ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * capacity);
        int newFixedPartInBytes = ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * newCapacity);

        // Calculate old cursor position relative to starting offset
        int oldCursor = cursor();
        int variableDataSize = oldCursor - startingOffset - oldHeaderInBytes - oldFixedPartInBytes;

        // Grow buffer for new header + fixed + existing variable data
        int additionalSpace = (newHeaderInBytes - oldHeaderInBytes) + (newFixedPartInBytes - oldFixedPartInBytes);
        grow(additionalSpace);

        // Need to move data in reverse order to avoid overwriting
        // Move variable-length data first (if any)
        if (variableDataSize > 0) {
            int oldVariableStart = startingOffset + oldHeaderInBytes + oldFixedPartInBytes;
            int newVariableStart = startingOffset + newHeaderInBytes + newFixedPartInBytes;
            Platform.copyMemory(
                getBuffer(), oldVariableStart,
                getBuffer(), newVariableStart,
                variableDataSize
            );
        }

        // Move fixed-length data if header size changed
        if (newHeaderInBytes > oldHeaderInBytes) {
            int oldFixedStart = startingOffset + oldHeaderInBytes;
            int newFixedStart = startingOffset + newHeaderInBytes;
            // Only copy existing element data, not the padding
            int existingDataBytes = count * elementSize;
            Platform.copyMemory(
                getBuffer(), oldFixedStart,
                getBuffer(), newFixedStart,
                existingDataBytes
            );
        }

        // Initialize new null bits (from old header size to new header size)
        for (int i = oldHeaderInBytes; i < newHeaderInBytes; i += 8) {
            Platform.putLong(getBuffer(), startingOffset + i, 0L);
        }

        // Zero out new fixed region slots (beyond existing count)
        int newFixedStart = startingOffset + newHeaderInBytes;
        for (int i = count * elementSize; i < newFixedPartInBytes; i += 8) {
            Platform.putLong(getBuffer(), newFixedStart + i, 0L);
        }

        // Update cursor to account for new layout
        int newCursor = startingOffset + newHeaderInBytes + newFixedPartInBytes + variableDataSize;
        increaseCursor(newCursor - oldCursor);

        this.capacity = newCapacity;
        this.headerInBytes = newHeaderInBytes;
    }

    private void ensureCapacity(int ordinal) {
        // Allocate on first write if not yet allocated
        if (!allocated) {
            allocate();
        }

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
        if (!allocated) {
            allocate();
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
