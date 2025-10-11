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
import org.apache.spark.unsafe.bitset.BitSetMethods;

import static org.apache.spark.sql.catalyst.expressions.UnsafeArrayData.calculateHeaderPortionInBytes;

/**
 * A growable version of UnsafeArrayWriter that supports dynamic size expansion.
 * <p>
 * Unlike UnsafeArrayWriter which requires the exact element count upfront via initialize(numElements),
 * GrowableArrayWriter starts with zero size and automatically grows as elements are written.
 * <p>
 * <b>Usage Pattern:</b>
 * <pre>
 *   GrowableArrayWriter writer = new GrowableArrayWriter(parentWriter, elementSize);
 *   writer.sizeHint(10);      // Optional: pre-allocate space for 10 elements
 *   writer.write(0, value1);
 *   writer.write(1, value2);
 *   writer.write(100, value3); // Auto-grows to accommodate ordinal 100
 *   int count = writer.complete(); // Finalize array, returns 101
 * </pre>
 * <p>
 * <b>Growth Strategy:</b>
 * <ul>
 *   <li><b>Header capacity:</b> Grows exponentially (64 → 128 → 256 → 512...) using powers of 2
 *       to minimize header resizes. Computed in O(1) time using bit manipulation.</li>
 *   <li><b>Element space:</b> Grows linearly to exactly (ordinal + 1) on each write beyond current size.</li>
 *   <li><b>Data movement:</b> Only occurs when header size changes (at 64, 128, 192, 256... element boundaries),
 *       resulting in logarithmic number of resizes.</li>
 *   <li><b>Buffer allocation:</b> Delegated to BufferHolder which uses its own 2x exponential growth.</li>
 * </ul>
 * <p>
 * <b>Terminology:</b>
 * <ul>
 *   <li><b>headerCapacity:</b> The header size boundary (64, 128, 256...). Determines when header needs to grow.</li>
 *   <li><b>size:</b> Allocated element slots. Equals max(ordinal + 1) across all writes.</li>
 *   <li><b>elementSize:</b> Fixed size per element in bytes (8 for long, 4 for int, etc.).</li>
 * </ul>
 * <p>
 * <b>Key Features:</b>
 * <ul>
 *   <li><b>Optional size hint:</b> Call sizeHint() to pre-allocate space and avoid reallocation overhead.</li>
 *   <li><b>Automatic growth:</b> Writes beyond current size trigger automatic expansion.</li>
 *   <li><b>Sparse arrays:</b> Writing to ordinals 0 and 100 creates an array of size 101 (unwritten slots remain null/zero).</li>
 *   <li><b>Fixed-size elements only:</b> Supports primitives and compact Decimal. Variable-length data throws UnsupportedOperationException.</li>
 *   <li><b>Finalization required:</b> Must call complete() to write the final element count to the array header.</li>
 * </ul>
 * <p>
 * <b>Performance Characteristics:</b>
 * <ul>
 *   <li>Header capacity computation: O(1) using bit manipulation</li>
 *   <li>Number of resizes: O(log N) where N is the final size</li>
 *   <li>Data movement: Only when crossing header boundaries (every 64 elements)</li>
 * </ul>
 *
 * @see org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter
 */
public final class GrowableArrayWriter extends UnsafeWriter {

    // ========== Fields ==========

    // Element configuration
    private final int elementSize;

    // Header tracking (grows exponentially: 64, 128, 256...)
    private int headerCapacity;
    private int headerInBytes;

    // Element space tracking (grows linearly to ordinal+1)
    private int size;

    // Finalization tracking
    private boolean finalized;

    // ========== Constructor ==========

    public GrowableArrayWriter(UnsafeWriter writer, int elementSize) {
        super(writer.getBufferHolder());
        this.elementSize = elementSize;
        this.headerCapacity = 0;  // 0 indicates not yet allocated
        this.size = 0;
        this.finalized = false;
    }

    // ========== Public API ==========

    /**
     * Provide a size hint to optimize space allocation.
     * Can be called multiple times - keeps existing data and grows if needed.
     *
     * @param minSize the minimum size hint
     */
    public void sizeHint(int minSize) {
        if (minSize > size) {
            growToSize(minSize);
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

        // Update the numElements field in the header with the actual size
        Platform.putLong(getBuffer(), startingOffset, size);

        finalized = true;
        return size;
    }

    /**
     * Get the current element size without finalizing.
     * Useful for checking progress or debugging.
     *
     * @return the current number of elements allocated
     */
    public int getSize() {
        return size;
    }

    /**
     * Get the current header capacity (for testing).
     * This represents the header size boundary, not element count.
     *
     * @return the current header capacity
     */
    public int getHeaderCapacity() {
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

    // ========== Private Helpers ==========

    /**
     * Grow the array to accommodate newSize elements.
     * Handles initial allocation and header/element space growth.
     *
     * @param newSize the target size (must be >= current size)
     */
    private void growToSize(int newSize) {
        assert newSize >= this.size;

        int newHeaderCapacity = Math.max(64, ceilPow2(newSize));
        int oldFixedPartInBytes = roundToWord(elementSize * size);
        int newFixedPartInBytes = roundToWord(elementSize * newSize);

        int additionalSpace;

        if (newHeaderCapacity == headerCapacity) {
            // Header capacity unchanged - only grow element space
            additionalSpace = newFixedPartInBytes - oldFixedPartInBytes;
            grow(additionalSpace);
        } else {
            // Header capacity changed - handle header growth and data movement
            int newHeaderInBytes = calculateHeaderPortionInBytes(newHeaderCapacity);

            boolean isInitialAllocation = (headerCapacity == 0);
            if (isInitialAllocation) {
                this.startingOffset = cursor();
            }

            int oldHeaderInBytes = headerInBytes;
            additionalSpace = (newHeaderInBytes - oldHeaderInBytes) + (newFixedPartInBytes - oldFixedPartInBytes);

            grow(additionalSpace);

            // Move existing data if not initial allocation
            if (!isInitialAllocation) {
                Platform.copyMemory(
                        getBuffer(), startingOffset + oldHeaderInBytes,
                        getBuffer(), startingOffset + newHeaderInBytes,
                        (long) size * elementSize
                );
            }

            // Zero out new header portion
            for (int i = oldHeaderInBytes; i < newHeaderInBytes; i += 8) {
                Platform.putLong(getBuffer(), startingOffset + i, 0L);
            }

            // Update header state
            this.headerCapacity = newHeaderCapacity;
            this.headerInBytes = newHeaderInBytes;
        }

        // Common: update cursor and size
        increaseCursor(additionalSpace);
        this.size = newSize;
    }

    private void ensureSize(int ordinal) {
        // Fast path: single-element growth within header capacity
        // Most common case for sequential writes
        if (ordinal == size && ordinal < headerCapacity) {
            int newBytes = elementSize * size + elementSize;
            int additionalSpace = (elementSize + ((-newBytes) & 7)) & ~7;

            grow(additionalSpace);
            increaseCursor(additionalSpace);
            this.size++;
        } else if (ordinal >= size) {
            // Slow path: multi-element jump or header growth needed
            growToSize(ordinal + 1);
        }
    }

    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
    }

    private long getElementOffset(int ordinal) {
        return startingOffset + headerInBytes + ordinal * (long) elementSize;
    }

    private void setNullBit(int ordinal) {
        assertIndexIsValid(ordinal);
        BitSetMethods.set(getBuffer(), startingOffset + 8, ordinal);
    }

    // ========== Override: Null Setters ==========

    @Override
    public void setNull1Bytes(int ordinal) {
        ensureSize(ordinal);
        setNullBit(ordinal);
        writeByte(getElementOffset(ordinal), (byte) 0);
    }

    @Override
    public void setNull2Bytes(int ordinal) {
        ensureSize(ordinal);
        setNullBit(ordinal);
        writeShort(getElementOffset(ordinal), (short) 0);
    }

    @Override
    public void setNull4Bytes(int ordinal) {
        ensureSize(ordinal);
        setNullBit(ordinal);
        writeInt(getElementOffset(ordinal), 0);
    }

    @Override
    public void setNull8Bytes(int ordinal) {
        ensureSize(ordinal);
        setNullBit(ordinal);
        writeLong(getElementOffset(ordinal), 0);
    }

    public void setNull(int ordinal) {
        setNull8Bytes(ordinal);
    }

    // ========== Override: Write Primitives ==========

    @Override
    public void write(int ordinal, boolean value) {
        assertIndexIsValid(ordinal);
        ensureSize(ordinal);
        writeBoolean(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, byte value) {
        assertIndexIsValid(ordinal);
        ensureSize(ordinal);
        writeByte(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, short value) {
        assertIndexIsValid(ordinal);
        ensureSize(ordinal);
        writeShort(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, int value) {
        assertIndexIsValid(ordinal);
        ensureSize(ordinal);
        writeInt(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, long value) {
        assertIndexIsValid(ordinal);
        ensureSize(ordinal);
        writeLong(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, float value) {
        assertIndexIsValid(ordinal);
        ensureSize(ordinal);
        writeFloat(getElementOffset(ordinal), value);
    }

    @Override
    public void write(int ordinal, double value) {
        assertIndexIsValid(ordinal);
        ensureSize(ordinal);
        writeDouble(getElementOffset(ordinal), value);
    }

    // ========== Override: Write Complex Types ==========

    @Override
    public void write(int ordinal, Decimal input, int precision, int scale) {
        assertIndexIsValid(ordinal);
        ensureSize(ordinal);
        if (input != null && input.changePrecision(precision, scale)) {
            if (precision <= Decimal.MAX_LONG_DIGITS()) {
                write(ordinal, input.toUnscaledLong());
            } else {
                final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
                final int numBytes = bytes.length;
                assert numBytes <= 16;
                int roundedSize = roundToWord(numBytes);
                grow(roundedSize);

                zeroOutPaddingBytes(numBytes);

                // Write the bytes to the variable length portion
                Platform.copyMemory(
                        bytes, Platform.BYTE_ARRAY_OFFSET, getBuffer(), cursor(), numBytes);
                setOffsetAndSize(ordinal, numBytes);

                increaseCursor(roundedSize);
            }
        } else {
            setNull(ordinal);
        }
    }

    // ========== Override: Forbidden Operations ==========

    @Override
    protected final void setOffsetAndSize(int ordinal, int currentCursor, int size) {
        throw new UnsupportedOperationException(
                "GrowableArrayWriter does not support variable-length data. " +
                        "Only fixed-size primitive types and Decimal are supported.");
    }

    // ========== Static Utilities ==========

    /**
     * Round number of bytes to nearest 8-byte word.
     * Uses bit manipulation for fast computation.
     *
     * @param numBytes the number of bytes
     * @return numBytes rounded up to nearest multiple of 8
     */
    private static int roundToWord(int numBytes) {
        return (numBytes + 7) & ~7;
    }

    /**
     * Compute the ceiling power of 2 for a given value.
     * Uses bit manipulation for O(1) computation.
     *
     * @param x the input value
     * @return the smallest power of 2 >= x
     */
    private static int ceilPow2(int x) {
        x -= 1;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        x += 1;
        return x;
    }
}
