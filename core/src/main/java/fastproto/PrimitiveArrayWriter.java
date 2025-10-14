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
import org.apache.spark.unsafe.Platform;

/**
 * High-performance append-only array writer for non-nullable primitives.
 *
 * <p>Optimized for sequential writes with minimal overhead:
 * <ul>
 *   <li>Pre-allocates header for 64 elements (16 bytes)</li>
 *   <li>Arrays ≤64 elements: Zero data movement at complete()</li>
 *   <li>Arrays >64 elements: One data movement at complete()</li>
 *   <li>No null support - simpler and faster</li>
 *   <li>BufferHolder manages buffer growth with exponential doubling</li>
 * </ul>
 *
 * <p><b>Usage:</b>
 * <pre>
 *   PrimitiveArrayWriter writer = new PrimitiveArrayWriter(parent, 8, 100);
 *   for (long value : values) {
 *       writer.writeLong(value);
 *   }
 *   int count = writer.complete();
 * </pre>
 */
public final class PrimitiveArrayWriter extends UnsafeWriter {

    private final int elementSize;
    private int count = 0;

    // Simple layout: [8-byte count][8-byte bitmap for ≤64 elements][data...]
    private final int dataOffset;      // Always startingOffset + 16
    private int writePosition;         // Current write position
    private int elementCapacity;       // Max elements in current buffer

    /**
     * Create array writer with optional capacity hint.
     * @param parent the parent writer
     * @param elementSize size of each element (1,2,4,8 bytes)
     * @param initialCapacity expected elements (0 for no hint)
     */
    public PrimitiveArrayWriter(UnsafeWriter parent, int elementSize, int initialCapacity) {
        super(parent.getBufferHolder());
        this.elementSize = elementSize;
        this.startingOffset = cursor();

        // Pre-allocate header space for 64 elements: 8 bytes count + 8 bytes bitmap
        // This ensures zero data movement for arrays ≤64 elements
        this.dataOffset = startingOffset + 16;
        this.writePosition = dataOffset;

        // Calculate current buffer capacity
        // dataOffset already includes Platform.BYTE_ARRAY_OFFSET, so subtract it
        int availableBytes = getBuffer().length - (dataOffset - Platform.BYTE_ARRAY_OFFSET);
        this.elementCapacity = availableBytes / elementSize;

        // Grow if needed for initial capacity hint
        // Use byte comparison to handle negative elementCapacity correctly
        if (elementSize * initialCapacity > availableBytes) {
            growBuffer(initialCapacity);
        }
    }

    // ===== Write Methods (No null support) =====

    public void writeLong(long value) {
        ensureCapacity();
        Platform.putLong(getBuffer(), writePosition, value);
        writePosition += 8;
        count++;
    }

    public void writeInt(int value) {
        ensureCapacity();
        Platform.putInt(getBuffer(), writePosition, value);
        writePosition += 4;
        count++;
    }

    public void writeDouble(double value) {
        ensureCapacity();
        Platform.putDouble(getBuffer(), writePosition, value);
        writePosition += 8;
        count++;
    }

    public void writeFloat(float value) {
        ensureCapacity();
        Platform.putFloat(getBuffer(), writePosition, value);
        writePosition += 4;
        count++;
    }

    public void writeShort(short value) {
        ensureCapacity();
        Platform.putShort(getBuffer(), writePosition, value);
        writePosition += 2;
        count++;
    }

    public void writeByte(byte value) {
        ensureCapacity();
        Platform.putByte(getBuffer(), writePosition, value);
        writePosition += 1;
        count++;
    }

    public void writeBoolean(boolean value) {
        ensureCapacity();
        Platform.putBoolean(getBuffer(), writePosition, value);
        writePosition += 1;
        count++;
    }

    // ===== Unsupported Operations (No ordinal-based access, no nulls) =====

    @Override
    public void setNull1Bytes(int ordinal) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support null values or ordinal-based writes");
    }

    @Override
    public void setNull2Bytes(int ordinal) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support null values or ordinal-based writes");
    }

    @Override
    public void setNull4Bytes(int ordinal) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support null values or ordinal-based writes");
    }

    @Override
    public void setNull8Bytes(int ordinal) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support null values or ordinal-based writes");
    }

    @Override
    public void write(int ordinal, boolean value) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support ordinal-based writes. " +
            "Use append-only writeBoolean(value)");
    }

    @Override
    public void write(int ordinal, byte value) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support ordinal-based writes. " +
            "Use append-only writeByte(value)");
    }

    @Override
    public void write(int ordinal, short value) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support ordinal-based writes. " +
            "Use append-only writeShort(value)");
    }

    @Override
    public void write(int ordinal, int value) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support ordinal-based writes. " +
            "Use append-only writeInt(value)");
    }

    @Override
    public void write(int ordinal, long value) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support ordinal-based writes. " +
            "Use append-only writeLong(value)");
    }

    @Override
    public void write(int ordinal, float value) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support ordinal-based writes. " +
            "Use append-only writeFloat(value)");
    }

    @Override
    public void write(int ordinal, double value) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support ordinal-based writes. " +
            "Use append-only writeDouble(value)");
    }

    @Override
    public void write(int ordinal, org.apache.spark.sql.types.Decimal input, int precision, int scale) {
        throw new UnsupportedOperationException(
            "PrimitiveArrayWriter does not support Decimal or ordinal-based writes");
    }

    // ===== Internal Methods =====

    private void ensureCapacity() {
        if (count >= elementCapacity) {
            growBuffer(count + 1);
        }
    }

    private void growBuffer(int targetCapacity) {
        // Update cursor to current end of data
        increaseCursor(writePosition - cursor());

        // Grow buffer to accommodate target capacity
        // BufferHolder.grow() handles exponential growth internally
        int neededBytes = roundToWord((targetCapacity - count) * elementSize);
        grow(neededBytes);

        // Recalculate element capacity after buffer growth
        // dataOffset already includes Platform.BYTE_ARRAY_OFFSET, so subtract it
        int availableBytes = getBuffer().length - (dataOffset - Platform.BYTE_ARRAY_OFFSET);
        this.elementCapacity = availableBytes / elementSize;
    }

    /**
     * Complete the array, writing header and moving data if needed.
     * @return the number of elements written
     */
    public int complete() {
        byte[] buffer = getBuffer();

        // Calculate final header size based on actual count
        int headerBytes = calculateHeaderPortionInBytes(count);

        if (headerBytes > 16) {
            // Arrays >64 elements need larger header - move data forward
            int dataSize = count * elementSize;
            Platform.copyMemory(
                buffer, dataOffset,                    // from: current data at offset+16
                buffer, startingOffset + headerBytes,  // to: after full header
                dataSize
            );
        }

        // Zero out the entire null bitmap (pre-allocated 8 bytes covers ≤64 elements)
        for (int i = 8; i < headerBytes; i += 8) {
            Platform.putLong(buffer, startingOffset + i, 0L);
        }

        // Write element count
        Platform.putLong(buffer, startingOffset, count);

        // Update cursor to final position
        int totalSize = headerBytes + roundToWord(count * elementSize);
        increaseCursor(startingOffset + totalSize - cursor());

        return count;
    }

    /**
     * Get the current number of elements written.
     */
    public int size() {
        return count;
    }

    /**
     * Get the starting offset of this array in the buffer.
     */
    public int getStartingOffset() {
        return startingOffset;
    }

    // ===== Utility Methods =====

    private static int roundToWord(int bytes) {
        return (bytes + 7) & ~7;
    }

    private static int calculateHeaderPortionInBytes(int numElements) {
        // Same as UnsafeArrayData.calculateHeaderPortionInBytes
        return 8 + ((numElements + 63) / 64) * 8;
    }
}