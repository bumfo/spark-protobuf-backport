package fastproto;

import java.nio.ByteBuffer;

/**
 * Specialized growable ByteBuffer list for repeated message field parsing.
 * Uses public fields for performance and direct access in generated code.
 * Optimized for ByteBuffer accumulation to avoid data copying for nested messages.
 * This avoids generic overhead compared to GenericList<ByteBuffer>.
 */
public class BufferList extends FastList {
    public ByteBuffer[] array;

    public BufferList() {
        this.array = new ByteBuffer[DEFAULT_CAPACITY];
        this.count = 0;
    }

    /**
     * Ensure capacity for at least minCapacity elements.
     * Preserves existing data up to current count.
     */
    public void sizeHint(int minCapacity) {
        if (array.length < minCapacity) {
            int newCapacity = Math.max(array.length * 2, minCapacity);
            ByteBuffer[] newArray = new ByteBuffer[newCapacity];
            System.arraycopy(array, 0, newArray, 0, count);
            array = newArray;
        }
    }

    /**
     * Grow array capacity, using currentCount for data preservation.
     * Called during parsing when we've already updated local count.
     */
    public void grow(int currentCount) {
        int newCapacity = array.length * 2;
        ByteBuffer[] newArray = new ByteBuffer[newCapacity];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        array = newArray;
    }

    /**
     * Add a single ByteBuffer to the list, growing if necessary.
     * Used for repeated message field values.
     */
    public void add(ByteBuffer value) {
        if (count >= array.length) {
            grow(count);
        }
        array[count++] = value;
    }
}