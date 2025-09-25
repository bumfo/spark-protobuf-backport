package fastproto;

/**
 * Efficient growable long array for packed repeated field parsing.
 * Uses public fields for performance and direct access in generated code.
 */
public class LongList extends FastList {
    public long[] array;

    public LongList() {
        this.array = new long[DEFAULT_CAPACITY];
        this.count = 0;
    }

    /**
     * Ensure capacity for at least minCapacity elements.
     * Preserves existing data up to current count.
     */
    public void sizeHint(int minCapacity) {
        if (array.length < minCapacity) {
            int newCapacity = Math.max(array.length * 2, minCapacity);
            long[] newArray = new long[newCapacity];
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
        long[] newArray = new long[newCapacity];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        array = newArray;
    }

    /**
     * Add a single value to the list, growing if necessary.
     * Used for single repeated field values (not packed parsing).
     */
    public void add(long value) {
        if (count >= array.length) {
            grow(count);
        }
        array[count++] = value;
    }
}