package fastproto;

/**
 * Efficient growable int array for packed repeated field parsing.
 * Uses public fields for performance and direct access in generated code.
 */
public class IntList {
    public int[] array;
    public int count;

    private static final int DEFAULT_CAPACITY = 10;

    public IntList() {
        this.array = new int[DEFAULT_CAPACITY];
        this.count = 0;
    }

    /**
     * Ensure capacity for at least minCapacity elements.
     * Preserves existing data up to current count.
     */
    public void sizeHint(int minCapacity) {
        if (array.length < minCapacity) {
            int newCapacity = Math.max(array.length * 2, minCapacity);
            int[] newArray = new int[newCapacity];
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
        int[] newArray = new int[newCapacity];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        array = newArray;
    }

    /**
     * Add a single value to the list, growing if necessary.
     * Used for single repeated field values (not packed parsing).
     */
    public void add(int value) {
        if (count >= array.length) {
            grow(count);
        }
        array[count++] = value;
    }
}