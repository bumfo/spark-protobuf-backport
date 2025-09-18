package fastproto;

/**
 * Efficient growable boolean array for packed repeated field parsing.
 * Uses public fields for performance and direct access in generated code.
 */
public class BooleanList {
    public boolean[] array;
    public int count;

    private static final int DEFAULT_CAPACITY = 10;

    public BooleanList() {
        this.array = new boolean[DEFAULT_CAPACITY];
        this.count = 0;
    }

    /**
     * Ensure capacity for at least minCapacity elements.
     * Preserves existing data up to current count.
     */
    public void sizeHint(int minCapacity) {
        if (array.length < minCapacity) {
            int newCapacity = Math.max(array.length * 2, minCapacity);
            boolean[] newArray = new boolean[newCapacity];
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
        boolean[] newArray = new boolean[newCapacity];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        array = newArray;
    }

    /**
     * Add a single value to the list, growing if necessary.
     * Used for single repeated field values (not packed parsing).
     */
    public void add(boolean value) {
        if (count >= array.length) {
            grow(count);
        }
        array[count++] = value;
    }
}