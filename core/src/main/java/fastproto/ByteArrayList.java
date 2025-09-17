package fastproto;

/**
 * Efficient growable byte array list for repeated string/bytes/message field parsing.
 * Uses public fields for performance and direct access in generated code.
 * Each element is a byte array representing a string, bytes field, or serialized message.
 */
public class ByteArrayList {
    public byte[][] array;
    public int count;

    private static final int DEFAULT_CAPACITY = 10;

    public ByteArrayList() {
        this.array = new byte[DEFAULT_CAPACITY][];
        this.count = 0;
    }

    /**
     * Ensure capacity for at least minCapacity elements.
     * Preserves existing data up to current count.
     */
    public void sizeHint(int minCapacity) {
        if (array.length < minCapacity) {
            int newCapacity = Math.max(array.length * 2, minCapacity);
            byte[][] newArray = new byte[newCapacity][];
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
        byte[][] newArray = new byte[newCapacity][];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        array = newArray;
    }

    /**
     * Add a single byte array to the list, growing if necessary.
     * Used for repeated string/bytes/message field values.
     */
    public void add(byte[] value) {
        if (count >= array.length) {
            grow(count);
        }
        array[count++] = value;
    }
}