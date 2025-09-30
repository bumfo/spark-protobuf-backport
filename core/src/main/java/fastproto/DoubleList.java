package fastproto;

/**
 * Efficient growable double array for packed repeated field parsing.
 * Uses public fields for performance and direct access in generated code.
 */
public class DoubleList extends FastList {
    public double[] array;

    public DoubleList() {
        this.array = new double[DEFAULT_CAPACITY];
        this.count = 0;
    }

    /**
     * Ensure capacity for at least minCapacity elements.
     * Preserves existing data up to current count.
     */
    public void sizeHint(int minCapacity) {
        if (array.length < minCapacity) {
            int newCapacity = Math.max(array.length * 2, minCapacity);
            double[] newArray = new double[newCapacity];
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
        double[] newArray = new double[newCapacity];
        System.arraycopy(array, 0, newArray, 0, currentCount);
        array = newArray;
    }

    /**
     * Add a single value to the list, growing if necessary.
     * Used for single repeated field values (not packed parsing).
     */
    public void add(double value) {
        if (count >= array.length) {
            grow(count);
        }
        array[count++] = value;
    }
}