package fastproto;

public abstract class FastList {
    public int count;

    protected static final int DEFAULT_CAPACITY = 10;

    public void reset() {
        count = 0;
    }

    public abstract void sizeHint(int minCapacity);

    public abstract void grow(int currentCount);

    // no common `add` method to avoid boxing
}
