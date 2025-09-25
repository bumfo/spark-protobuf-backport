package fastproto;

import java.lang.reflect.Array;

/**
 * Generic growable list for repeated field parsing.
 * Uses public fields for performance and direct access in generated code.
 * Can hold any object type T, used primarily for ByteBuffer accumulation
 * to avoid data copying for nested messages.
 */
public class GenericList<T> extends ObjectList<T> {
    private final Class<T> elementClass;

    public GenericList(Class<T> elementClass) {
        super(false); // Don't initialize in parent constructor
        if (elementClass == null) {
            throw new IllegalArgumentException("elementClass cannot be null");
        }
        this.elementClass = elementClass;
        // Now initialize manually
        this.array = newArray(DEFAULT_CAPACITY);
        this.count = 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T[] newArray(int len) {
        return (T[]) Array.newInstance(elementClass, len);
    }

}