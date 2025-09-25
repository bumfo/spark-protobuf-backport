package fastproto;

/**
 * Generic growable list for repeated field parsing.
 * Uses public fields for performance and direct access in generated code.
 * Can hold any object type T, used primarily for ByteBuffer accumulation
 * to avoid data copying for nested messages.
 */
public class GenericList<T> extends ObjectList<T> {
    @Override
    @SuppressWarnings("unchecked")
    protected T[] newArray(int len) {
        return (T[]) new Object[len];
    }
}