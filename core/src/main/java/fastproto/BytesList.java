package fastproto;

/**
 * Efficient growable byte array list for repeated string/bytes/message field parsing.
 * Uses public fields for performance and direct access in generated code.
 * Each element is a byte array representing a string, bytes field, or serialized message.
 */
public class BytesList extends ObjectList<byte[]> {
    @Override
    protected byte[][] newArray(int len) {
        return new byte[len][];
    }
}