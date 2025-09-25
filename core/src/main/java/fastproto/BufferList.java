package fastproto;

import java.nio.ByteBuffer;

/**
 * Specialized growable ByteBuffer list for repeated message field parsing.
 * Uses public fields for performance and direct access in generated code.
 * Optimized for ByteBuffer accumulation to avoid data copying for nested messages.
 * This avoids generic overhead compared to GenericList<ByteBuffer>.
 */
public class BufferList extends ObjectList<ByteBuffer> {
    @Override
    protected ByteBuffer[] newArray(int len) {
        return new ByteBuffer[len];
    }
}