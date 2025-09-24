package fastproto;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;

/**
 * Base wrapper for UnsafeWriter that provides cross-classloader BufferHolder access.
 * Uses existing public UnsafeRowWriter to avoid package-private BufferHolder creation.
 */
public abstract class BaseWriter extends UnsafeWriter {
    protected final UnsafeRow row;
    protected final int nullBitsSize;
    protected final int fixedSize;

    /**
     * Create BaseWriter using an existing UnsafeWriter's BufferHolder.
     * Avoids class loader issues by using the public getBufferHolder() method.
     */
    protected BaseWriter(UnsafeRow row, UnsafeWriter writer, int numFields) {
        super(writer.getBufferHolder());
        this.row = row;
        this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);
        this.fixedSize = nullBitsSize + 8 * numFields;
        this.startingOffset = cursor();
    }

    /**
     * Create BaseWriter wrapping an UnsafeRowWriter.
     * Uses the writer's existing row and BufferHolder for optimal compatibility.
     */
    protected BaseWriter(UnsafeRowWriter writer, int numFields) {
        this(writer.getRow(), writer, numFields);
    }
}
