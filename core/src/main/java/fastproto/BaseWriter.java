package fastproto;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;

public abstract class BaseWriter extends UnsafeWriter {
    protected final UnsafeRow row;
    protected final int nullBitsSize;
    protected final int fixedSize;

    protected BaseWriter(UnsafeRow row, UnsafeWriter writer, int numFields) {
        super(writer.getBufferHolder());
        this.row = row;
        this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);
        this.fixedSize = nullBitsSize + 8 * numFields;
        this.startingOffset = cursor();
    }

    protected BaseWriter(UnsafeRowWriter writer, int numFields) {
        this(writer.getRow(), writer, numFields);
    }
}
