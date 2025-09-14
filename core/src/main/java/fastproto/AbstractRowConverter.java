package fastproto;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.StructType;

/**
 * Base class for row converters that convert binary protobuf data to Spark SQL rows.
 * <p>
 * Provides shared writer management and conversion logic for different converter implementations.
 */
public abstract class AbstractRowConverter implements RowConverter {

    protected final StructType schema;
    private final UnsafeRowWriter instanceWriter;

    public AbstractRowConverter(StructType schema) {
        this.schema = schema;
        this.instanceWriter = new UnsafeRowWriter(schema.length());
    }

    protected UnsafeRowWriter prepareWriter(UnsafeWriter parentWriter) {
        if (parentWriter == null) {
            instanceWriter.reset();
            instanceWriter.zeroOutNullBytes();
            return instanceWriter;
        } else {
            UnsafeRowWriter writer = new UnsafeRowWriter(parentWriter, schema.length());
            writer.resetRowWriter();
            writer.zeroOutNullBytes();
            return writer;
        }
    }

    protected abstract void writeData(byte[] binary, UnsafeRowWriter writer) throws java.io.IOException;

    public StructType schema() {
        return schema;
    }

    public void convert(byte[] binary, UnsafeRowWriter writer) throws java.io.IOException {
        writeData(binary, writer);
    }

    @Override
    public InternalRow convert(byte[] binary, UnsafeWriter parentWriter) throws java.io.IOException {
        UnsafeRowWriter writer = prepareWriter(parentWriter);
        writeData(binary, writer);
        return parentWriter == null ? writer.getRow() : null;
    }
}