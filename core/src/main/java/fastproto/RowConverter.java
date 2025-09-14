package fastproto;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

/**
 * Base interface for converting protobuf binary data into Spark's
 * {@link InternalRow}. This interface supports both direct binary conversion
 * and message-based conversion through its sub-interfaces.
 * <p>
 * Implementations are usually generated at runtime by the
 * {@link ProtoToRowGenerator} using Janino compilation.
 */
public interface RowConverter extends Serializable {

    /**
     * Convert protobuf binary data into Spark's internal row representation.
     * This is the primary conversion method that takes raw protobuf bytes
     * and produces an {@link InternalRow}.
     *
     * @param binary the protobuf binary data to convert
     * @return an {@link InternalRow} containing the extracted field values
     */
    default InternalRow convert(byte[] binary) throws java.io.IOException {
        return convert(binary, null);
    }

    /**
     * Convert protobuf binary data using a shared UnsafeWriter for BufferHolder sharing.
     * This method enables efficient nested conversions by sharing the underlying
     * buffer across the entire row tree, reducing memory allocations.
     * <p>
     * When parentWriter is provided, the implementation should create a new
     * UnsafeRowWriter that shares the BufferHolder from the parent writer.
     *
     * @param binary       the protobuf binary data to convert
     * @param parentWriter the parent UnsafeWriter to share BufferHolder with, can be null
     * @return an {@link InternalRow} containing the extracted field values
     */
    InternalRow convert(byte[] binary, UnsafeWriter parentWriter) throws java.io.IOException;

    /**
     * The Catalyst schema corresponding to this converter. This schema
     * describes the structure of the {@link InternalRow} produced by {@link #convert}.
     * Implementations should return the {@link StructType} used to build the
     * UnsafeRow. This allows callers to inspect the field names and types
     * without regenerating the schema from a descriptor.
     *
     * @return the Spark SQL schema for this converter
     */
    StructType schema();
}
