package fastproto;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import org.apache.spark.sql.catalyst.expressions.codegen.NullDefaultRowWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.bitset.BitSetMethods;

/**
 * High-performance helper for UnsafeRowWriter null bit manipulation.
 * Uses MethodHandles to access private fields efficiently while maintaining performance.
 */
public class UnsafeRowWriterHelper {
    private static final MethodHandle startingOffsetGetter;
    private static final MethodHandle nullBitsSizeGetter;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();

            // Get private fields via reflection then create MethodHandles for performance
            Field startingOffsetField = UnsafeWriter.class.getDeclaredField("startingOffset");
            Field nullBitsSizeField = UnsafeRowWriter.class.getDeclaredField("nullBitsSize");

            startingOffsetField.setAccessible(true);
            nullBitsSizeField.setAccessible(true);

            startingOffsetGetter = lookup.unreflectGetter(startingOffsetField);
            nullBitsSizeGetter = lookup.unreflectGetter(nullBitsSizeField);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize UnsafeRowWriterHelper", e);
        }
    }

    /**
     * Set all fields to null state (opposite of zeroOutNullBytes).
     * This correctly initializes the null bit vector so that absent protobuf fields
     * are properly marked as null instead of appearing as non-null with uninitialized data.
     *
     * For NullDefaultRowWriter instances, this is a no-op since resetRowWriter() already
     * calls setAllNullBytes() automatically.
     *
     * @param writer the UnsafeWriter to initialize
     */
    public static void setAllFieldsNull(UnsafeWriter writer) {
        // NullDefaultRowWriter already sets all fields to null in resetRowWriter()
        if (writer instanceof NullDefaultRowWriter) {
            return; // No-op - already handled by resetRowWriter()
        }

        // Handle UnsafeRowWriter instances
        try {
            int startingOffset = (int) startingOffsetGetter.invokeExact(writer);
            int nullBitsSize = (int) nullBitsSizeGetter.invokeExact(writer);

            // Set all bits to 1 (null state) - opposite of zeroOutNullBytes which sets to 0
            for (int i = 0; i < nullBitsSize; i += 8) {
                Platform.putLong(writer.getBuffer(), startingOffset + i, -1L);
            }
        } catch (Throwable e) {
            throw new RuntimeException("Failed to set null bits", e);
        }
    }

    /**
     * Clear the null bit for a specific field (mark it as non-null).
     * This should be called after writing data to a field when starting with all fields null.
     *
     * @param writer the UnsafeRowWriter
     * @param ordinal the field ordinal to mark as non-null
     */
    public static void clearNullAt(UnsafeRowWriter writer, int ordinal) {
        try {
            int startingOffset = (int) startingOffsetGetter.invokeExact((UnsafeWriter) writer);
            BitSetMethods.unset(writer.getBuffer(), startingOffset, ordinal);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to clear null bit", e);
        }
    }

    /**
     * Clear the null bit for a specific field using cached starting offset.
     * This is a performance optimization to avoid repeated MethodHandle calls.
     *
     * @param writer the UnsafeRowWriter
     * @param startingOffset the cached starting offset from getStartingOffset
     * @param ordinal the field ordinal to mark as non-null
     */
    public static void clearNullAt(UnsafeRowWriter writer, int startingOffset, int ordinal) {
        BitSetMethods.unset(writer.getBuffer(), startingOffset, ordinal);
    }

    /**
     * Get the starting offset for a writer (for caching).
     *
     * @param writer the UnsafeRowWriter
     * @return the starting offset
     */
    public static int getStartingOffset(UnsafeWriter writer) {
        try {
            return (int) startingOffsetGetter.invokeExact(writer);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to get starting offset", e);
        }
    }
}