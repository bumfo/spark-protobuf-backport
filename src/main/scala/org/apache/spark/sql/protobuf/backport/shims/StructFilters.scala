/*
 * Simple shim providing predicate pushdown filters for the Protobuf backport.
 *
 * Spark 3.4 introduces classes `StructFilters` and `NoopFilters` to support
 * predicate pushdown during file reading.  These APIs are not available in
 * Spark 3.2.x, but the backported Protobuf reader accepts a filters object
 * for API completeness.  This shim defines a minimal interface and a no‑op
 * implementation that always returns false (i.e. never skips a row).
 */

package org.apache.spark.sql.protobuf.backport.shims

import org.apache.spark.sql.catalyst.InternalRow

/**
 * Abstract predicate filter for struct reads.  Implementations decide
 * whether a particular field index should cause the row to be skipped.
 */
trait StructFilters {
  /**
   * Determine whether the current row should be skipped based on the given
   * field index.  Returns true if the row should be skipped, false
   * otherwise.  The default implementation always returns false.
   */
  def skipRow(row: InternalRow, ordinal: Int): Boolean
}

/**
 * A trivial filter that never skips any rows.  Used as the default
 * implementation in the Protobuf deserializer to preserve behaviour on
 * Spark 3.2.x where filter pushdown is not implemented for the reader.
 */
class NoopFilters extends StructFilters {
  override def skipRow(row: InternalRow, ordinal: Int): Boolean = false
}