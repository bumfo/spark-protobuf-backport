/*
 * Shim providing execution‑time error helpers missing in Spark 3.2.x.
 *
 * In Spark 3.4 the Protobuf implementation throws structured
 * `QueryExecutionErrors`.  These helpers are not available in Spark 3.2.x,
 * so this shim provides a minimal API used by the backport.  All
 * methods return a RuntimeException wrapping the underlying cause and
 * explanatory text.
 */

package org.apache.spark.sql.protobuf.backport.shims

object QueryExecutionErrors {
  /**
   * Raised when a Protobuf message cannot be parsed in fail‑fast mode.
   * Returns a RuntimeException with the original cause attached.
   */
  def malformedProtobufMessageDetectedInMessageParsingError(cause: Throwable): RuntimeException = {
    new RuntimeException("Malformed Protobuf message detected during parsing", cause)
  }
}