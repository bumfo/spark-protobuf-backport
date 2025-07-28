package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Utility object to expose Spark's internal DataFrame creation API.  This is
 * required when constructing a DataFrame from an RDD of [[InternalRow]]s
 * outside of Spark's public API.  Spark's `internalCreateDataFrame` method
 * is package-private and cannot be accessed directly from arbitrary packages.
 * By defining this helper inside the `org.apache.spark.sql` package we can
 * call the method on behalf of users.
 */
object DevInterface {
  /**
   * Create a DataFrame from a sequence of internal rows.  This delegates to
   * `SparkSession.internalCreateDataFrame` which bypasses schema inference
   * and uses the provided schema exactly as specified.
   *
   * @param spark the active [[SparkSession]]
   * @param catalystRows the underlying row data
   * @param schema the Catalyst schema describing the rows
   * @param isStreaming flag indicating whether the resulting DataFrame is streaming
   * @return a [[DataFrame]] with the specified schema and rows
   */
  def internalCreateDataFrame(
      spark: SparkSession,
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean = false
  ): DataFrame = {
    spark.internalCreateDataFrame(catalystRows, schema, isStreaming)
  }
}
