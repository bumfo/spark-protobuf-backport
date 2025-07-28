/*
 * Backport of Spark 3.4's ProtobufOptions to Spark 3.2.1.
 *
 * Holds reader options for the Protobuf connector, stored in a
 * case‑insensitive map.  Provides defaults for parse mode and
 * recursive field depth.  In Spark 3.4 this class extends
 * `FileSourceOptions`; in the backport we implement only the options
 * relevant to Protobuf reading and writing.
 */

package org.apache.spark.sql.protobuf.backport.utils

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailFastMode, ParseMode}

/**
 * Options for the Protobuf reader and writer, stored in a case‑insensitive
 * manner.  Only a subset of Spark 3.4's options are implemented in this
 * backport.
 *
 * @param parameters user‑provided options
 * @param conf       Hadoop configuration used by the reader
 */
private[backport] class ProtobufOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    @transient val conf: Configuration)
    extends Serializable {

  def this(parameters: Map[String, String], conf: Configuration) = {
    this(CaseInsensitiveMap(parameters), conf)
  }

  /** Reader parse mode.  Supported values are "PERMISSIVE" (default) and
   *  "FAILFAST".  Defaults to [[FailFastMode]].
   */
  val parseMode: ParseMode = parameters.get("mode").map(ParseMode.fromString).getOrElse(FailFastMode)

  /** Maximum recursion depth for nested message fields.  A value of -1
   *  disables recursion entirely.  Values greater than 10 are prohibited.
   */
  val recursiveFieldMaxDepth: Int = parameters.getOrElse("recursive.fields.max.depth", "-1").toInt
}

private[backport] object ProtobufOptions {
  def apply(parameters: Map[String, String]): ProtobufOptions = {
    // In Spark 3.2.x we cannot reliably access SparkSession.sessionState.newHadoopConf because
    // the SessionState API may differ or not be available.  Use a fresh Hadoop Configuration
    // instead.  Users can specify a different configuration when instantiating ProtobufOptions
    // directly if needed.
    new ProtobufOptions(CaseInsensitiveMap(parameters), new Configuration())
  }
}