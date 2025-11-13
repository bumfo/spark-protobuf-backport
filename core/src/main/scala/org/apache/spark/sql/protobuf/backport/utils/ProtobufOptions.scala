/*
 * Backport of Spark 3.4's ProtobufOptions to Spark 3.2.1.
 *
 * Holds reader options for the Protobuf connector, stored in a
 * case-insensitive map.  Provides defaults for parse mode and
 * recursive field depth.  In Spark 3.4 this class extends
 * `FileSourceOptions`; in the backport we implement only the options
 * relevant to Protobuf reading and writing.
 */

package org.apache.spark.sql.protobuf.backport.utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailFastMode, ParseMode}

/**
 * Options for the Protobuf reader and writer, stored in a case-insensitive manner.
 * Only a subset of Spark 3.4's options are implemented in this backport.
 *
 * Configuration Guide:
 *
 * 1. Parse Mode (mode):
 *    - "PERMISSIVE": Returns null on parsing errors
 *    - "FAILFAST": Throws exception on errors (default)
 *    Applies to: All parsers (error handling)
 *
 * 2. Recursive Fields Mode (recursive.fields.mode):
 *    User-facing values (recommended):
 *    - "" (empty): Default behavior based on allow_recursion and depth
 *    - "drop": Drop recursive fields from schema
 *    - "binary": Mock recursive fields as BinaryType
 *
 *    Internal values (discouraged for users):
 *    - "fail": Forbid recursive schemas (throw error on recursion)
 *    - "recursive": Allow RecursiveStructType (only valid with depth=-1)
 *
 * 3. Recursive Fields Max Depth (recursive.fields.max.depth):
 *    - "-1": Default behavior (default)
 *    - "0+": Maximum recursive depth before applying mode handling
 *
 * Configuration Precedence:
 *
 * When depth=-1 (default/unlimited):
 * - mode="" + allow_recursion=false → fail (forbid recursion)
 * - mode="" + allow_recursion=true → recursive (RecursiveStructType)
 * - mode="drop"/"binary" → IGNORED (depth=-1 takes precedence)
 * - mode="fail" → fail (explicit)
 * - mode="recursive" → recursive (explicit)
 *
 * When depth>=0 (depth-limited):
 * - mode="" → drop (default)
 * - mode="drop" → drop recursive fields beyond depth
 * - mode="binary" → mock recursive fields beyond depth as BinaryType
 * - mode="fail" → forbid recursion (unusual with depth limit)
 * - mode="recursive" → ERROR (illegal combination)
 * - allow_recursion → IGNORED (mode takes precedence)
 *
 * Depth Counting:
 * - Depth is counted from INSIDE a recursive cycle
 * - Example A→B→A→C→A: depths are 0, 0, 0, 1, 2
 * - C is first visit but inside cycle, so depth 1
 *
 * Parser Defaults:
 * - WireFormat parser: allow_recursion=true
 * - Generated/Dynamic parsers: allow_recursion=false
 *
 * @param parameters user-provided options
 * @param conf       Hadoop configuration used by the reader
 * @param allowRecursion Whether recursion is allowed (set by parser selection)
 */
private[backport] class ProtobufOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    @transient val conf: Configuration,
    val allowRecursion: Boolean = false)
  extends Serializable {

  def this(parameters: Map[String, String], conf: Configuration) = {
    this(CaseInsensitiveMap(parameters), conf, allowRecursion = false)
  }

  def this(parameters: Map[String, String], conf: Configuration, allowRecursion: Boolean) = {
    this(CaseInsensitiveMap(parameters), conf, allowRecursion)
  }

  /**
   * Reader parse mode.
   *
   * Controls error handling behavior for all parsers.
   * - PERMISSIVE: Returns null when parsing fails
   * - FAILFAST: Throws exception when parsing fails (default)
   *
   * Applies to: All parsers
   */
  val parseMode: ParseMode = parameters.get("mode").map(ParseMode.fromString).getOrElse(FailFastMode)

  /**
   * Maximum recursion depth for nested message fields.
   *
   * Controls when mode-based handling kicks in for recursive fields.
   * - "-1": Default behavior (unlimited for recursive mode, forbidden for fail mode)
   * - "0+": Apply mode handling when recursive depth exceeds this value
   *
   * Depth is counted from inside a recursive cycle (see class documentation).
   *
   * Applies to: All parsers via RecursiveSchemaConverters
   */
  val recursiveFieldMaxDepth: Int = parameters.getOrElse("recursive.fields.max.depth", "-1").toInt

  /**
   * How to handle recursive message types in schema conversion.
   *
   * User-facing values:
   * - "" (empty, default): Behavior depends on allow_recursion and depth
   * - "drop": Drop recursive fields from schema
   * - "binary": Replace recursive fields with BinaryType
   *
   * Internal values (not recommended for users):
   * - "fail": Throw exception on recursion detection
   * - "recursive": Use RecursiveStructType (only valid with depth=-1)
   *
   * Precedence rules:
   * - depth=-1: mode="drop"/"binary" ignored, uses allow_recursion
   * - depth>=0: allow_recursion ignored, uses mode (default="drop")
   *
   * Applies to: All parsers via RecursiveSchemaConverters
   */
  val recursiveFieldsMode: String = {
    val mode = parameters.getOrElse("recursive.fields.mode", "")

    // Validate mode value
    if (!Set("", "drop", "binary", "fail", "recursive").contains(mode)) {
      throw new IllegalArgumentException(
        s"Invalid value for 'recursive.fields.mode': '$mode'. " +
        "Supported values are: '' (empty), 'drop', 'binary', 'fail', 'recursive'")
    }

    // Validate illegal combination: recursive + depth>=0
    if (mode == "recursive" && recursiveFieldMaxDepth >= 0) {
      throw new IllegalArgumentException(
        "Invalid configuration: 'recursive.fields.mode=recursive' cannot be used with " +
        s"'recursive.fields.max.depth=${recursiveFieldMaxDepth}'. " +
        "RecursiveStructType requires unlimited depth (depth=-1).")
    }

    mode
  }
}

private[backport] object ProtobufOptions {
  def apply(parameters: Map[String, String]): ProtobufOptions = {
    // In Spark 3.2.x we cannot reliably access SparkSession.sessionState.newHadoopConf because
    // the SessionState API may differ or not be available.  Use a fresh Hadoop Configuration
    // instead.  Users can specify a different configuration when instantiating ProtobufOptions
    // directly if needed.
    new ProtobufOptions(CaseInsensitiveMap(parameters), new Configuration(), allowRecursion = false)
  }

  def apply(parameters: Map[String, String], allowRecursion: Boolean): ProtobufOptions = {
    new ProtobufOptions(CaseInsensitiveMap(parameters), new Configuration(), allowRecursion)
  }
}
