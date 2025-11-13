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
 *    - "struct": Use RecursiveStructType with true circular references (default)
 *    - "binary": Mock recursive fields as BinaryType
 *    - "drop": Omit recursive fields from schema entirely
 *    Currently applies to: WireFormat parser (via RecursiveSchemaConverters)
 *    Future: Will apply to all parsers
 *
 * 3. Recursive Fields Max Depth (recursive.fields.max.depth):
 *    - "-1": Unlimited recursion (default, only with mode="struct")
 *    - "0+": Maximum nesting depth before dropping fields
 *    Currently applies to: Generated/Dynamic parsers (via SchemaConverters)
 *    Future: Will apply to all parsers
 *
 * Interaction between mode and maxDepth:
 * - mode="struct" + maxDepth=-1: RecursiveStructType (unlimited recursion)
 * - mode="struct" + maxDepth>=0: Regular StructType with depth limit
 * - mode="binary": maxDepth ignored, recursive fields become BinaryType
 * - mode="drop": maxDepth ignored, recursive fields dropped
 *
 * Parser Compatibility Matrix (Current State):
 * | Config                     | WireFormat | Generated | Dynamic | Applied At |
 * |----------------------------|------------|-----------|---------|------------|
 * | mode                       | ✓          | ✓         | ✓       | Error handling |
 * | recursive.fields.mode      | ✓          | (future)  | (future)| Schema generation |
 * | recursive.fields.max.depth | (future)   | ✓         | ✓       | Schema generation |
 *
 * Future Migration:
 * - All parsers will use RecursiveSchemaConverters.toSqlType() for schema generation
 * - All configs will be respected consistently across parser types
 * - See RecursiveSchemaConverters.toSqlType() for unified config handling
 *
 * @param parameters user-provided options
 * @param conf       Hadoop configuration used by the reader
 */
private[backport] class ProtobufOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    @transient val conf: Configuration)
  extends Serializable {

  def this(parameters: Map[String, String], conf: Configuration) = {
    this(CaseInsensitiveMap(parameters), conf)
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
   * Controls how deep nested messages can go before fields are dropped.
   * - "-1": Unlimited depth (default, works with recursive.fields.mode="struct")
   * - "0+": Maximum nesting levels allowed
   *
   * Currently applies to:
   * - Generated parser (via SchemaConverters)
   * - Dynamic parser (via SchemaConverters)
   *
   * Future: Will work with all parsers via RecursiveSchemaConverters.
   * When mode="struct" and maxDepth is set, produces regular StructType (not RecursiveStructType).
   */
  val recursiveFieldMaxDepth: Int = parameters.getOrElse("recursive.fields.max.depth", "-1").toInt

  /**
   * How to handle recursive message types in schema conversion.
   *
   * Controls schema generation for messages with recursive references.
   * - "struct": Use RecursiveStructType with true circular references (default)
   * - "binary": Replace recursive fields with BinaryType
   * - "drop": Remove recursive fields from schema entirely
   *
   * Currently applies to:
   * - WireFormat parser (via RecursiveSchemaConverters)
   *
   * Future: Will work with all parsers via RecursiveSchemaConverters.
   *
   * Interaction with recursive.fields.max.depth:
   * - mode="struct" + maxDepth=-1: RecursiveStructType (unlimited)
   * - mode="struct" + maxDepth=N: Regular StructType with depth limit
   * - mode="binary": maxDepth ignored
   * - mode="drop": maxDepth ignored
   */
  val recursiveFieldsMode: String = {
    val mode = parameters.getOrElse("recursive.fields.mode", "struct")
    if (!Set("struct", "binary", "drop").contains(mode)) {
      throw new IllegalArgumentException(
        s"Invalid value for 'recursive.fields.mode': '$mode'. " +
        "Supported values are: 'struct', 'binary', 'drop'")
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
    new ProtobufOptions(CaseInsensitiveMap(parameters), new Configuration())
  }
}
