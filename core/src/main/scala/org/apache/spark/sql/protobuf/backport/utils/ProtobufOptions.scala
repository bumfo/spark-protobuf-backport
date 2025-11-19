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
 *    - "recursive": Allow RecursiveStructType (only valid with depth=0 or depth=-1)
 *
 * 3. Recursive Fields Max Depth (recursive.fields.max.depth):
 *    Spark-aligned values:
 *    - "-1": Forbid recursive fields (Spark 3.4+ default, throws error on recursion)
 *    - "0": Unlimited recursion (our extension, enables RecursiveStructType)
 *    - "1": Drop all recursive fields (Spark semantics: 0 recursions allowed)
 *    - "2": Allow recursed once (Spark semantics: field appears twice total)
 *    - "3-10": Allow recursed N-1 times (Spark semantics: field appears N times total)
 *
 * Configuration Precedence:
 *
 * When depth=-1 (forbid, Spark default):
 * - mode="" → fail if allow_recursion=false, recursive if allow_recursion=true
 * - mode="fail" → fail (explicit)
 * - mode="drop"/"binary"/"recursive" → use specified mode (override Spark default)
 * - allow_recursion → used when mode="" (determines default behavior)
 *
 * When depth=0 (unlimited, our extension):
 * - mode="" → recursive (RecursiveStructType, always)
 * - mode="recursive" → recursive (explicit)
 * - mode="drop"/"binary"/"fail" → use specified mode (override unlimited default)
 * - allow_recursion → IGNORED (mode="" always uses recursive)
 *
 * When depth>=1 (Spark-aligned depth limit):
 * - mode="" → drop (default for depth-limited, always)
 * - mode="drop"/"binary" → use specified mode
 * - mode="fail" → forbid (unusual with depth limit)
 * - mode="recursive" → ERROR (illegal combination)
 * - allow_recursion → IGNORED (mode takes precedence)
 *
 * Depth Semantics:
 * - depth=1: Drop immediately at first cycle detection
 * - depth=2: Allow 1 message traversal after entering cycle
 * - depth=N: Allow N-1 message traversals after entering cycle
 *
 * IMPORTANT: Depth counts ALL nested message fields visited inside a cycle,
 * not per-type occurrences. In mutual recursion A↔B, visiting B after entering
 * the cycle increments depth even though B is not a recursive edge.
 *
 * Internal Implementation:
 * - Spark depth values used directly (no conversion)
 * - First cycle occurrence is at depth=1
 * - Depth increments for EVERY message field visited inside cycle
 * - Example A→B→A→C→A: internal depths are 0, 0, 1, 2, 3
 *   (A and B not in cycle=0, first A recursion=1, C inside cycle=2, second A recursion=3)
 *
 * Semantic Difference from Spark SQL:
 *
 * Our implementation counts nested message field traversals after entering a cycle.
 * The depth counter increments for EVERY message field visited while in a cycle,
 * including non-recursive intermediate types.
 *
 * Example - Mutual recursion A↔B with depth=3:
 * - Our result: A→B→A→B(primitives only, recursive fields dropped)
 * - Cycle path: A(depth 0) → B(depth 0) → A(cycle depth 1) → B(next depth 2, cycle depth 3)
 * - At cycle depth 3: B's recursive fields (a_field) are dropped
 *
 * Spark SQL tracks total occurrences per message type independently:
 * - Spark depth=3 for type A: allows A to appear 3 times total
 * - Spark depth=3 for type B: allows B to appear 3 times total
 * - Each type tracked separately
 *
 * This difference means our depth limit may drop fields earlier than expected
 * in complex mutual recursion patterns.
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
   * Maximum recursion depth for nested message fields (Spark-aligned semantics).
   *
   * Spark 3.4+ compatible values:
   * - "-1": Forbid recursive fields (Spark default, throws error on recursion)
   * - "1": Drop all recursive fields (no recursion allowed)
   * - "2": Allow recursed once (field appears twice total)
   * - "3-10": Allow recursed N-1 times (field appears N times total)
   *
   * Our extension:
   * - "0": Unlimited recursion (enables RecursiveStructType, not in Spark)
   *
   * Validation: Must be in range [-1, 10]. Values > 10 can cause stack overflows.
   *
   * Internal conversion: User depth N → Internal maxRecursiveDepth N-1
   * (Spark counts total appearances, we count depth from recursion point)
   *
   * Applies to: All parsers via RecursiveSchemaConverters
   */
  val recursiveFieldMaxDepth: Int = {
    val depth = parameters.getOrElse("recursive.fields.max.depth", "-1").toInt

    // Validate depth range (Spark enforces -1 or 1-10, we add 0 for unlimited)
    if (depth < -1 || depth > 10) {
      throw new IllegalArgumentException(
        s"Invalid value for 'recursive.fields.max.depth': $depth. " +
        "Supported values are -1 (forbid), 0 (unlimited), or 1-10 (specific depth limit). " +
        "Values > 10 can cause stack overflows.")
    }

    depth
  }

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
   * - "recursive": Use RecursiveStructType (only valid with depth=0 or depth=-1)
   *
   * Precedence rules:
   * - depth=-1: mode="drop"/"binary" override Spark default (forbid)
   * - depth=0: Defaults to "recursive" (unlimited)
   * - depth>=1: Defaults to "drop", allow_recursion ignored
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

    // Validate illegal combination: recursive + depth>=1
    // (RecursiveStructType requires unlimited depth, depth=0 or depth=-1)
    if (mode == "recursive" && recursiveFieldMaxDepth >= 1) {
      throw new IllegalArgumentException(
        "Invalid configuration: 'recursive.fields.mode=recursive' cannot be used with " +
        s"'recursive.fields.max.depth=${recursiveFieldMaxDepth}'. " +
        "RecursiveStructType requires unlimited depth (depth=0 or depth=-1 with explicit mode).")
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
