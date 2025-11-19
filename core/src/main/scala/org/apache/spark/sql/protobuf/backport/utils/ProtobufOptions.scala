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

import fastproto.RecursionMode
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
 *    Values:
 *    - "-1": Default behavior (recursive for WireFormat parser, fail for others)
 *    - "0": No recursive fields allowed (drop on first recursion)
 *    - "1": Allow 1 recursion (drop when depth > 1)
 *    - "2": Allow 2 recursions (drop when depth > 2)
 *    - "3-10": Allow N recursions (drop when depth > N)
 *
 * Configuration Precedence:
 *
 * When depth=-1 (default):
 * - mode="" → recursive if allow_recursion=true, fail if allow_recursion=false
 * - mode="fail"/"drop"/"binary"/"recursive" → use specified mode (explicit override)
 * - allow_recursion → used when mode="" (parser default: true for WireFormat, false for others)
 *
 * When depth=0 (no recursions):
 * - mode="" → drop (default for depth=0)
 * - mode="drop"/"binary"/"fail" → use specified mode
 * - mode="recursive" → ERROR (no recursions means no RecursiveStructType)
 * - allow_recursion → IGNORED (depth takes precedence)
 *
 * When depth>=1 (depth limit):
 * - mode="" → drop (default for depth-limited)
 * - mode="drop"/"binary"/"fail" → use specified mode
 * - mode="recursive" → ERROR (depth limit incompatible with RecursiveStructType)
 * - allow_recursion → IGNORED (depth takes precedence)
 *
 * Depth Semantics:
 * - depth=N: Allow N recursions (depth values 1 through N)
 * - First recursion is assigned depth=1
 * - Drop when depth > maxDepth
 * - Example with depth=3: allows recursions at depth 1, 2, and 3, drops at depth 4
 *
 * IMPORTANT: Depth increments for ALL nested message fields visited inside a cycle,
 * not just recursive edges. Non-recursive types visited while in a cycle also increment depth.
 *
 * Internal Implementation:
 * - User depth N maps to internal maxRecursiveDepth N
 * - First recursion: recursiveDepth=0 → depth=0+1=1
 * - Depth increments for EVERY message field visited while recursiveDepth > 0
 * - Example A→B→A→C with depth=2: depths 0, 0, 1, 2
 *   (A root=0, B first=0, A recursion=1, C inside cycle=2 > 2, dropped)
 *
 * Semantic Difference from Spark SQL:
 *
 * Our implementation uses a single depth counter that increments for ALL message fields
 * visited after entering a recursive cycle. The limit applies globally across all types.
 *
 * Example - Mutual recursion A↔B with depth=3:
 * - Our result: A→B→A→B→A(primitives only, recursive fields dropped)
 * - Path depths: A(0) → B(0) → A(1) → B(2) → A(3) → B(4>3, dropped)
 * - depth=3 allows recursions at depths 1, 2, and 3 (total 3 recursions across all types)
 *
 * Spark SQL tracks occurrences per message type independently:
 * - Spark depth=3 for type A: allows A to appear 3 times total
 * - Spark depth=3 for type B: allows B to appear 3 times total
 * - Each type has its own counter
 *
 * With Spark depth=3 and A↔B mutual recursion:
 * - Spark result: A→B→A→B→A→B (allows 3 A's and 3 B's)
 * - Our result: A→B→A→B→A (allows 3 recursions total)
 *
 * Our approach limits total recursion depth to prevent stack overflow,
 * while Spark's per-type counting allows deeper nesting in mutual recursion.
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
   * Values:
   * - "-1": Forbid recursive fields (Spark 3.4+ default, throws error on recursion)
   * - "0": Unlimited recursion (enables RecursiveStructType, our extension)
   * - "1": Allow 1 recursion (drop when depth > 1)
   * - "2": Allow 2 recursions (drop when depth > 2)
   * - "3-10": Allow N recursions (drop when depth > N)
   *
   * Validation: Must be in range [-1, 10]. Values > 10 can cause stack overflows.
   *
   * Depth counting:
   * - First recursion assigned depth=1
   * - Counts total recursions across all message types in a cycle
   * - Different from Spark's per-type occurrence counting
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
   * How to handle recursive message types in schema conversion (raw string from config).
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
   * Applies to: All parsers via RecursiveSchemaConverters
   */
  val recursiveFieldsMode: String = parameters.getOrElse("recursive.fields.mode", "")

  /**
   * Parsed RecursionMode enum for internal use.
   * Parse the string at config boundary and validate combinations.
   */
  val recursiveFieldsModeEnum: RecursionMode = {
    // Parse mode string to enum (validation happens in fromString)
    val parsedMode = RecursionMode.fromString(recursiveFieldsMode)

    // Validate illegal combinations: recursive mode + depth >= 0
    // (RecursiveStructType requires depth=-1 with explicit mode, not depth limits)
    if (parsedMode == RecursionMode.AllowRecursive && recursiveFieldMaxDepth >= 0) {
      throw new IllegalArgumentException(
        "Invalid configuration: 'recursive.fields.mode=recursive' cannot be used with " +
        s"'recursive.fields.max.depth=${recursiveFieldMaxDepth}'. " +
        "RecursiveStructType requires depth=-1 (default) with explicit mode=recursive.")
    }

    parsedMode
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
