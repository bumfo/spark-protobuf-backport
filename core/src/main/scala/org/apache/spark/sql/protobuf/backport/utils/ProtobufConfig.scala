/*
 * Configuration support for the Protobuf backport connector.
 *
 * Provides access to Protobuf-specific configuration options via Spark's
 * SQLConf. Configuration keys follow Spark's naming convention and can be
 * set using spark.conf.set() or SparkConf.
 */

package org.apache.spark.sql.protobuf.backport.utils

import org.apache.spark.sql.internal.SQLConf

/**
 * Configuration keys and accessors for the Protobuf connector.  These
 * settings can be configured at the Spark session level using
 * spark.conf.set("spark.sql.protobuf.<key>", value).
 */
private[backport] object ProtobufConfig {

  /**
   * Configuration key for enabling nested schema pruning in protobuf
   * deserialization.  When enabled, the connector will only parse fields
   * that are actually accessed in the query, potentially improving
   * performance for wide schemas.
   *
   * This optimization currently applies only to expressions using the
   * WireFormat parser (binary descriptor set usage).
   *
   * Default: true
   */
  val NESTED_SCHEMA_PRUNING_ENABLED = "spark.sql.protobuf.nestedSchemaPruning.enabled"

  /**
   * Check whether nested schema pruning is enabled for protobuf
   * deserialization.  Reads from the current SQLConf and returns the
   * configured value, defaulting to true if not set.
   *
   * @return true if nested schema pruning is enabled, false otherwise
   */
  def nestedSchemaPruningEnabled: Boolean = {
    SQLConf.get.getConfString(NESTED_SCHEMA_PRUNING_ENABLED, "true").toBoolean
  }
}
