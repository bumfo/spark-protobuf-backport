/*
 * Backport of Spark 3.4's Protobuf functions to Spark 3.2.1.
 *
 * These helper methods can be used in the DataFrame API to convert
 * between binary Protobuf data and Catalyst types.  They mirror the
 * functions shipped with Spark 3.4 under `org.apache.spark.sql.protobuf`.
 */

package org.apache.spark.sql.protobuf.backport

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Column

// scalastyle:off: object.name
object functions {
// scalastyle:on: object.name

  /**
   * Converts a binary column of Protobuf format into its corresponding Catalyst
   * value.  The Protobuf definition is provided through a descriptor file.
   *
   * @param data         the binary column
   * @param messageName  the Protobuf message name to look for in the descriptor file
   * @param descFilePath the Protobuf descriptor file path
   * @param options      extra reader options (case‑insensitive)
   */
  @Experimental
  def from_protobuf(
      data: Column,
      messageName: String,
      descFilePath: String,
      options: java.util.Map[String, String]): Column = {
    new Column(
      ProtobufDataToCatalyst(data.expr, messageName, Some(descFilePath), options.asScala.toMap)
    )
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding Catalyst
   * value.  The Protobuf definition is provided through a descriptor file.
   *
   * @param data         the binary column
   * @param messageName  the Protobuf message name to look for in the descriptor file
   * @param descFilePath the Protobuf descriptor file path
   */
  @Experimental
  def from_protobuf(data: Column, messageName: String, descFilePath: String): Column = {
    new Column(ProtobufDataToCatalyst(data.expr, messageName, descFilePath = Some(descFilePath)))
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding Catalyst
   * value.  The Protobuf class is provided by its fully qualified Java class
   * name.  The containing jar must shade `com.google.protobuf` to
   * `org.sparkproject.spark.protobuf311`.
   *
   * @param data             the binary column
   * @param messageClassName the fully qualified Protobuf Java class name
   */
  @Experimental
  def from_protobuf(data: Column, messageClassName: String): Column = {
    new Column(ProtobufDataToCatalyst(data.expr, messageClassName))
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding Catalyst
   * value.  The Protobuf class is provided by its fully qualified Java class
   * name.  The containing jar must shade `com.google.protobuf` to
   * `org.sparkproject.spark.protobuf311`.
   *
   * @param data             the binary column
   * @param messageClassName the fully qualified Protobuf Java class name
   * @param options          extra reader options (case‑insensitive)
   */
  @Experimental
  def from_protobuf(
      data: Column,
      messageClassName: String,
      options: java.util.Map[String, String]): Column = {
    new Column(ProtobufDataToCatalyst(data.expr, messageClassName, None, options.asScala.toMap))
  }

  /**
   * Converts a column into a binary column of Protobuf format.  The Protobuf
   * definition is provided through a descriptor file.
   *
   * @param data         the data column
   * @param messageName  the Protobuf message name
   * @param descFilePath the Protobuf descriptor file
   */
  @Experimental
  def to_protobuf(data: Column, messageName: String, descFilePath: String): Column = {
    new Column(CatalystDataToProtobuf(data.expr, messageName, Some(descFilePath)))
  }

  /**
   * Converts a column into a binary column of Protobuf format.  The Protobuf
   * definition is provided through a descriptor file.
   *
   * @param data         the data column
   * @param messageName  the Protobuf message name
   * @param descFilePath the Protobuf descriptor file
   * @param options      extra writer options (currently unused)
   */
  @Experimental
  def to_protobuf(
      data: Column,
      messageName: String,
      descFilePath: String,
      options: java.util.Map[String, String]): Column = {
    new Column(
      CatalystDataToProtobuf(data.expr, messageName, Some(descFilePath), options.asScala.toMap)
    )
  }

  /**
   * Converts a column into a binary column of Protobuf format.  The Protobuf
   * definition is provided by a shaded Java class name.
   *
   * @param data             the data column
   * @param messageClassName the fully qualified Protobuf Java class name
   */
  @Experimental
  def to_protobuf(data: Column, messageClassName: String): Column = {
    new Column(CatalystDataToProtobuf(data.expr, messageClassName))
  }

  /**
   * Converts a column into a binary column of Protobuf format.  The Protobuf
   * definition is provided by a shaded Java class name.
   *
   * @param data             the data column
   * @param messageClassName the fully qualified Protobuf Java class name
   * @param options          extra writer options (currently unused)
   */
  @Experimental
  def to_protobuf(
      data: Column,
      messageClassName: String,
      options: java.util.Map[String, String]): Column = {
    new Column(CatalystDataToProtobuf(data.expr, messageClassName, None, options.asScala.toMap))
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding Catalyst
   * value using a binary descriptor set rather than a descriptor file.  This is
   * useful when the descriptor file exists only on the driver and cannot be
   * accessed from executors.  The binary descriptor set can be obtained by
   * reading the `.desc` file generated by protoc and passed as a byte array.
   *
   * @param data             the binary column
   * @param messageName      the Protobuf message name to look for in the descriptor set
   * @param binaryDescriptor the serialized FileDescriptorSet bytes
   */
  @Experimental
  def from_protobuf(data: Column, messageName: String, binaryDescriptor: Array[Byte]): Column = {
    new Column(ProtobufDataToCatalyst(data.expr, messageName, None, Map.empty, Some(binaryDescriptor)))
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding Catalyst
   * value using a binary descriptor set with additional options.
   *
   * @param data             the binary column
   * @param messageName      the Protobuf message name to look for in the descriptor set
   * @param binaryDescriptor the serialized FileDescriptorSet bytes
   * @param options          extra reader options (case‑insensitive)
   */
  @Experimental
  def from_protobuf(
      data: Column,
      messageName: String,
      binaryDescriptor: Array[Byte],
      options: java.util.Map[String, String]
  ): Column = {
    new Column(ProtobufDataToCatalyst(data.expr, messageName, None, options.asScala.toMap, Some(binaryDescriptor)))
  }

  /**
   * Converts a column into a binary column of Protobuf format using a binary
   * descriptor set rather than a descriptor file.  This is useful when the
   * descriptor file exists only on the driver and cannot be accessed from
   * executors.  The binary descriptor set can be obtained by reading the
   * `.desc` file generated by protoc and passed as a byte array.
   *
   * @param data             the data column
   * @param messageName      the Protobuf message name
   * @param binaryDescriptor the serialized FileDescriptorSet bytes
   */
  @Experimental
  def to_protobuf(data: Column, messageName: String, binaryDescriptor: Array[Byte]): Column = {
    new Column(CatalystDataToProtobuf(data.expr, messageName, None, Map.empty, Some(binaryDescriptor)))
  }

  /**
   * Converts a column into a binary column of Protobuf format using a binary
   * descriptor set with additional options.
   *
   * @param data             the data column
   * @param messageName      the Protobuf message name
   * @param binaryDescriptor the serialized FileDescriptorSet bytes
   * @param options          extra writer options (currently unused)
   */
  @Experimental
  def to_protobuf(
      data: Column,
      messageName: String,
      binaryDescriptor: Array[Byte],
      options: java.util.Map[String, String]
  ): Column = {
    new Column(CatalystDataToProtobuf(data.expr, messageName, None, options.asScala.toMap, Some(binaryDescriptor)))
  }
}