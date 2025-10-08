/*
 * Schema utility methods for the Protobuf backport connector.
 *
 * Provides helper functions for schema pruning and field reference analysis.
 */

package org.apache.spark.sql.protobuf.backport.utils

import org.apache.spark.sql.types._

/**
 * Utilities for analyzing and pruning Spark SQL schemas in the context of
 * protobuf deserialization.  These methods support nested schema pruning
 * by identifying required fields and building minimal schemas.
 */
private[backport] object SchemaUtils {

  /**
   * Prune a schema to contain only the specified required fields and their
   * parents.  For nested field references like "person.name", this ensures
   * that the "person" struct is included but contains only the "name" field.
   *
   * @param fullSchema    the complete schema to prune
   * @param requiredPaths the set of field paths to retain (e.g., Set("a.b.c", "d"))
   * @return a minimal schema containing only the required fields
   */
  def pruneSchema(fullSchema: StructType, requiredPaths: Set[Seq[String]]): StructType = {
    if (requiredPaths.isEmpty) {
      // No fields required, return empty schema
      StructType(Seq.empty)
    } else {
      pruneStructType(fullSchema, requiredPaths, Seq.empty)
    }
  }

  /**
   * Recursively prune a StructType based on required field paths.
   *
   * @param schema        the struct type to prune
   * @param requiredPaths the set of required field paths (absolute from root)
   * @param currentPath   the current path context (for recursive calls)
   * @return pruned StructType containing only required fields
   */
  private def pruneStructType(
      schema: StructType,
      requiredPaths: Set[Seq[String]],
      currentPath: Seq[String]): StructType = {

    val prunedFields = schema.fields.flatMap { field =>
      val fieldPath = currentPath :+ field.name

      // Check if this field or any of its descendants are required
      val isRequired = requiredPaths.exists { reqPath =>
        // Field matches if required path starts with current field path
        reqPath.startsWith(fieldPath) || fieldPath.startsWith(reqPath)
      }

      if (isRequired) {
        // Recursively prune nested structs
        val prunedDataType = field.dataType match {
          case structType: StructType =>
            pruneStructType(structType, requiredPaths, fieldPath)
          case ArrayType(structType: StructType, containsNull) =>
            ArrayType(pruneStructType(structType, requiredPaths, fieldPath), containsNull)
          case MapType(keyType, structType: StructType, valueContainsNull) =>
            MapType(keyType, pruneStructType(structType, requiredPaths, fieldPath), valueContainsNull)
          case other => other
        }
        Some(field.copy(dataType = prunedDataType))
      } else {
        None
      }
    }

    StructType(prunedFields)
  }

  /**
   * Extract required field paths from a schema based on which fields would
   * actually be accessed. This is a simplified version that assumes all
   * fields in the provided schema are required.
   *
   * @param schema the schema to analyze
   * @return set of field paths (as sequences of field names)
   */
  def getRequiredPaths(schema: StructType): Set[Seq[String]] = {
    extractPaths(schema, Seq.empty)
  }

  private def extractPaths(dataType: DataType, currentPath: Seq[String]): Set[Seq[String]] = {
    dataType match {
      case structType: StructType =>
        structType.fields.flatMap { field =>
          val fieldPath = currentPath :+ field.name
          Set(fieldPath) ++ extractPaths(field.dataType, fieldPath)
        }.toSet
      case ArrayType(elementType, _) =>
        extractPaths(elementType, currentPath)
      case MapType(_, valueType, _) =>
        extractPaths(valueType, currentPath)
      case _ =>
        Set(currentPath)
    }
  }
}
