package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Unified schema converter that handles recursive message types with full config support.
 *
 * Supports all recursive field handling modes and depth limiting:
 * - recursive.fields.mode: "struct", "binary", or "drop"
 * - recursive.fields.max.depth: Maximum nesting depth for messages
 *
 * Standard SchemaConverters.toSqlType() fails on recursive protobuf messages because
 * Spark SQL cannot represent infinite recursive types. This converter provides multiple
 * strategies for handling recursion based on configuration.
 */
object RecursiveSchemaConverters {

  /** Helper to get supported fields from descriptor (excludes deprecated GROUP fields) */
  private def supportedFields(descriptor: Descriptor) =
    descriptor.getFields.asScala.filter(_.getType != FieldDescriptor.Type.GROUP)

  /**
   * Convert protobuf descriptor to Spark SQL type respecting all configuration options.
   *
   * This is the unified entry point that handles all recursive field configurations:
   * - recursive.fields.mode: "struct", "binary", or "drop"
   * - recursive.fields.max.depth: Maximum recursion depth (-1 for unlimited, 0+ for limit)
   *
   * Mode behavior:
   * - "struct" + maxDepth=-1: RecursiveStructType with true circular references (unlimited)
   * - "struct" + maxDepth>=0: Regular StructType with depth limit (drops fields beyond depth)
   * - "binary": Mock recursive fields as BinaryType (maxDepth ignored)
   * - "drop": Omit recursive fields entirely (maxDepth ignored)
   *
   * @param descriptor the protobuf message descriptor
   * @param recursiveFieldsMode How to handle recursive fields ("struct", "binary", "drop")
   * @param recursiveFieldMaxDepth Maximum recursion depth (-1 for unlimited, 0+ for limit)
   * @param enumAsInt if true, represent enum fields as IntegerType instead of StringType
   * @return Spark SQL DataType with recursive fields handled according to config
   */
  def toSqlType(
      descriptor: Descriptor,
      recursiveFieldsMode: String,
      recursiveFieldMaxDepth: Int,
      enumAsInt: Boolean = false): DataType = {

    recursiveFieldsMode match {
      case "struct" =>
        if (recursiveFieldMaxDepth == -1) {
          // Unlimited recursion - use RecursiveStructType
          toSqlTypeWithTrueRecursion(descriptor, enumAsInt)
        } else {
          // Limited recursion depth - convert with depth limit
          toSqlTypeWithDepthLimit(descriptor, enumAsInt, recursiveFieldMaxDepth)
        }

      case "binary" =>
        // Mock recursive fields as BinaryType (depth is ignored)
        toSqlTypeWithRecursionMocking(descriptor, enumAsInt)

      case "drop" =>
        // Drop recursive fields entirely (depth is ignored)
        toSqlTypeWithRecursionDropping(descriptor, enumAsInt)
    }
  }

  /**
   * Convert protobuf descriptor to Spark SQL type with recursion detection and mocking.
   *
   * When a recursive field is detected (e.g., DomNode.children referring back to DomNode),
   * it's replaced with BinaryType (or ArrayType(BinaryType) for repeated fields).
   * This allows the schema to be finite while still testing recursive parsing performance.
   *
   * @param descriptor the protobuf message descriptor
   * @param enumAsInt if true, represent enum fields as IntegerType instead of StringType
   * @return Spark SQL StructType with recursive fields mocked
   */
  def toSqlTypeWithRecursionMocking(descriptor: Descriptor, enumAsInt: Boolean = false): DataType = {
    val visitedTypes = mutable.Set[String]()
    convertMessageType(descriptor, visitedTypes, enumAsInt, dropRecursive = false)
  }

  /**
   * Convert protobuf descriptor to Spark SQL type with recursive fields dropped.
   *
   * When a recursive field is detected, it's completely omitted from the schema.
   * This produces the most compact schema but loses data from recursive fields.
   *
   * @param descriptor the protobuf message descriptor
   * @param enumAsInt if true, represent enum fields as IntegerType instead of StringType
   * @return Spark SQL StructType with recursive fields dropped
   */
  def toSqlTypeWithRecursionDropping(descriptor: Descriptor, enumAsInt: Boolean = false): DataType = {
    val visitedTypes = mutable.Set[String]()
    convertMessageType(descriptor, visitedTypes, enumAsInt, dropRecursive = true)
  }

  /**
   * Convert protobuf descriptor to Spark SQL type with depth limit.
   *
   * Creates a regular StructType (not RecursiveStructType) that drops fields beyond maxDepth.
   * This allows controlled nesting without circular references.
   *
   * @param descriptor the protobuf message descriptor
   * @param enumAsInt if true, represent enum fields as IntegerType instead of StringType
   * @param maxDepth Maximum nesting depth (0 = no nested messages, 1 = one level, etc.)
   * @return Regular StructType with fields beyond maxDepth dropped
   */
  private def toSqlTypeWithDepthLimit(
      descriptor: Descriptor,
      enumAsInt: Boolean,
      maxDepth: Int): StructType = {

    require(maxDepth >= 0, s"maxDepth must be >= 0, got $maxDepth")

    val visitedTypes = mutable.Set[String]()
    convertMessageTypeWithDepth(descriptor, visitedTypes, enumAsInt, currentDepth = 0, maxDepth)
  }

  /**
   * Convert a message type to StructType with depth tracking.
   */
  private def convertMessageTypeWithDepth(
      descriptor: Descriptor,
      visitedTypes: mutable.Set[String],
      enumAsInt: Boolean,
      currentDepth: Int,
      maxDepth: Int): StructType = {

    val typeName = descriptor.getFullName
    val wasAlreadyVisited = visitedTypes.contains(typeName)
    visitedTypes += typeName

    val fields = supportedFields(descriptor).flatMap { field =>
      convertFieldTypeWithDepth(field, visitedTypes, wasAlreadyVisited, enumAsInt, currentDepth, maxDepth) match {
        case None => None
        case Some(sparkType) => Some(StructField(field.getName, sparkType, nullable = true))
      }
    }.toSeq

    if (!wasAlreadyVisited) {
      visitedTypes -= typeName
    }

    StructType(fields)
  }

  /**
   * Convert a single field type with depth tracking.
   * Returns None if the field should be dropped due to depth limit or recursion.
   */
  private def convertFieldTypeWithDepth(
      field: FieldDescriptor,
      visitedTypes: mutable.Set[String],
      parentWasVisited: Boolean,
      enumAsInt: Boolean,
      currentDepth: Int,
      maxDepth: Int): Option[DataType] = {

    require(field.getType != FieldDescriptor.Type.GROUP, "GROUP fields are not supported")

    val baseTypeOpt = field.getJavaType match {
      case FieldDescriptor.JavaType.MESSAGE =>
        val messageDescriptor = field.getMessageType

        // For depth limiting, check depth first regardless of recursion.
        // The depth limit alone is sufficient to prevent infinite recursion,
        // so we don't need the visitedTypes check here.
        if (currentDepth >= maxDepth) {
          // At or beyond max depth - drop nested messages
          None
        } else {
          // Within depth limit - continue
          Some(convertMessageTypeWithDepth(
            messageDescriptor,
            visitedTypes.clone(),
            enumAsInt,
            currentDepth + 1,
            maxDepth))
        }

      case FieldDescriptor.JavaType.INT => Some(IntegerType)
      case FieldDescriptor.JavaType.LONG => Some(LongType)
      case FieldDescriptor.JavaType.FLOAT => Some(FloatType)
      case FieldDescriptor.JavaType.DOUBLE => Some(DoubleType)
      case FieldDescriptor.JavaType.BOOLEAN => Some(BooleanType)
      case FieldDescriptor.JavaType.STRING => Some(StringType)
      case FieldDescriptor.JavaType.BYTE_STRING => Some(BinaryType)
      case FieldDescriptor.JavaType.ENUM => Some(if (enumAsInt) IntegerType else StringType)
    }

    // Handle repeated fields
    baseTypeOpt.flatMap { baseType =>
      if (field.isRepeated) {
        if (field.isMapField) {
          val mapEntryDescriptor = field.getMessageType
          val keyField = mapEntryDescriptor.findFieldByName("key")
          val valueField = mapEntryDescriptor.findFieldByName("value")

          val keyTypeOpt = convertFieldTypeWithDepth(keyField, visitedTypes, parentWasVisited, enumAsInt, currentDepth, maxDepth)
          val valueTypeOpt = convertFieldTypeWithDepth(valueField, visitedTypes, parentWasVisited, enumAsInt, currentDepth, maxDepth)

          // Only create map entry if both key and value are available
          for {
            keyType <- keyTypeOpt
            valueType <- valueTypeOpt
          } yield {
            val mapEntryStruct = StructType(Seq(
              StructField("key", keyType, nullable = false),
              StructField("value", valueType, nullable = true)
            ))
            ArrayType(mapEntryStruct)
          }
        } else {
          Some(ArrayType(baseType))
        }
      } else {
        Some(baseType)
      }
    }
  }

  /**
   * Convert protobuf descriptor to Spark SQL type with TRUE recursion support.
   *
   * This creates actual recursive schemas where fields can reference back to their parent types.
   * Uses RecursiveStructType which can handle circular references in hashCode and string methods.
   *
   * @param descriptor the protobuf message descriptor
   * @param enumAsInt if true, represent enum fields as IntegerType instead of StringType
   * @return RecursiveStructType with true recursive references
   */
  def toSqlTypeWithTrueRecursion(descriptor: Descriptor, enumAsInt: Boolean = false): RecursiveStructType = {
    val schemaMap = mutable.Map[String, RecursiveStructType]()

    // Pass 1: Create all StructTypes with placeholder fields for recursion
    val rootSchema = createSchemaWithPlaceholders(descriptor, schemaMap, enumAsInt)

    // Pass 2: Patch recursive fields to create true recursion
    patchRecursiveReferences(descriptor, rootSchema, schemaMap)

    rootSchema
  }

  /**
   * Convert a message type to StructType, tracking visited types to detect recursion.
   */
  private def convertMessageType(
      descriptor: Descriptor,
      visitedTypes: mutable.Set[String],
      enumAsInt: Boolean = false,
      dropRecursive: Boolean = false): StructType = {

    // Mark this type as being visited to detect cycles
    val typeName = descriptor.getFullName
    val wasAlreadyVisited = visitedTypes.contains(typeName)
    visitedTypes += typeName

    val fields = supportedFields(descriptor).flatMap { field =>
      convertFieldType(field, visitedTypes, wasAlreadyVisited, enumAsInt, dropRecursive) match {
        case None => None  // Field dropped due to recursion
        case Some(sparkType) => Some(StructField(field.getName, sparkType, nullable = true))
      }
    }.toSeq

    // Clean up: remove from visited set when done (for clean recursion detection)
    if (!wasAlreadyVisited) {
      visitedTypes -= typeName
    }

    StructType(fields)
  }

  /**
   * Convert a single field type, handling recursion detection.
   * Returns None if the field should be dropped due to recursion.
   */
  private def convertFieldType(
      field: FieldDescriptor,
      visitedTypes: mutable.Set[String],
      parentWasVisited: Boolean,
      enumAsInt: Boolean = false,
      dropRecursive: Boolean = false): Option[DataType] = {
    // Filter out deprecated GROUP fields - should not happen but defensive check
    require(field.getType != FieldDescriptor.Type.GROUP, "GROUP fields are not supported")

    val baseTypeOpt = field.getJavaType match {
      case FieldDescriptor.JavaType.MESSAGE =>
        val messageDescriptor = field.getMessageType
        val messageTypeName = messageDescriptor.getFullName

        // Check for recursion: if we're already processing this type, mock it or drop it
        if (visitedTypes.contains(messageTypeName)) {
          // RECURSION DETECTED!
          if (dropRecursive) {
            None  // Drop the field
          } else {
            Some(BinaryType)  // Mock with BinaryType
          }
        } else {
          // Not recursive yet, continue normal conversion
          Some(convertMessageType(messageDescriptor, visitedTypes.clone(), enumAsInt, dropRecursive))
        }

      case FieldDescriptor.JavaType.INT =>
        Some(IntegerType)
      case FieldDescriptor.JavaType.LONG =>
        Some(LongType)
      case FieldDescriptor.JavaType.FLOAT =>
        Some(FloatType)
      case FieldDescriptor.JavaType.DOUBLE =>
        Some(DoubleType)
      case FieldDescriptor.JavaType.BOOLEAN =>
        Some(BooleanType)
      case FieldDescriptor.JavaType.STRING =>
        Some(StringType)
      case FieldDescriptor.JavaType.BYTE_STRING =>
        Some(BinaryType)
      case FieldDescriptor.JavaType.ENUM =>
        Some(if (enumAsInt) IntegerType else StringType)
    }

    // Handle repeated fields
    baseTypeOpt.flatMap { baseType =>
      if (field.isRepeated) {
        // Handle map fields - for compatibility with WireFormatParser,
        // treat maps as ArrayType[StructType] rather than MapType
        if (field.isMapField) {
          val mapEntryDescriptor = field.getMessageType
          val keyField = mapEntryDescriptor.findFieldByName("key")
          val valueField = mapEntryDescriptor.findFieldByName("value")

          val keyTypeOpt = convertFieldType(keyField, visitedTypes, parentWasVisited, enumAsInt, dropRecursive)
          val valueTypeOpt = convertFieldType(valueField, visitedTypes, parentWasVisited, enumAsInt, dropRecursive)

          // Only create map entry if both key and value are available
          for {
            keyType <- keyTypeOpt
            valueType <- valueTypeOpt
          } yield {
            val mapEntryStruct = StructType(Seq(
              StructField("key", keyType, nullable = false),
              StructField("value", valueType, nullable = true)
            ))
            ArrayType(mapEntryStruct)
          }
        } else {
          Some(ArrayType(baseType))
        }
      } else {
        Some(baseType)
      }
    }
  }

  /**
   * Get information about which fields were mocked due to recursion.
   * Useful for debugging and understanding schema transformations.
   */
  def getMockedFields(descriptor: Descriptor): Set[String] = {
    val visitedTypes = mutable.Set[String]()
    val mockedFields = mutable.Set[String]()
    collectMockedFields(descriptor, visitedTypes, mockedFields, "")
    mockedFields.toSet
  }

  private def collectMockedFields(
      descriptor: Descriptor,
      visitedTypes: mutable.Set[String],
      mockedFields: mutable.Set[String],
      pathPrefix: String): Unit = {

    val typeName = descriptor.getFullName
    visitedTypes += typeName

    supportedFields(descriptor).foreach { field =>
      val fieldPath = if (pathPrefix.isEmpty) field.getName else s"$pathPrefix.${field.getName}"

      if (field.getJavaType == FieldDescriptor.JavaType.MESSAGE) {
        val messageDescriptor = field.getMessageType
        val messageTypeName = messageDescriptor.getFullName

        if (visitedTypes.contains(messageTypeName)) {
          mockedFields += fieldPath
        } else {
          collectMockedFields(messageDescriptor, visitedTypes.clone(), mockedFields, fieldPath)
        }
      }
    }

    visitedTypes -= typeName
  }

  /**
   * Print schema information for debugging.
   */
  def printSchemaInfo(descriptor: Descriptor): Unit = {
    val schema = toSqlTypeWithRecursionMocking(descriptor)
    val mockedFields = getMockedFields(descriptor)

    println(s"Schema for ${descriptor.getFullName}:")
    println(schema.prettyJson)
    println(s"\nMocked recursive fields: ${mockedFields.mkString(", ")}")
  }

  // ========== True Recursion Support Methods ==========

  /**
   * Create RecursiveStructType with placeholder fields for recursive references.
   * This is Pass 1 of the two-pass algorithm.
   */
  private def createSchemaWithPlaceholders(
      descriptor: Descriptor,
      schemaMap: mutable.Map[String, RecursiveStructType],
      enumAsInt: Boolean = false): RecursiveStructType = {

    val typeName = descriptor.getFullName

    // Check if we already have a schema for this type
    schemaMap.get(typeName) match {
      case Some(existingSchema) => existingSchema
      case None =>
        // Create placeholder schema first to handle immediate recursion
        val placeholderFields = supportedFields(descriptor).map { field =>
          StructField(field.getName, NullType, nullable = true)
        }.toArray

        val schema = new RecursiveStructType(placeholderFields, typeName)
        schemaMap(typeName) = schema

        // Now populate the fields properly
        supportedFields(descriptor).zipWithIndex.foreach { case (field, index) =>
          val fieldType = convertFieldTypeForTrueRecursion(field, schemaMap, enumAsInt)
          placeholderFields(index) = StructField(field.getName, fieldType, nullable = true)
        }

        schema
    }
  }

  /**
   * Convert a single field type for true recursion support.
   * This handles MESSAGE types by creating recursive references.
   */
  private def convertFieldTypeForTrueRecursion(
      field: FieldDescriptor,
      schemaMap: mutable.Map[String, RecursiveStructType],
      enumAsInt: Boolean = false): DataType = {
    // Filter out deprecated GROUP fields - should not happen but defensive check
    require(field.getType != FieldDescriptor.Type.GROUP, "GROUP fields are not supported")

    val baseType = field.getJavaType match {
      case FieldDescriptor.JavaType.MESSAGE =>
        val messageDescriptor = field.getMessageType
        // Recursively create schema (will use existing one if already created)
        createSchemaWithPlaceholders(messageDescriptor, schemaMap, enumAsInt)

      case FieldDescriptor.JavaType.INT =>
        IntegerType
      case FieldDescriptor.JavaType.LONG =>
        LongType
      case FieldDescriptor.JavaType.FLOAT =>
        FloatType
      case FieldDescriptor.JavaType.DOUBLE =>
        DoubleType
      case FieldDescriptor.JavaType.BOOLEAN =>
        BooleanType
      case FieldDescriptor.JavaType.STRING =>
        StringType
      case FieldDescriptor.JavaType.BYTE_STRING =>
        BinaryType
      case FieldDescriptor.JavaType.ENUM =>
        if (enumAsInt) IntegerType else StringType
    }

    // Handle repeated fields and maps
    if (field.isRepeated) {
      if (field.isMapField) {
        val mapEntryDescriptor = field.getMessageType
        val keyField = mapEntryDescriptor.findFieldByName("key")
        val valueField = mapEntryDescriptor.findFieldByName("value")

        val keyType = convertFieldTypeForTrueRecursion(keyField, schemaMap, enumAsInt)
        val valueType = convertFieldTypeForTrueRecursion(valueField, schemaMap, enumAsInt)

        // Create map entry struct for compatibility with parsers
        val mapEntryStruct = StructType(Seq(
          StructField("key", keyType, nullable = false),
          StructField("value", valueType, nullable = true)
        ))
        ArrayType(mapEntryStruct)
      } else {
        ArrayType(baseType)
      }
    } else {
      baseType
    }
  }

  /**
   * Pass 2: Patch recursive references to create true recursion.
   * This method is intentionally minimal since Pass 1 already handles most cases correctly.
   */
  private def patchRecursiveReferences(
      descriptor: Descriptor,
      schema: RecursiveStructType,
      schemaMap: mutable.Map[String, RecursiveStructType]): Unit = {

    // Since Pass 1 creates schemas recursively, most recursive references are already correct.
    // We only need to patch cases where we had to break cycles during creation.
    // For the DOM case, the DomNode.children field should already point to the correct DomNode schema.

    // Optional: Add validation to ensure recursion is working correctly
    supportedFields(descriptor).foreach { field =>
      if (field.getJavaType == FieldDescriptor.JavaType.MESSAGE) {
        val fieldIndex = schema.fieldIndex(field.getName)
        val sparkField = schema.fields(fieldIndex)

        sparkField.dataType match {
          case struct: StructType =>
            // Single message field - should reference correct schema
          case ArrayType(struct: StructType, _) =>
            // Repeated message field - should reference correct schema
          case other =>
            // This should not happen in true recursion mode
            println(s"Warning: Field ${field.getName} has unexpected type $other")
        }
      }
    }
  }

  /**
   * Print true recursive schema information for debugging.
   */
  def printTrueRecursiveSchemaInfo(descriptor: Descriptor): Unit = {
    val schema = toSqlTypeWithTrueRecursion(descriptor)

    println(s"True Recursive Schema for ${descriptor.getFullName}:")
    println(s"Root schema fields: ${schema.fieldNames.mkString(", ")}")

    // Print field types to show recursion
    schema.fields.foreach { field =>
      field.dataType match {
        case struct: StructType =>
          println(s"Field '${field.name}': StructType with ${struct.fields.length} fields")
        case ArrayType(struct: StructType, _) =>
          println(s"Field '${field.name}': Array[StructType] with ${struct.fields.length} fields")
        case other =>
          println(s"Field '${field.name}': ${other.getClass.getSimpleName}")
      }
    }
  }
}