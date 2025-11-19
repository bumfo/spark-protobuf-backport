package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Strategy for handling recursive message types during schema conversion.
 */
private[fastproto] sealed trait RecursionMode extends Serializable

private[fastproto] object RecursionMode {
  /** Throw exception when recursion is detected */
  case object Fail extends RecursionMode

  /** Drop recursive fields from schema entirely */
  case object Drop extends RecursionMode

  /** Replace recursive fields with BinaryType */
  case object MockAsBinary extends RecursionMode

  /** Allow recursive types using RecursiveStructType (unlimited depth) */
  case object AllowRecursive extends RecursionMode
}

/**
 * Unified schema converter that handles recursive message types with full config support.
 *
 * Supports all recursive field handling modes and depth limiting:
 * - recursive.fields.mode: "recursive", "binary", "drop", "fail", or "" (default)
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
   * Determine the effective recursion handling mode based on configuration precedence rules.
   *
   * Precedence:
   * 1. Explicit mode (if specified) always takes precedence
   * 2. Depth-based defaults when mode="" (empty)
   * 3. allowRecursion flag only used for depth=-1 with mode=""
   */
  private def determineRecursionMode(
      recursiveFieldsMode: String,
      recursiveFieldMaxDepth: Int,
      allowRecursion: Boolean): RecursionMode = {

    // Parse explicit mode string to enum
    def parseMode(mode: String): RecursionMode = mode match {
      case "fail" => RecursionMode.Fail
      case "drop" => RecursionMode.Drop
      case "binary" => RecursionMode.MockAsBinary
      case "recursive" => RecursionMode.AllowRecursive
      case "" => null  // Will be handled by depth-based defaults
      case _ => throw new IllegalArgumentException(s"Invalid recursion mode: '$mode'")
    }

    // If explicit mode specified (non-empty), use it
    if (recursiveFieldsMode != "") {
      parseMode(recursiveFieldsMode)
    } else {
      // mode="" → use depth-based defaults
      recursiveFieldMaxDepth match {
        case -1 =>
          // Spark default: forbid recursion, but allow if allowRecursion=true
          if (allowRecursion) RecursionMode.AllowRecursive else RecursionMode.Fail

        case 0 =>
          // Our extension: unlimited recursion
          RecursionMode.AllowRecursive

        case _ if recursiveFieldMaxDepth >= 1 =>
          // Spark-aligned depth limit: default to drop
          RecursionMode.Drop
      }
    }
  }

  /**
   * Convert protobuf descriptor to Spark SQL type respecting all configuration options.
   *
   * This is the unified entry point that handles all recursive field configurations with
   * precedence rules:
   *
   * When depth=-1 (default/unlimited):
   * - mode="" + allowRecursion=false → fail (throw on recursion)
   * - mode="" + allowRecursion=true → recursive (RecursiveStructType)
   * - mode="drop"/"binary" → IGNORED (depth=-1 takes precedence)
   * - mode="fail"/"recursive" → explicit mode used
   *
   * When depth>=0 (depth-limited):
   * - mode="" → drop (default)
   * - mode="drop"/"binary"/"fail" → use specified mode
   * - allowRecursion → IGNORED (mode takes precedence)
   *
   * @param descriptor the protobuf message descriptor
   * @param recursiveFieldsMode How to handle recursive fields ("", "drop", "binary", "fail", "recursive")
   * @param recursiveFieldMaxDepth Maximum recursion depth (-1 for unlimited, 0+ for limit)
   * @param allowRecursion Whether recursion is allowed (set by parser selection)
   * @param enumAsInt if true, represent enum fields as IntegerType instead of StringType
   * @return Spark SQL DataType with recursive fields handled according to config
   */
  def toSqlType(
      descriptor: Descriptor,
      recursiveFieldsMode: String,
      recursiveFieldMaxDepth: Int,
      allowRecursion: Boolean,
      enumAsInt: Boolean = false): DataType = {

    // Determine effective mode using precedence rules
    val mode = determineRecursionMode(recursiveFieldsMode, recursiveFieldMaxDepth, allowRecursion)

    // Convert user depth to internal max depth
    // User depth N means allow N recursions (recursiveDepth 0 to N-1)
    val internalMaxDepth = recursiveFieldMaxDepth match {
      case -1 => -1  // Not used in fail/recursive modes
      case 0 => -1   // Unlimited (not used in recursive mode)
      case n if n >= 1 => n  // Allow recursiveDepth from 0 to n-1
    }

    // Apply effective mode
    mode match {
      case RecursionMode.AllowRecursive =>
        // RecursiveStructType with true circular references (unlimited only)
        toSqlTypeWithTrueRecursion(descriptor, enumAsInt)

      case RecursionMode.Fail | RecursionMode.Drop | RecursionMode.MockAsBinary =>
        // Use unified conversion with mode-specific handling
        val visitedTypes = mutable.Set[String]()
        convertMessageType(descriptor, visitedTypes, enumAsInt, mode,
                         recursiveDepth = 0, maxRecursiveDepth = internalMaxDepth)
          .getOrElse(StructType(Seq.empty))  // Return empty struct if depth exceeded
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
   *
   * Recursive depth tracking:
   * - recursiveDepth=0: Not in a cycle
   * - recursiveDepth=1: First recursion
   * - recursiveDepth>1: Nested recursions
   * - Returns None when recursiveDepth > maxRecursiveDepth (for drop/mock modes)
   *
   * Example A→B→A→B→A with maxDepth=3:
   * - A (root): depth=0
   * - B (first): depth=0
   * - A (recurse): depth=1
   * - B (recurse): depth=2
   * - A (recurse): depth=3
   * - B (recurse): depth=4 > 3, returns None
   */
  private def convertMessageType(
      descriptor: Descriptor,
      visitedTypes: mutable.Set[String],
      enumAsInt: Boolean = false,
      mode: RecursionMode = RecursionMode.Fail,
      recursiveDepth: Int = 0,
      maxRecursiveDepth: Int = -1): Option[StructType] = {

    // Check depth limit (drop when depth > maxDepth)
    if (maxRecursiveDepth >= 0 && recursiveDepth > maxRecursiveDepth) {
      return None
    }

    // Mark this type as being visited to detect cycles
    val typeName = descriptor.getFullName
    val wasAlreadyVisited = visitedTypes.contains(typeName)
    visitedTypes += typeName

    val fields = supportedFields(descriptor).flatMap { field =>
      convertFieldType(field, visitedTypes, enumAsInt, mode, recursiveDepth, maxRecursiveDepth) match {
        case None => None  // Field dropped due to recursion or depth limit
        case Some(sparkType) => Some(StructField(field.getName, sparkType, nullable = true))
      }
    }.toSeq

    // Clean up: remove from visited set when done (for clean recursion detection)
    if (!wasAlreadyVisited) {
      visitedTypes -= typeName
    }

    Some(StructType(fields))
  }

  /**
   * Convert a single field type, handling recursion detection and depth tracking.
   *
   * Returns None if the field should be dropped due to recursion or depth limit.
   */
  private def convertFieldType(
      field: FieldDescriptor,
      visitedTypes: mutable.Set[String],
      enumAsInt: Boolean = false,
      mode: RecursionMode = RecursionMode.Fail,
      recursiveDepth: Int = 0,
      maxRecursiveDepth: Int = -1): Option[DataType] = {

    require(field.getType != FieldDescriptor.Type.GROUP, "GROUP fields are not supported")

    val baseTypeOpt = field.getJavaType match {
      case FieldDescriptor.JavaType.MESSAGE =>
        val messageDescriptor = field.getMessageType
        val messageTypeName = messageDescriptor.getFullName

        // Check for recursion: if we're already processing this type
        if (visitedTypes.contains(messageTypeName)) {
          // RECURSION DETECTED! Apply mode-specific handling
          mode match {
            case RecursionMode.Fail =>
              throw new IllegalArgumentException(
                s"Recursive field detected: ${field.getName} of type ${messageTypeName}. " +
                "Recursive schemas are not supported in fail mode.")

            case RecursionMode.Drop =>
              // Check if we've exceeded depth (for depth=-1, drop immediately on recursion)
              if (maxRecursiveDepth == -1 || recursiveDepth + 1 > maxRecursiveDepth) {
                // Drop recursive field
                None
              } else {
                // Within depth limit, continue recursing
                convertMessageType(messageDescriptor, visitedTypes.clone(), enumAsInt, mode,
                                 recursiveDepth + 1, maxRecursiveDepth)
              }

            case RecursionMode.MockAsBinary =>
              // Check if we've exceeded depth (for depth=-1, mock immediately on recursion)
              if (maxRecursiveDepth == -1 || recursiveDepth + 1 > maxRecursiveDepth) {
                // Mock recursive field as BinaryType
                Some(BinaryType)
              } else {
                // Within depth limit, continue recursing
                convertMessageType(messageDescriptor, visitedTypes.clone(), enumAsInt, mode,
                                 recursiveDepth + 1, maxRecursiveDepth)
              }

            case RecursionMode.AllowRecursive =>
              // This should not happen (AllowRecursive uses toSqlTypeWithTrueRecursion)
              throw new IllegalStateException("AllowRecursive mode should use toSqlTypeWithTrueRecursion")
          }
        } else {
          // Not recursive - check if we're inside a cycle
          val nextDepth = if (recursiveDepth > 0) {
            // Inside a cycle: increment for ALL message fields
            recursiveDepth + 1
          } else {
            // Not in a cycle yet, stay at 0
            0
          }

          // Continue normal conversion
          convertMessageType(messageDescriptor, visitedTypes.clone(), enumAsInt, mode,
                           nextDepth, maxRecursiveDepth)
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

          val keyTypeOpt = convertFieldType(keyField, visitedTypes, enumAsInt, mode,
                                           recursiveDepth, maxRecursiveDepth)
          val valueTypeOpt = convertFieldType(valueField, visitedTypes, enumAsInt, mode,
                                             recursiveDepth, maxRecursiveDepth)

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
    val schema = toSqlType(descriptor, recursiveFieldsMode = "binary", recursiveFieldMaxDepth = -1,
                          allowRecursion = false, enumAsInt = false)
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