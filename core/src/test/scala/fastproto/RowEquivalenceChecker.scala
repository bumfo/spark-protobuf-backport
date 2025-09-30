package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.scalatest.matchers.should.Matchers._

/**
 * Configuration options for row equivalence checking.
 */
case class EquivalenceOptions(
    treatEmptyStringAsNull: Boolean = true,
    allowEnumStringIntEquivalence: Boolean = true,
    // FIXME: Honor ignoreMapOrder when comparing map fields, see https://github.com/bumfo/spark-protobuf-backport/pull/17/files/5685b440e18207b3e412853ce874d943f2973b38#r2366192461
    ignoreMapOrder: Boolean = true,
    strictMode: Boolean = false
)

object EquivalenceOptions {
  val default: EquivalenceOptions = EquivalenceOptions()
  val strict: EquivalenceOptions = EquivalenceOptions(
    treatEmptyStringAsNull = false,
    allowEnumStringIntEquivalence = false,
    ignoreMapOrder = false,
    strictMode = true
  )
}

/**
 * Utility for comparing InternalRows with protobuf-specific equivalence handling.
 *
 * Handles common protobuf conversion differences between parsers:
 * - Empty strings vs nulls (protobuf default behavior)
 * - Enum string names vs integer values
 * - Map field ordering differences
 * - Default value representations
 */
object RowEquivalenceChecker {

  /**
   * Assert that two InternalRows are equivalent according to protobuf semantics.
   * Supports different schemas for each row to handle cases where parsers
   * generate different representations (e.g., enum as string vs int).
   *
   * @param row1       First row to compare
   * @param schema1    Spark schema for row1
   * @param row2       Second row to compare
   * @param schema2    Spark schema for row2
   * @param descriptor Optional protobuf descriptor for enum field identification
   * @param options    Comparison options
   * @param path       Field path for error reporting
   */
  def assertRowsEquivalent(
      row1: InternalRow,
      schema1: StructType,
      row2: InternalRow,
      schema2: StructType,
      descriptor: Option[Descriptor],
      options: EquivalenceOptions,
      path: String
  ): Unit = {
    compareRows(row1, row2, schema1, schema2, descriptor, options, path)
  }

  /**
   * Assert that two InternalRows are equivalent using the same schema.
   * Convenience method for backward compatibility.
   *
   * @param row1       First row to compare
   * @param row2       Second row to compare
   * @param schema     Spark schema for both rows
   * @param descriptor Optional protobuf descriptor for enum field identification
   * @param options    Comparison options
   * @param path       Field path for error reporting
   */
  def assertRowsEquivalent(
      row1: InternalRow,
      row2: InternalRow,
      schema: StructType,
      descriptor: Option[Descriptor] = None,
      options: EquivalenceOptions = EquivalenceOptions.default,
      path: String = "root"
  ): Unit = {
    assertRowsEquivalent(row1, schema, row2, schema, descriptor, options, path)
  }

  private def compareRows(
      row1: InternalRow,
      row2: InternalRow,
      schema1: StructType,
      schema2: StructType,
      descriptor: Option[Descriptor],
      options: EquivalenceOptions,
      path: String
  ): Unit = {
    row1.numFields shouldBe row2.numFields
    schema1.fields.length shouldBe schema2.fields.length

    for (i <- schema1.fields.indices) {
      val field1 = schema1.fields(i)
      val field2 = schema2.fields(i)

      // Field names should match
      field1.name shouldBe field2.name

      val fieldPath = s"$path.${field1.name}"
      val fieldDescriptor = descriptor.flatMap(d =>
        Option(d.findFieldByName(field1.name))
      )

      compareField(row1, row2, i, field1.dataType, field2.dataType, fieldDescriptor, options, fieldPath)
    }
  }

  private def compareField(
      row1: InternalRow,
      row2: InternalRow,
      fieldIndex: Int,
      dataType1: DataType,
      dataType2: DataType,
      fieldDescriptor: Option[FieldDescriptor],
      options: EquivalenceOptions,
      fieldPath: String
  ): Unit = {
    val isNull1 = row1.isNullAt(fieldIndex)
    val isNull2 = row2.isNullAt(fieldIndex)

    if (isNull1 && isNull2) {
      return
    }

    // Check if this is an enum field - enums can be stored as either string or int by different parsers
    val isEnum = fieldDescriptor.exists(_.getType == FieldDescriptor.Type.ENUM)


    if (isEnum && options.allowEnumStringIntEquivalence) {
      // For enum fields, read each row according to its schema and normalize to enum name
      val enumStr1 = getEnumAsString(row1, fieldIndex, dataType1, fieldDescriptor, fieldPath)
      val enumStr2 = getEnumAsString(row2, fieldIndex, dataType2, fieldDescriptor, fieldPath)

      if (enumStr1 != enumStr2) {
        fail(s"Enum mismatch at $fieldPath: '$enumStr1' != '$enumStr2'")
      }
    } else {
      // For non-enum fields, handle type-specific comparison
      (dataType1, dataType2) match {
        case (structType1: StructType, structType2: StructType) =>
          // StructTypes might differ only in enum field representations - let compareStructField handle it
          compareStructField(row1, row2, fieldIndex, structType1, structType2, fieldDescriptor, options, fieldPath)
          return  // Early return to avoid the dataType1 match below

        case (arrayType1: ArrayType, arrayType2: ArrayType) =>
          // ArrayTypes might differ only in enum field representations in their element types
          compareArrayField(row1, row2, fieldIndex, arrayType1, arrayType2, fieldDescriptor, options, fieldPath)
          return  // Early return to avoid the dataType1 match below

        case _ =>
          // For other types, data types should match exactly
          if (dataType1 != dataType2) {
            fail(s"Data type mismatch at $fieldPath: $dataType1 != $dataType2")
          }
      }

      dataType1 match {
        case _: org.apache.spark.sql.types.StringType =>
          compareStringField(row1, row2, fieldIndex, fieldDescriptor, options, fieldPath)

        case _: org.apache.spark.sql.types.IntegerType =>
          compareIntField(row1, row2, fieldIndex, fieldDescriptor, options, fieldPath)

        case _: org.apache.spark.sql.types.LongType =>
          val long1 = if (isNull1) 0L else row1.getLong(fieldIndex)
          val long2 = if (isNull2) 0L else row2.getLong(fieldIndex)
          if (long1 != long2) {
            fail(s"Long mismatch at $fieldPath: $long1 != $long2")
          }

        case _: org.apache.spark.sql.types.BooleanType =>
          val bool1 = if (isNull1) false else row1.getBoolean(fieldIndex)
          val bool2 = if (isNull2) false else row2.getBoolean(fieldIndex)
          if (bool1 != bool2) {
            fail(s"Boolean mismatch at $fieldPath: $bool1 != $bool2")
          }

        case _: org.apache.spark.sql.types.DoubleType =>
          val double1 = if (isNull1) 0.0 else row1.getDouble(fieldIndex)
          val double2 = if (isNull2) 0.0 else row2.getDouble(fieldIndex)
          if (math.abs(double1 - double2) > 1e-9) {
            fail(s"Double mismatch at $fieldPath: $double1 != $double2")
          }

        case _: org.apache.spark.sql.types.FloatType =>
          val float1 = if (isNull1) 0.0f else row1.getFloat(fieldIndex)
          val float2 = if (isNull2) 0.0f else row2.getFloat(fieldIndex)
          if (math.abs(float1 - float2) > 1e-6f) {
            fail(s"Float mismatch at $fieldPath: $float1 != $float2")
          }

        case _: org.apache.spark.sql.types.BinaryType =>
          if (isNull1 != isNull2) {
            fail(s"Binary null mismatch at $fieldPath")
          }
          if (!isNull1 && !isNull2) {
            val bin1 = row1.getBinary(fieldIndex)
            val bin2 = row2.getBinary(fieldIndex)
            if (!java.util.Arrays.equals(bin1, bin2)) {
              fail(s"Binary mismatch at $fieldPath")
            }
          }

        // ArrayType case is handled above to allow for enum type differences

        // StructType case is handled above to allow for enum type differences

        case mapType: MapType =>
          val mapType2 = dataType2.asInstanceOf[MapType]
          compareMapField(row1, row2, fieldIndex, mapType, mapType2, options, fieldPath)

        case other =>
          fail(s"Unsupported field type $other at $fieldPath")
      }
    }
  }

  private def compareStringField(
      row1: InternalRow,
      row2: InternalRow,
      fieldIndex: Int,
      fieldDescriptor: Option[FieldDescriptor],
      options: EquivalenceOptions,
      fieldPath: String
  ): Unit = {
    val isNull1 = row1.isNullAt(fieldIndex)
    val isNull2 = row2.isNullAt(fieldIndex)

    val str1 = if (isNull1) null else row1.getUTF8String(fieldIndex).toString
    val str2 = if (isNull2) null else row2.getUTF8String(fieldIndex).toString

    val isEnum = fieldDescriptor.exists(_.getType == FieldDescriptor.Type.ENUM)

    if (isEnum && options.allowEnumStringIntEquivalence) {
      if (!areEnumsEquivalent(str1, str2, fieldDescriptor)) {
        fail(s"Enum mismatch at $fieldPath: '$str1' != '$str2'")
      }
    } else if (options.treatEmptyStringAsNull) {
      val normalizedStr1 = if (str1 == null || str1.isEmpty) null else str1
      val normalizedStr2 = if (str2 == null || str2.isEmpty) null else str2
      if (normalizedStr1 != normalizedStr2) {
        fail(s"String mismatch at $fieldPath: '$str1' != '$str2'")
      }
    } else {
      if (str1 != str2) {
        fail(s"String mismatch at $fieldPath: '$str1' != '$str2'")
      }
    }
  }

  private def compareIntField(
      row1: InternalRow,
      row2: InternalRow,
      fieldIndex: Int,
      fieldDescriptor: Option[FieldDescriptor],
      options: EquivalenceOptions,
      fieldPath: String
  ): Unit = {
    val isNull1 = row1.isNullAt(fieldIndex)
    val isNull2 = row2.isNullAt(fieldIndex)

    val int1 = if (isNull1) 0 else row1.getInt(fieldIndex)
    val int2 = if (isNull2) 0 else row2.getInt(fieldIndex)

    if (int1 != int2) {
      fail(s"Int mismatch at $fieldPath: $int1 != $int2")
    }
  }

  private def compareArrayField(
      row1: InternalRow,
      row2: InternalRow,
      fieldIndex: Int,
      arrayType1: ArrayType,
      arrayType2: ArrayType,
      fieldDescriptor: Option[FieldDescriptor],
      options: EquivalenceOptions,
      fieldPath: String
  ): Unit = {
    val isNull1 = row1.isNullAt(fieldIndex)
    val isNull2 = row2.isNullAt(fieldIndex)

    if (isNull1 && isNull2) return
    if (isNull1 != isNull2) {
      fail(s"Array null mismatch at $fieldPath")
    }

    val array1 = row1.getArray(fieldIndex)
    val array2 = row2.getArray(fieldIndex)

    compareArrays(array1, array2, arrayType1, arrayType2, fieldDescriptor, options, fieldPath)
  }

  private def compareStructField(
      row1: InternalRow,
      row2: InternalRow,
      fieldIndex: Int,
      structType1: StructType,
      structType2: StructType,
      fieldDescriptor: Option[FieldDescriptor],
      options: EquivalenceOptions,
      fieldPath: String
  ): Unit = {
    val isNull1 = row1.isNullAt(fieldIndex)
    val isNull2 = row2.isNullAt(fieldIndex)

    if (isNull1 && isNull2) return
    if (isNull1 != isNull2) {
      fail(s"Struct null mismatch at $fieldPath")
    }

    val struct1 = row1.getStruct(fieldIndex, structType1.size)
    val struct2 = row2.getStruct(fieldIndex, structType2.size)

    val nestedDescriptor = fieldDescriptor.flatMap(fd =>
      if (fd.getType == FieldDescriptor.Type.MESSAGE) Some(fd.getMessageType) else None
    )


    compareRows(struct1, struct2, structType1, structType2, nestedDescriptor, options, fieldPath)
  }

  private def compareMapField(
      row1: InternalRow,
      row2: InternalRow,
      fieldIndex: Int,
      mapType1: MapType,
      mapType2: MapType,
      options: EquivalenceOptions,
      fieldPath: String
  ): Unit = {
    val isNull1 = row1.isNullAt(fieldIndex)
    val isNull2 = row2.isNullAt(fieldIndex)

    if (isNull1 && isNull2) return
    if (isNull1 != isNull2) {
      fail(s"Map null mismatch at $fieldPath")
    }

    val array1 = row1.getArray(fieldIndex)
    val array2 = row2.getArray(fieldIndex)

    if (array1.numElements() != array2.numElements()) {
      fail(s"Map size mismatch at $fieldPath: ${array1.numElements()} != ${array2.numElements()}")
    }

    val keyValueSchema1 = StructType(Seq(
      org.apache.spark.sql.types.StructField("key", mapType1.keyType),
      org.apache.spark.sql.types.StructField("value", mapType1.valueType)
    ))

    val keyValueSchema2 = StructType(Seq(
      org.apache.spark.sql.types.StructField("key", mapType2.keyType),
      org.apache.spark.sql.types.StructField("value", mapType2.valueType)
    ))

    compareArrays(array1, array2, ArrayType(keyValueSchema1), ArrayType(keyValueSchema2), None, options, fieldPath)
  }

  private def compareArrays(
      array1: ArrayData,
      array2: ArrayData,
      arrayType1: ArrayType,
      arrayType2: ArrayType,
      fieldDescriptor: Option[FieldDescriptor],
      options: EquivalenceOptions,
      path: String
  ): Unit = {
    if (array1.numElements() != array2.numElements()) {
      fail(s"Array size mismatch at $path: ${array1.numElements()} != ${array2.numElements()}")
    }

    for (i <- 0 until array1.numElements()) {
      val elemPath = s"$path[$i]"
      compareArrayElement(array1, array2, i, arrayType1.elementType, arrayType2.elementType, fieldDescriptor, options, elemPath)
    }
  }

  private def compareArrayElement(
      array1: ArrayData,
      array2: ArrayData,
      index: Int,
      elementType1: DataType,
      elementType2: DataType,
      fieldDescriptor: Option[FieldDescriptor],
      options: EquivalenceOptions,
      path: String
  ): Unit = {
    val isNull1 = array1.isNullAt(index)
    val isNull2 = array2.isNullAt(index)

    if (isNull1 && isNull2) return
    if (isNull1 != isNull2) {
      fail(s"Array element null mismatch at $path")
    }

    // Check if this is an enum element
    val isEnum = fieldDescriptor.exists(_.getType == FieldDescriptor.Type.ENUM)

    if (isEnum && options.allowEnumStringIntEquivalence) {
      // For enum elements, read each according to its schema and normalize to enum name
      val enumStr1 = getEnumAsString(array1, index, elementType1, fieldDescriptor, s"$path[$index]")
      val enumStr2 = getEnumAsString(array2, index, elementType2, fieldDescriptor, s"$path[$index]")

      if (enumStr1 != enumStr2) {
        fail(s"Array enum mismatch at $path: '$enumStr1' != '$enumStr2'")
      }
    } else {
      // For non-enum elements, handle type-specific comparison
      (elementType1, elementType2) match {
        case (structType1: StructType, structType2: StructType) =>
          // StructTypes might differ only in enum field representations
          val struct1 = array1.getStruct(index, structType1.size)
          val struct2 = array2.getStruct(index, structType2.size)
          val nestedDescriptor = fieldDescriptor.flatMap(fd =>
            if (fd.getType == FieldDescriptor.Type.MESSAGE) Some(fd.getMessageType) else None
          )
          compareRows(struct1, struct2, structType1, structType2, nestedDescriptor, options, path)
          return  // Early return to avoid the elementType1 match below

        case _ =>
          // For other element types, types should match exactly
          if (elementType1 != elementType2) {
            fail(s"Array element type mismatch at $path: $elementType1 != $elementType2")
          }
      }

      elementType1 match {
        case _: org.apache.spark.sql.types.StringType =>
          val utf8Str1 = array1.getUTF8String(index)
          val utf8Str2 = array2.getUTF8String(index)
          val str1 = if (utf8Str1 != null) utf8Str1.toString else null
          val str2 = if (utf8Str2 != null) utf8Str2.toString else null

          if (options.treatEmptyStringAsNull) {
            val normalizedStr1 = if (str1 == null || str1.isEmpty) null else str1
            val normalizedStr2 = if (str2 == null || str2.isEmpty) null else str2
            if (normalizedStr1 != normalizedStr2) {
              fail(s"Array string mismatch at $path: '$str1' != '$str2'")
            }
          } else {
            if (str1 != str2) {
              fail(s"Array string mismatch at $path: '$str1' != '$str2'")
            }
          }

        case _: org.apache.spark.sql.types.IntegerType =>
          val int1 = array1.getInt(index)
          val int2 = array2.getInt(index)
          if (int1 != int2) {
            fail(s"Array int mismatch at $path: $int1 != $int2")
          }

        case _: org.apache.spark.sql.types.LongType =>
          val long1 = array1.getLong(index)
          val long2 = array2.getLong(index)
          if (long1 != long2) {
            fail(s"Array long mismatch at $path: $long1 != $long2")
          }

        case _: org.apache.spark.sql.types.FloatType =>
          val float1 = array1.getFloat(index)
          val float2 = array2.getFloat(index)
          if (math.abs(float1 - float2) > 1e-6f) {
            fail(s"Array float mismatch at $path: $float1 != $float2")
          }

        case _: org.apache.spark.sql.types.DoubleType =>
          val double1 = array1.getDouble(index)
          val double2 = array2.getDouble(index)
          if (math.abs(double1 - double2) > 1e-9) {
            fail(s"Array double mismatch at $path: $double1 != $double2")
          }

        case _: org.apache.spark.sql.types.BooleanType =>
          val bool1 = array1.getBoolean(index)
          val bool2 = array2.getBoolean(index)
          if (bool1 != bool2) {
            fail(s"Array boolean mismatch at $path: $bool1 != $bool2")
          }

        case _: org.apache.spark.sql.types.BinaryType =>
          val bin1 = array1.getBinary(index)
          val bin2 = array2.getBinary(index)
          if (!java.util.Arrays.equals(bin1, bin2)) {
            fail(s"Array binary mismatch at $path")
          }

        // StructType case is handled above to allow for enum type differences

        case other =>
          fail(s"Unsupported array element type $other at $path")
      }
    }
  }

  /**
   * Get enum value as a normalized string representation.
   * If stored as int, converts to enum name. If stored as string, returns as-is.
   */
  private def getEnumAsString(
      row: InternalRow,
      fieldIndex: Int,
      dataType: DataType,
      fieldDescriptor: Option[FieldDescriptor],
      fieldPath: String
  ): String = {
    if (row.isNullAt(fieldIndex)) return ""

    dataType match {
      case _: org.apache.spark.sql.types.StringType =>
        // Stored as string - return as-is (could be enum name or empty/garbage if actually int)
        val utf8Str = row.getUTF8String(fieldIndex)
        val str = if (utf8Str != null) utf8Str.toString else ""
        // // If empty string, likely int bytes read as UTF8 - assume it's enum value 0
        // if (str.isEmpty) {
        //   fieldDescriptor match {
        //     case Some(fd) if fd.getType == FieldDescriptor.Type.ENUM =>
        //       val enumValue = fd.getEnumType.findValueByNumber(0)
        //       if (enumValue != null) enumValue.getName else str
        //     case _ => str
        //   }
        // } else {
        //   str
        // }
      str

      case _: org.apache.spark.sql.types.IntegerType =>
        // Stored as int - convert to enum name
        val intVal = row.getInt(fieldIndex)
        fieldDescriptor match {
          case Some(fd) if fd.getType == FieldDescriptor.Type.ENUM =>
            val enumValue = fd.getEnumType.findValueByNumber(intVal)
            if (enumValue eq null) fail(s"Unexpected enum value $intVal at $fieldPath (row with IntegerType) for enum ${fd.getEnumType.getName}")
            enumValue.getName
          case _ =>
            fail(s"IntegerType field at $fieldPath is not an enum but fieldDescriptor says it should be")
        }

      case _ =>
        fail(s"Unexpected data type $dataType for enum field at $fieldPath")
    }
  }

  /**
   * Get enum value as a normalized string representation from an ArrayData element.
   * If stored as int, converts to enum name. If stored as string, returns as-is.
   */
  private def getEnumAsString(
      array: ArrayData,
      index: Int,
      dataType: DataType,
      fieldDescriptor: Option[FieldDescriptor],
      path: String
  ): String = {
    if (array.isNullAt(index)) return ""

    dataType match {
      case _: org.apache.spark.sql.types.StringType =>
        // Stored as string - return as-is
        val utf8Str = array.getUTF8String(index)
        val str = if (utf8Str != null) utf8Str.toString else ""
        // If empty string, likely int bytes read as UTF8 - assume it's enum value 0
        if (str.isEmpty) {
          fieldDescriptor match {
            case Some(fd) if fd.getType == FieldDescriptor.Type.ENUM =>
              val enumValue = fd.getEnumType.findValueByNumber(0)
              if (enumValue != null) enumValue.getName else str
            case _ => str
          }
        } else {
          str
        }

      case _: org.apache.spark.sql.types.IntegerType =>
        // Stored as int - convert to enum name
        val intVal = array.getInt(index)
        fieldDescriptor match {
          case Some(fd) if fd.getType == FieldDescriptor.Type.ENUM =>
            val enumValue = fd.getEnumType.findValueByNumber(intVal)
            if (enumValue != null) enumValue.getName else intVal.toString
          case _ => intVal.toString
        }

      case _ =>
        fail(s"Unexpected data type $dataType for enum array element at $path[$index]")
    }
  }

  /**
   * Check if two enum values are equivalent considering different representations.
   */
  private def areEnumsEquivalent(
      value1: String,
      value2: String,
      fieldDescriptor: Option[FieldDescriptor]
  ): Boolean = {
    if (value1 == value2) return true

    fieldDescriptor match {
      case Some(fd) if fd.getType == FieldDescriptor.Type.ENUM =>
        val enumType = fd.getEnumType

        // Try to parse as numbers and convert to names
        val name1 = try {
          val intVal = value1.toInt
          val enumValue = enumType.findValueByNumber(intVal)
          if (enumValue != null) enumValue.getName else value1
        } catch {
          case _: NumberFormatException => value1
        }

        val name2 = try {
          val intVal = value2.toInt
          val enumValue = enumType.findValueByNumber(intVal)
          if (enumValue != null) enumValue.getName else value2
        } catch {
          case _: NumberFormatException => value2
        }

        name1 == name2

      case _ =>
        // If we don't have enum metadata, accept common patterns
        (value1.isEmpty && value2 == "0") ||
          (value2.isEmpty && value1 == "0") ||
          value1 == value2
    }
  }
}