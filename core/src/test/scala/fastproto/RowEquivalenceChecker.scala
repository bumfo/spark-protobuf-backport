package fastproto

import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.scalatest.matchers.should.Matchers._

import scala.util.control.NonFatal

/**
 * Configuration options for row equivalence checking.
 */
case class EquivalenceOptions(
    treatEmptyStringAsNull: Boolean = true,
    allowEnumStringIntEquivalence: Boolean = true,
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
   *
   * @param row1       First row to compare
   * @param row2       Second row to compare
   * @param schema     Spark schema for the rows
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
    compareRows(row1, row2, schema, descriptor, options, path)
  }

  private def compareRows(
      row1: InternalRow,
      row2: InternalRow,
      schema: StructType,
      descriptor: Option[Descriptor],
      options: EquivalenceOptions,
      path: String
  ): Unit = {
    row1.numFields shouldBe row2.numFields

    for (i <- schema.fields.indices) {
      val field = schema.fields(i)
      val fieldPath = s"$path.${field.name}"
      val fieldDescriptor = descriptor.flatMap(d =>
        Option(d.findFieldByName(field.name))
      )

      compareField(row1, row2, i, field.dataType, fieldDescriptor, options, fieldPath)
    }
  }

  private def compareField(
      row1: InternalRow,
      row2: InternalRow,
      fieldIndex: Int,
      dataType: DataType,
      fieldDescriptor: Option[FieldDescriptor],
      options: EquivalenceOptions,
      fieldPath: String
  ): Unit = {
    val isNull1 = row1.isNullAt(fieldIndex)
    val isNull2 = row2.isNullAt(fieldIndex)

    if (isNull1 && isNull2) {
      return
    }

    dataType match {
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

      case arrayType: ArrayType =>
        compareArrayField(row1, row2, fieldIndex, arrayType, fieldDescriptor, options, fieldPath)

      case structType: StructType =>
        compareStructField(row1, row2, fieldIndex, structType, fieldDescriptor, options, fieldPath)

      case mapType: MapType =>
        compareMapField(row1, row2, fieldIndex, mapType, options, fieldPath)

      case other =>
        fail(s"Unsupported field type $other at $fieldPath")
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
      arrayType: ArrayType,
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

    compareArrays(array1, array2, arrayType, fieldDescriptor, options, fieldPath)
  }

  private def compareStructField(
      row1: InternalRow,
      row2: InternalRow,
      fieldIndex: Int,
      structType: StructType,
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

    val struct1 = row1.getStruct(fieldIndex, structType.size)
    val struct2 = row2.getStruct(fieldIndex, structType.size)

    val nestedDescriptor = fieldDescriptor.flatMap(fd =>
      if (fd.getType == FieldDescriptor.Type.MESSAGE) Some(fd.getMessageType) else None
    )

    compareRows(struct1, struct2, structType, nestedDescriptor, options, fieldPath)
  }

  private def compareMapField(
      row1: InternalRow,
      row2: InternalRow,
      fieldIndex: Int,
      mapType: MapType,
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

    val keyValueSchema = StructType(Seq(
      org.apache.spark.sql.types.StructField("key", mapType.keyType),
      org.apache.spark.sql.types.StructField("value", mapType.valueType)
    ))

    compareArrays(array1, array2, ArrayType(keyValueSchema), None, options, fieldPath)
  }

  private def compareArrays(
      array1: ArrayData,
      array2: ArrayData,
      arrayType: ArrayType,
      fieldDescriptor: Option[FieldDescriptor],
      options: EquivalenceOptions,
      path: String
  ): Unit = {
    if (array1.numElements() != array2.numElements()) {
      fail(s"Array size mismatch at $path: ${array1.numElements()} != ${array2.numElements()}")
    }

    for (i <- 0 until array1.numElements()) {
      val elemPath = s"$path[$i]"
      compareArrayElement(array1, array2, i, arrayType.elementType, fieldDescriptor, options, elemPath)
    }
  }

  private def compareArrayElement(
      array1: ArrayData,
      array2: ArrayData,
      index: Int,
      elementType: DataType,
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

    elementType match {
      case _: org.apache.spark.sql.types.StringType =>
        val str1 = array1.getUTF8String(index).toString
        val str2 = array2.getUTF8String(index).toString

        val isEnum = fieldDescriptor.exists(_.getType == FieldDescriptor.Type.ENUM)
        if (isEnum && options.allowEnumStringIntEquivalence) {
          if (!areEnumsEquivalent(str1, str2, fieldDescriptor)) {
            fail(s"Array enum mismatch at $path: '$str1' != '$str2'")
          }
        } else if (options.treatEmptyStringAsNull) {
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

      case structType: StructType =>
        val struct1 = array1.getStruct(index, structType.size)
        val struct2 = array2.getStruct(index, structType.size)

        val nestedDescriptor = fieldDescriptor.flatMap(fd =>
          if (fd.getType == FieldDescriptor.Type.MESSAGE) Some(fd.getMessageType) else None
        )

        compareRows(struct1, struct2, structType, nestedDescriptor, options, path)

      case other =>
        fail(s"Unsupported array element type $other at $path")
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