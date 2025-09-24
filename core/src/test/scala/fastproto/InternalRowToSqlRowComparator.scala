package fastproto

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._

/**
 * Utility for comparing InternalRow (from parsers) with Row (from Spark SQL).
 * Handles null safety and protobuf-specific comparison semantics.
 */
object InternalRowToSqlRowComparator {

  /**
   * Compare an InternalRow with a Spark SQL Row for equivalence.
   *
   * @param internalRow InternalRow from parser
   * @param sqlRow Row from Spark SQL
   * @param schema Schema for both rows
   * @param path Field path for error reporting
   */
  def compareRows(
      internalRow: InternalRow,
      sqlRow: org.apache.spark.sql.Row,
      schema: StructType,
      path: String = "root"
  ): Unit = {
    internalRow.numFields should equal(sqlRow.size)

    schema.fields.zipWithIndex.foreach { case (field, idx) =>
      val fieldPath = s"$path.${field.name}"

      field.dataType match {
        case StringType =>
          // For string fields, treat null and empty string as equivalent (protobuf semantics)
          val str1 = if (internalRow.isNullAt(idx)) {
            ""
          } else {
            val utf8Str = internalRow.getUTF8String(idx)
            if (utf8Str != null) utf8Str.toString else ""
          }
          val str2 = if (sqlRow.isNullAt(idx)) "" else sqlRow.getAs[String](idx)
          str1 should equal(str2)

        case _ =>
          // For non-string fields, use strict nullability comparison
          (internalRow.isNullAt(idx), sqlRow.isNullAt(idx)) match {
            case (true, true) => // Both null - OK

            case (false, false) =>
              field.dataType match {
                case IntegerType =>
                  val int1 = internalRow.getInt(idx)
                  val int2 = sqlRow.getAs[Int](idx)
                  int1 should equal(int2)

                case BooleanType =>
                  val bool1 = internalRow.getBoolean(idx)
                  val bool2 = sqlRow.getAs[Boolean](idx)
                  bool1 should equal(bool2)

                case BinaryType =>
                  val bytes1 = internalRow.getBinary(idx)
                  val bytes2 = sqlRow.getAs[Array[Byte]](idx)
                  bytes1 should equal(bytes2)

                case arrayType: ArrayType =>
                  val array1 = internalRow.getArray(idx)
                  val array2 = sqlRow.getAs[Seq[_]](idx)

                  if (array1 == null && array2 == null) {
                    // Both null - OK
                  } else if (array1 != null && array2 != null) {
                    array1.numElements() should equal(array2.size)

                    // Compare each element based on element type
                    arrayType.elementType match {
                      case StringType =>
                        for (i <- 0 until array1.numElements()) {
                          // For string array elements, treat null and empty string as equivalent
                          val elem1 = if (array1.isNullAt(i)) {
                            ""
                          } else {
                            val utf8Str = array1.getUTF8String(i)
                            if (utf8Str != null) utf8Str.toString else ""
                          }
                          val elem2 = if (array2(i) == null) "" else array2(i).asInstanceOf[String]
                          elem1 should equal(elem2)
                        }

                      case structType: StructType =>
                        for (i <- 0 until array1.numElements()) {
                          if (!array1.isNullAt(i)) {
                            val struct1 = array1.getStruct(i, structType.fields.length)
                            val struct2 = array2(i).asInstanceOf[org.apache.spark.sql.Row]
                            compareRows(struct1, struct2, structType, s"$fieldPath[$i]")
                          }
                        }

                      case _ =>
                        // For other array element types, just compare sizes for now
                        array1.numElements() should equal(array2.size)
                    }
                  } else {
                    fail(s"Array mismatch at $fieldPath: one is null, other is not")
                  }

                case structType: StructType =>
                  val struct1 = internalRow.getStruct(idx, structType.fields.length)
                  val struct2 = sqlRow.getAs[org.apache.spark.sql.Row](idx)

                  if (struct1 == null && struct2 == null) {
                    // Both null - OK
                  } else if (struct1 != null && struct2 != null) {
                    compareRows(struct1, struct2, structType, fieldPath)
                  } else {
                    fail(s"Struct mismatch at $fieldPath: one is null, other is not")
                  }

                case _ =>
                  fail(s"Unsupported field type ${field.dataType} at $fieldPath")
              }

            case (null1, null2) =>
              fail(s"Nullability mismatch at $fieldPath: internalRow.isNull=$null1, sqlRow.isNull=$null2")
          }
      }
    }
  }
}