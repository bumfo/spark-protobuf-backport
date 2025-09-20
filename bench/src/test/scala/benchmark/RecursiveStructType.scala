package benchmark

import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * A StructType subclass that handles circular references properly.
 *
 * Unlike regular StructType, this class can handle recursive schemas where
 * fields reference back to the same or parent types without causing infinite
 * loops in hashCode computation or string representation.
 */
class RecursiveStructType(fields: Array[StructField], override val typeName: String)
    extends StructType(fields) {

  /**
   * Override hashCode with recursion tracking to avoid infinite loops.
   * Uses identity hash for tracking to avoid calling hashCode during recursion detection.
   */
  override lazy val hashCode: Int = {
    hashCodeInternal(mutable.Set.empty[Int])
  }

  private def hashCodeInternal(visitedIdentities: mutable.Set[Int]): Int = {
    val thisIdentity = System.identityHashCode(this)
    if (visitedIdentities.contains(thisIdentity)) {
      // Circular reference detected - return deterministic value based on type name and field count
      typeName.hashCode * 31 + fields.length
    } else {
      visitedIdentities += thisIdentity

      // Manually compute hash like Arrays.hashCode but with recursion handling
      var result = 1
      fields.foreach { field =>
        val fieldHash = computeFieldHash(field, visitedIdentities)
        result = 31 * result + fieldHash
      }

      visitedIdentities -= thisIdentity
      result
    }
  }

  /**
   * Compute hash for a single StructField with visited tracking.
   */
  private def computeFieldHash(field: StructField, visitedIdentities: mutable.Set[Int]): Int = {
    // Manually compute like case class hashCode
    var hash = field.name.hashCode
    hash = hash * 31 + field.nullable.hashCode
    hash = hash * 31 + field.metadata.hashCode
    hash = hash * 31 + computeDataTypeHash(field.dataType, visitedIdentities)
    hash
  }

  /**
   * Compute hash for a DataType with visited tracking to handle nested recursive types.
   */
  private def computeDataTypeHash(dataType: DataType, visitedIdentities: mutable.Set[Int]): Int = {
    dataType match {
      case rst: RecursiveStructType =>
        rst.hashCodeInternal(visitedIdentities)

      case ArrayType(elementType, containsNull) =>
        val elemHash = computeDataTypeHash(elementType, visitedIdentities)
        elemHash * 31 + containsNull.hashCode

      case StructType(nestedFields) =>
        // Handle nested regular StructType
        var hash = 1
        nestedFields.foreach { f =>
          hash = 31 * hash + computeFieldHash(f, visitedIdentities)
        }
        hash

      case MapType(keyType, valueType, valueContainsNull) =>
        var hash = computeDataTypeHash(keyType, visitedIdentities)
        hash = 31 * hash + computeDataTypeHash(valueType, visitedIdentities)
        hash = 31 * hash + valueContainsNull.hashCode
        hash

      case other =>
        // For all other types, use their standard hashCode
        other.hashCode
    }
  }

  /**
   * Override equals to add reference equality check at the top level.
   * This breaks recursion naturally for circular references since the same
   * instance will be reference-equal to itself.
   */
  override def equals(that: Any): Boolean = {
    if (this eq that.asInstanceOf[AnyRef]) return true  // Reference equality check
    super.equals(that)  // Delegate to parent - Arrays.equals handles the rest
  }

  /**
   * Override simpleString with visited tracking to avoid infinite loops.
   */
  override def simpleString: String = simpleStringInternal(mutable.Set.empty[Int])

  /**
   * Override toString to use the recursion-safe simpleString instead of the problematic default.
   */
  override def toString: String = simpleString

  private def simpleStringInternal(visitedIdentities: mutable.Set[Int]): String = {
    val thisIdentity = System.identityHashCode(this)
    if (visitedIdentities.contains(thisIdentity)) {
      s"struct<recursive:$typeName>"
    } else {
      visitedIdentities += thisIdentity
      val fieldStrings = fields.map { field =>
        val typeStr = field.dataType match {
          case rst: RecursiveStructType =>
            rst.simpleStringInternal(visitedIdentities)
          case ArrayType(rst: RecursiveStructType, _) =>
            s"array<${rst.simpleStringInternal(visitedIdentities)}>"
          case MapType(rst: RecursiveStructType, valueType, _) =>
            s"map<${rst.simpleStringInternal(visitedIdentities)},${dataTypeString(valueType, visitedIdentities)}>"
          case MapType(keyType, rst: RecursiveStructType, _) =>
            s"map<${dataTypeString(keyType, visitedIdentities)},${rst.simpleStringInternal(visitedIdentities)}>"
          case dt =>
            dataTypeString(dt, visitedIdentities)
        }
        s"${field.name}:$typeStr"
      }
      visitedIdentities -= thisIdentity
      s"struct<${fieldStrings.mkString(",")}>"
    }
  }

  private def dataTypeString(dataType: DataType, visitedIdentities: mutable.Set[Int]): String = {
    dataType match {
      case rst: RecursiveStructType => rst.simpleStringInternal(visitedIdentities)
      case ArrayType(rst: RecursiveStructType, _) => s"array<${rst.simpleStringInternal(visitedIdentities)}>"
      case ArrayType(elementType, _) => s"array<${dataTypeString(elementType, visitedIdentities)}>"
      case MapType(keyType, valueType, _) =>
        s"map<${dataTypeString(keyType, visitedIdentities)},${dataTypeString(valueType, visitedIdentities)}>"
      case StructType(nestedFields) =>
        val fieldStrings = nestedFields.map { field =>
          s"${field.name}:${dataTypeString(field.dataType, visitedIdentities)}"
        }
        s"struct<${fieldStrings.mkString(",")}>"
      case other => other.simpleString
    }
  }

  /**
   * Override catalogString with visited tracking.
   */
  override def catalogString: String = catalogStringInternal(mutable.Set.empty[Int])

  private def catalogStringInternal(visitedIdentities: mutable.Set[Int]): String = {
    val thisIdentity = System.identityHashCode(this)
    if (visitedIdentities.contains(thisIdentity)) {
      s"STRUCT<recursive:$typeName>"
    } else {
      visitedIdentities += thisIdentity
      val fieldStrings = fields.map { field =>
        val typeStr = field.dataType match {
          case rst: RecursiveStructType =>
            rst.catalogStringInternal(visitedIdentities)
          case ArrayType(rst: RecursiveStructType, _) =>
            s"ARRAY<${rst.catalogStringInternal(visitedIdentities)}>"
          case dt =>
            catalogDataTypeString(dt, visitedIdentities)
        }
        s"${field.name}:$typeStr"
      }
      visitedIdentities -= thisIdentity
      s"STRUCT<${fieldStrings.mkString(",")}>"
    }
  }

  private def catalogDataTypeString(dataType: DataType, visitedIdentities: mutable.Set[Int]): String = {
    dataType match {
      case rst: RecursiveStructType => rst.catalogStringInternal(visitedIdentities)
      case ArrayType(rst: RecursiveStructType, _) => s"ARRAY<${rst.catalogStringInternal(visitedIdentities)}>"
      case ArrayType(elementType, _) => s"ARRAY<${catalogDataTypeString(elementType, visitedIdentities)}>"
      case MapType(keyType, valueType, _) =>
        s"MAP<${catalogDataTypeString(keyType, visitedIdentities)},${catalogDataTypeString(valueType, visitedIdentities)}>"
      case StructType(nestedFields) =>
        val fieldStrings = nestedFields.map { field =>
          s"${field.name}:${catalogDataTypeString(field.dataType, visitedIdentities)}"
        }
        s"STRUCT<${fieldStrings.mkString(",")}>"
      case other => other.catalogString
    }
  }

  /**
   * Override sql with visited tracking.
   */
  override def sql: String = sqlInternal(mutable.Set.empty[Int])

  private def sqlInternal(visitedIdentities: mutable.Set[Int]): String = {
    val thisIdentity = System.identityHashCode(this)
    if (visitedIdentities.contains(thisIdentity)) {
      s"STRUCT<recursive_${typeName.replaceAll("[^a-zA-Z0-9_]", "_")}>"
    } else {
      visitedIdentities += thisIdentity
      val fieldStrings = fields.map { field =>
        val typeStr = field.dataType match {
          case rst: RecursiveStructType =>
            rst.sqlInternal(visitedIdentities)
          case ArrayType(rst: RecursiveStructType, _) =>
            s"ARRAY<${rst.sqlInternal(visitedIdentities)}>"
          case dt =>
            sqlDataTypeString(dt, visitedIdentities)
        }
        s"`${field.name}` $typeStr"
      }
      visitedIdentities -= thisIdentity
      s"STRUCT<${fieldStrings.mkString(", ")}>"
    }
  }

  private def sqlDataTypeString(dataType: DataType, visitedIdentities: mutable.Set[Int]): String = {
    dataType match {
      case rst: RecursiveStructType => rst.sqlInternal(visitedIdentities)
      case ArrayType(rst: RecursiveStructType, _) => s"ARRAY<${rst.sqlInternal(visitedIdentities)}>"
      case ArrayType(elementType, _) => s"ARRAY<${sqlDataTypeString(elementType, visitedIdentities)}>"
      case MapType(keyType, valueType, _) =>
        s"MAP<${sqlDataTypeString(keyType, visitedIdentities)}, ${sqlDataTypeString(valueType, visitedIdentities)}>"
      case StructType(nestedFields) =>
        val fieldStrings = nestedFields.map { field =>
          s"`${field.name}` ${sqlDataTypeString(field.dataType, visitedIdentities)}"
        }
        s"STRUCT<${fieldStrings.mkString(", ")}>"
      case other => other.sql
    }
  }
}