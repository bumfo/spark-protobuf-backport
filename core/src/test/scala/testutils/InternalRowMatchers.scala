package testutils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Assertions}

/**
 * Custom matchers for safe and descriptive InternalRow field assertions.
 * Provides null-safe access and clear error messages for test failures.
 */
trait InternalRowMatchers extends Matchers with Assertions {

  /**
   * Matcher for individual field assertions with descriptive error messages.
   */
  case class FieldMatcher(row: InternalRow, ordinal: Int, fieldName: String = "") {

    /**
     * Assert field equals expected value with type-specific handling.
     * This is the fallback for Any type - prefer the type-specific overloads.
     */
    def shouldEqual(expected: Any): Assertion = expected match {
      case s: String => assertStringField(s)
      case i: Int => assertIntField(i)
      case l: Long => assertLongField(l)
      case f: Float => assertFloatField(f)
      case d: Double => assertDoubleField(d)
      case b: Boolean => assertBooleanField(b)
      case null => assertNull()
      case other => fail(s"Field ${fieldDesc}: Unsupported expected type ${other.getClass.getSimpleName}")
    }

    /**
     * Assert field equals expected string value (static dispatch).
     */
    def shouldEqual(expected: String): Assertion = assertStringField(expected)

    /**
     * Assert field equals expected int value (static dispatch).
     */
    def shouldEqual(expected: Int): Assertion = assertIntField(expected)

    /**
     * Assert field equals expected long value (static dispatch).
     */
    def shouldEqual(expected: Long): Assertion = assertLongField(expected)

    /**
     * Assert field equals expected float value (static dispatch).
     */
    def shouldEqual(expected: Float): Assertion = assertFloatField(expected)

    /**
     * Assert field equals expected double value (static dispatch).
     */
    def shouldEqual(expected: Double): Assertion = assertDoubleField(expected)

    /**
     * Assert field equals expected boolean value (static dispatch).
     */
    def shouldEqual(expected: Boolean): Assertion = assertBooleanField(expected)

    /**
     * Assert field is null.
     */
    def shouldBeNull(): Assertion = assertNull()

    /**
     * Assert field is not null.
     */
    def shouldNotBeNull(): Assertion = {
      if (row.isNullAt(ordinal)) {
        fail(s"Field ${fieldDesc} was null but expected to be non-null")
      }
      succeed
    }

    private def assertStringField(expected: String): Assertion = {
      if (row.isNullAt(ordinal)) {
        fail(s"Field ${fieldDesc} was null but expected '$expected'")
      }

      val utf8String = row.getUTF8String(ordinal)
      if (utf8String == null) {
        fail(s"Field ${fieldDesc} UTF8String was null but expected '$expected'")
      }

      val actualStr = utf8String.toString
      withClue(s"Field ${fieldDesc}: ") {
        actualStr should equal(expected)
      }
    }

    private def assertIntField(expected: Int): Assertion = {
      if (row.isNullAt(ordinal)) {
        fail(s"Field ${fieldDesc} was null but expected $expected")
      }
      val actual = row.getInt(ordinal)
      withClue(s"Field ${fieldDesc}: ") {
        actual should equal(expected)
      }
    }

    private def assertLongField(expected: Long): Assertion = {
      if (row.isNullAt(ordinal)) {
        fail(s"Field ${fieldDesc} was null but expected $expected")
      }
      val actual = row.getLong(ordinal)
      withClue(s"Field ${fieldDesc}: ") {
        actual should equal(expected)
      }
    }

    private def assertFloatField(expected: Float): Assertion = {
      if (row.isNullAt(ordinal)) {
        fail(s"Field ${fieldDesc} was null but expected $expected")
      }
      val actual = row.getFloat(ordinal)
      withClue(s"Field ${fieldDesc}: ") {
        actual should equal(expected +- 0.0001f)
      }
    }

    private def assertDoubleField(expected: Double): Assertion = {
      if (row.isNullAt(ordinal)) {
        fail(s"Field ${fieldDesc} was null but expected $expected")
      }
      val actual = row.getDouble(ordinal)
      withClue(s"Field ${fieldDesc}: ") {
        actual should equal(expected +- 0.0001)
      }
    }

    private def assertBooleanField(expected: Boolean): Assertion = {
      if (row.isNullAt(ordinal)) {
        fail(s"Field ${fieldDesc} was null but expected $expected")
      }
      val actual = row.getBoolean(ordinal)
      withClue(s"Field ${fieldDesc}: ") {
        actual should equal(expected)
      }
    }

    private def assertNull(): Assertion = {
      if (!row.isNullAt(ordinal)) {
        val actualValue = try {
          val utf8 = row.getUTF8String(ordinal)
          if (utf8 != null) s"'${utf8.toString}'" else "non-null but UTF8String is null"
        } catch {
          case _: Exception => "non-null (unable to extract value)"
        }
        fail(s"Field ${fieldDesc} was $actualValue but expected null")
      }
      succeed
    }

    private def fieldDesc: String = {
      if (fieldName.nonEmpty) s"'$fieldName' (ordinal $ordinal)" else s"at ordinal $ordinal"
    }
  }

  /**
   * Matcher for struct/nested row assertions.
   */
  case class StructMatcher(row: InternalRow, ordinal: Int, fieldName: String = "") {
    def field(nestedOrdinal: Int): FieldMatcher = {
      if (row.isNullAt(ordinal)) {
        fail(s"Cannot access nested field: struct ${fieldDesc} is null")
      }
      val struct = row.getStruct(ordinal, -1) // -1 means infer size
      FieldMatcher(struct, nestedOrdinal, s"${fieldName}.nested[$nestedOrdinal]")
    }

    def field(nestedOrdinal: Int, nestedFieldName: String): FieldMatcher = {
      if (row.isNullAt(ordinal)) {
        fail(s"Cannot access nested field: struct ${fieldDesc} is null")
      }
      val struct = row.getStruct(ordinal, -1)
      FieldMatcher(struct, nestedOrdinal, s"${fieldName}.$nestedFieldName")
    }

    private def fieldDesc: String = {
      if (fieldName.nonEmpty) s"'$fieldName' (ordinal $ordinal)" else s"at ordinal $ordinal"
    }
  }

  /**
   * Extension methods for InternalRow to provide fluent assertion API.
   */
  implicit class InternalRowOps(row: InternalRow) {
    /**
     * Create a field matcher for the specified ordinal.
     */
    def field(ordinal: Int): FieldMatcher = FieldMatcher(row, ordinal)

    /**
     * Create a field matcher with a descriptive name.
     */
    def field(ordinal: Int, name: String): FieldMatcher = FieldMatcher(row, ordinal, name)

    /**
     * Create a struct matcher for nested field access.
     */
    def struct(ordinal: Int): StructMatcher = StructMatcher(row, ordinal)

    /**
     * Create a struct matcher with a descriptive name.
     */
    def struct(ordinal: Int, name: String): StructMatcher = StructMatcher(row, ordinal, name)

    /**
     * Assert multiple fields at once using ordinal -> value pairs.
     */
    def assertFields(assertions: (Int, Any)*): Unit = {
      assertions.foreach { case (ordinal, expected) =>
        field(ordinal) shouldEqual expected
      }
    }

    /**
     * Get a safe string representation of a field for debugging.
     */
    def getFieldDebugString(ordinal: Int): String = {
      if (row.isNullAt(ordinal)) {
        "null"
      } else {
        try {
          val utf8 = row.getUTF8String(ordinal)
          if (utf8 != null) s"'${utf8.toString}'" else "non-null but UTF8String is null"
        } catch {
          case _: Exception => s"non-null (${row.get(ordinal, org.apache.spark.sql.types.StringType)})"
        }
      }
    }
  }

  /**
   * Helper method to safely extract string value or return null.
   */
  def safeGetString(row: InternalRow, ordinal: Int): String = {
    if (row.isNullAt(ordinal)) {
      null
    } else {
      val utf8 = row.getUTF8String(ordinal)
      if (utf8 != null) utf8.toString else null
    }
  }
}