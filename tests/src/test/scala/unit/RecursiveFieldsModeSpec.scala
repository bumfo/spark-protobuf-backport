package unit

import com.google.protobuf.Descriptors.Descriptor
import fastproto.RecursiveSchemaConverters
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import testproto.NestedProtos

class RecursiveFieldsModeSpec extends AnyFunSpec with Matchers {

  describe("RecursiveSchemaConverters") {

    describe("Self-recursion (Recursive message)") {
      val descriptor: Descriptor = NestedProtos.Recursive.getDescriptor

      it("mode 'struct' should use RecursiveStructType") {
        val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

        // Schema should be RecursiveStructType
        schema shouldBe a[RecursiveStructType]
        schema.fieldNames should contain allOf ("id", "depth", "child", "children")

        // Child field should be RecursiveStructType (self-reference)
        val childField = schema("child")
        childField.dataType shouldBe a[RecursiveStructType]
        childField.dataType.asInstanceOf[RecursiveStructType].fieldNames should contain allOf ("id", "depth", "child", "children")

        // Children field should be ArrayType(RecursiveStructType)
        val childrenField = schema("children")
        childrenField.dataType shouldBe a[ArrayType]
        val arrayType = childrenField.dataType.asInstanceOf[ArrayType]
        arrayType.elementType shouldBe a[RecursiveStructType]
        arrayType.elementType.asInstanceOf[RecursiveStructType].fieldNames should contain allOf ("id", "depth", "child", "children")
      }

      it("mode 'binary' should mock recursive fields as BinaryType") {
        val schema = RecursiveSchemaConverters.toSqlTypeWithRecursionMocking(descriptor, enumAsInt = true).asInstanceOf[StructType]

        // Schema should be regular StructType
        schema shouldBe a[StructType]
        schema should not be a[RecursiveStructType]
        schema.fieldNames should contain allOf ("id", "depth", "child", "children")

        // Child field should be BinaryType (mocked)
        val childField = schema("child")
        childField.dataType shouldBe BinaryType

        // Children field should be ArrayType(BinaryType)
        val childrenField = schema("children")
        childrenField.dataType shouldBe a[ArrayType]
        childrenField.dataType.asInstanceOf[ArrayType].elementType shouldBe BinaryType
      }

      it("mode 'drop' should omit recursive fields") {
        val schema = RecursiveSchemaConverters.toSqlTypeWithRecursionDropping(descriptor, enumAsInt = true).asInstanceOf[StructType]

        // Schema should be regular StructType
        schema shouldBe a[StructType]
        schema should not be a[RecursiveStructType]

        // Should have non-recursive fields
        schema.fieldNames should contain allOf ("id", "depth")

        // Should NOT have recursive fields
        schema.fieldNames should not contain "child"
        schema.fieldNames should not contain "children"
      }
    }

    describe("Mutual recursion (MutualA <-> MutualB)") {
      val descriptorA: Descriptor = NestedProtos.MutualA.getDescriptor
      val descriptorB: Descriptor = NestedProtos.MutualB.getDescriptor

      it("mode 'struct' should use RecursiveStructType for both") {
        val schemaA = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptorA, enumAsInt = true)

        // Schema A should be RecursiveStructType
        schemaA shouldBe a[RecursiveStructType]
        schemaA.fieldNames should contain allOf ("name", "value_a", "b_field", "b_list")

        // b_field should reference MutualB as RecursiveStructType
        val bField = schemaA("b_field")
        bField.dataType shouldBe a[RecursiveStructType]
        val bSchema = bField.dataType.asInstanceOf[RecursiveStructType]
        bSchema.fieldNames should contain allOf ("label", "value_b", "a_field", "a_list")

        // b_field.a_field should reference back to MutualA (circular)
        val aFieldInB = bSchema("a_field")
        aFieldInB.dataType shouldBe a[RecursiveStructType]

        // b_list should be ArrayType(RecursiveStructType)
        val bListField = schemaA("b_list")
        bListField.dataType shouldBe a[ArrayType]
        bListField.dataType.asInstanceOf[ArrayType].elementType shouldBe a[RecursiveStructType]
      }

      it("mode 'binary' should mock recursive fields as BinaryType") {
        val schemaA = RecursiveSchemaConverters.toSqlTypeWithRecursionMocking(descriptorA, enumAsInt = true).asInstanceOf[StructType]

        // Schema A should be regular StructType
        schemaA shouldBe a[StructType]
        schemaA should not be a[RecursiveStructType]
        schemaA.fieldNames should contain allOf ("name", "value_a", "b_field", "b_list")

        // b_field should contain a struct with a_field as BinaryType (because of recursion back to A)
        val bField = schemaA("b_field")
        bField.dataType shouldBe a[StructType]
        val bSchema = bField.dataType.asInstanceOf[StructType]
        bSchema.fieldNames should contain allOf ("label", "value_b", "a_field", "a_list")

        // a_field in MutualB should be BinaryType (recursive reference)
        val aFieldInB = bSchema("a_field")
        aFieldInB.dataType shouldBe BinaryType

        // a_list in MutualB should be ArrayType(BinaryType)
        val aListInB = bSchema("a_list")
        aListInB.dataType shouldBe a[ArrayType]
        aListInB.dataType.asInstanceOf[ArrayType].elementType shouldBe BinaryType

        // b_list should be ArrayType(StructType with a_field as BinaryType)
        val bListField = schemaA("b_list")
        bListField.dataType shouldBe a[ArrayType]
        val bListElementType = bListField.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
        bListElementType("a_field").dataType shouldBe BinaryType
      }

      it("mode 'drop' should omit recursive fields") {
        val schemaA = RecursiveSchemaConverters.toSqlTypeWithRecursionDropping(descriptorA, enumAsInt = true).asInstanceOf[StructType]

        // Schema A should be regular StructType
        schemaA shouldBe a[StructType]
        schemaA should not be a[RecursiveStructType]
        schemaA.fieldNames should contain allOf ("name", "value_a", "b_field", "b_list")

        // b_field should contain a struct without recursive fields
        val bField = schemaA("b_field")
        bField.dataType shouldBe a[StructType]
        val bSchema = bField.dataType.asInstanceOf[StructType]

        // MutualB should have non-recursive fields
        bSchema.fieldNames should contain allOf ("label", "value_b")

        // MutualB should NOT have recursive fields (a_field, a_list)
        bSchema.fieldNames should not contain "a_field"
        bSchema.fieldNames should not contain "a_list"

        // b_list should be ArrayType(StructType without recursive fields)
        val bListField = schemaA("b_list")
        bListField.dataType shouldBe a[ArrayType]
        val bListElementType = bListField.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
        bListElementType.fieldNames should contain allOf ("label", "value_b")
        bListElementType.fieldNames should not contain "a_field"
        bListElementType.fieldNames should not contain "a_list"
      }
    }

    describe("Schema string representations") {
      val descriptor: Descriptor = NestedProtos.Recursive.getDescriptor

      it("mode 'struct' should have recursive markers in string representation") {
        val schema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)

        val simpleStr = schema.simpleString
        // Should indicate recursion in string format
        simpleStr should include("recursive:")
      }

      it("mode 'binary' should show binary types") {
        val schema = RecursiveSchemaConverters.toSqlTypeWithRecursionMocking(descriptor, enumAsInt = true).asInstanceOf[StructType]

        val simpleStr = schema.simpleString
        simpleStr should include("binary")
      }

      it("mode 'drop' should have minimal schema") {
        val schema = RecursiveSchemaConverters.toSqlTypeWithRecursionDropping(descriptor, enumAsInt = true).asInstanceOf[StructType]

        // Should only have non-recursive fields
        val simpleStr = schema.simpleString
        simpleStr should include("id")
        simpleStr should include("depth")
        simpleStr should not include "child"
        simpleStr should not include "children"
      }
    }
  }
}
