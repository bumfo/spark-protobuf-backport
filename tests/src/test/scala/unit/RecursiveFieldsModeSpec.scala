package unit

import com.google.protobuf.Descriptors.Descriptor
import fastproto.{RecursionMode, RecursiveSchemaConverters}
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
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor, recursiveFieldsMode = RecursionMode.MockAsBinary, recursiveFieldMaxDepth = -1,
          allowRecursion = false, enumAsInt = true).asInstanceOf[StructType]

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
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor, recursiveFieldsMode = RecursionMode.Drop, recursiveFieldMaxDepth = -1,
          allowRecursion = false, enumAsInt = true).asInstanceOf[StructType]

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
        val schemaA = RecursiveSchemaConverters.toSqlType(
          descriptorA, recursiveFieldsMode = RecursionMode.MockAsBinary, recursiveFieldMaxDepth = -1,
          allowRecursion = false, enumAsInt = true).asInstanceOf[StructType]

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
        val schemaA = RecursiveSchemaConverters.toSqlType(
          descriptorA, recursiveFieldsMode = RecursionMode.Drop, recursiveFieldMaxDepth = -1,
          allowRecursion = false, enumAsInt = true).asInstanceOf[StructType]

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

      it("depth=3 with mutual recursion A↔B (tests depth counting through different types)") {
        val schemaA = RecursiveSchemaConverters.toSqlType(
          descriptorA,
          recursiveFieldsMode = RecursionMode.Drop,
          recursiveFieldMaxDepth = 3,
          allowRecursion = true,
          enumAsInt = true
        ).asInstanceOf[StructType]

        println("\n=== Testing depth=3 with A↔B (depth=1 at first recursion) ===")

        // Root A (depth 0)
        println("Root A (depth 0):")
        println(s"  Fields: ${schemaA.fieldNames.mkString(", ")}")
        schemaA.fieldNames should contain allOf ("name", "value_a", "b_field", "b_list")

        // First B (depth 0, not recursive)
        val bField = schemaA("b_field")
        bField.dataType shouldBe a[StructType]
        val bSchema = bField.dataType.asInstanceOf[StructType]
        println("First B (depth 0, not recursive):")
        println(s"  Fields: ${bSchema.fieldNames.mkString(", ")}")
        bSchema.fieldNames should contain allOf ("label", "value_b", "a_field", "a_list")

        // Second A (recursion: depth=0+1=1, check 1>3 false, continue)
        val aFieldInB = bSchema("a_field")
        aFieldInB.dataType shouldBe a[StructType]
        val aSchemaInB = aFieldInB.dataType.asInstanceOf[StructType]
        println("Second A (depth 1, first recursion):")
        println(s"  Fields: ${aSchemaInB.fieldNames.mkString(", ")}")
        aSchemaInB.fieldNames should contain allOf ("name", "value_a", "b_field", "b_list")

        // Second B (recursion: depth=1+1=2, check 2>3 false, continue)
        val secondBField = aSchemaInB("b_field")
        secondBField.dataType shouldBe a[StructType]
        val secondBSchema = secondBField.dataType.asInstanceOf[StructType]
        println("Second B (depth 2):")
        println(s"  Fields: ${secondBSchema.fieldNames.mkString(", ")}")
        secondBSchema.fieldNames should contain allOf ("label", "value_b", "a_field", "a_list")

        // Third A (recursion: depth=2+1=3, check 3>3 false, continue)
        val secondAFieldInB = secondBSchema("a_field")
        secondAFieldInB.dataType shouldBe a[StructType]
        val secondASchemaInB = secondAFieldInB.dataType.asInstanceOf[StructType]
        println("Third A (depth 3):")
        println(s"  Fields: ${secondASchemaInB.fieldNames.mkString(", ")}")
        secondASchemaInB.fieldNames should contain allOf ("name", "value_a")
        // Third B would recurse: depth=3+1=4, check 4>3 true, DROPPED
        secondASchemaInB.fieldNames should not contain "b_field"
        secondASchemaInB.fieldNames should not contain "b_list"

        println("Result: A→B→A→B→A(primitives only) - allows recursions at depth 1,2,3")
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
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor, recursiveFieldsMode = RecursionMode.MockAsBinary, recursiveFieldMaxDepth = -1,
          allowRecursion = false, enumAsInt = true).asInstanceOf[StructType]

        val simpleStr = schema.simpleString
        simpleStr should include("binary")
      }

      it("mode 'drop' should have minimal schema") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor, recursiveFieldsMode = RecursionMode.Drop, recursiveFieldMaxDepth = -1,
          allowRecursion = false, enumAsInt = true).asInstanceOf[StructType]

        // Should only have non-recursive fields
        val simpleStr = schema.simpleString
        simpleStr should include("id")
        simpleStr should include("depth")
        simpleStr should not include "child"
        simpleStr should not include "children"
      }
    }

    describe("Unified toSqlType() with Spark-aligned depth semantics") {
      val descriptor: Descriptor = NestedProtos.Recursive.getDescriptor

      it("mode='' + depth=-1 + allowRecursion=false should fail on recursion (Spark default)") {
        val thrown = intercept[IllegalArgumentException] {
          RecursiveSchemaConverters.toSqlType(
            descriptor,
            recursiveFieldsMode = null,
            recursiveFieldMaxDepth = -1,
            allowRecursion = false,  // Non-WireFormat parser
            enumAsInt = true
          )
        }
        thrown.getMessage should include("Recursive field")
      }

      it("mode='' + depth=-1 + allowRecursion=true should produce RecursiveStructType (WireFormat default)") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor,
          recursiveFieldsMode = null,
          recursiveFieldMaxDepth = -1,
          allowRecursion = true,  // WireFormat parser
          enumAsInt = true
        )

        // Should be RecursiveStructType (WireFormat allows unlimited recursion by default)
        schema shouldBe a[RecursiveStructType]
        schema.asInstanceOf[RecursiveStructType].fieldNames should contain allOf ("id", "depth", "child", "children")

        // Child field should be RecursiveStructType
        val childField = schema.asInstanceOf[RecursiveStructType]("child")
        childField.dataType shouldBe a[RecursiveStructType]
      }

      it("mode='recursive' + depth=-1 should produce RecursiveStructType (explicit override)") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor,
          recursiveFieldsMode = RecursionMode.AllowRecursive,
          recursiveFieldMaxDepth = -1,
          allowRecursion = false,  // Even non-WireFormat parser can be overridden
          enumAsInt = true
        )

        // Should be RecursiveStructType (explicit mode overrides allowRecursion)
        schema shouldBe a[RecursiveStructType]
        schema.asInstanceOf[RecursiveStructType].fieldNames should contain allOf ("id", "depth", "child", "children")
      }

      it("mode='' + depth=0 should drop all recursive fields (no recursions allowed)") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor,
          recursiveFieldsMode = null,
          recursiveFieldMaxDepth = 0,
          allowRecursion = true,
          enumAsInt = true
        ).asInstanceOf[StructType]

        // depth=0 means no recursive fields allowed (drop on first recursion)
        schema shouldBe a[StructType]
        schema should not be a[RecursiveStructType]

        // Should have non-recursive fields
        schema.fieldNames should contain allOf ("id", "depth")

        // Recursive fields should be dropped
        schema.fieldNames should not contain "child"
        schema.fieldNames should not contain "children"
      }

      it("mode='' + depth=1 should allow 1 recursion") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor,
          recursiveFieldsMode = null,
          recursiveFieldMaxDepth = 1,
          allowRecursion = true,
          enumAsInt = true
        ).asInstanceOf[StructType]

        // depth=1: first recursion at depth 1, drop when depth > 1
        // Root A (depth 0) → A.child (recursion: depth 1) → A.child.child (depth 2 > 1, DROP)
        schema shouldBe a[StructType]
        schema should not be a[RecursiveStructType]

        // Root should have recursive fields
        schema.fieldNames should contain allOf ("id", "depth", "child", "children")

        // First child (at depth 1) should have only primitives
        val childField = schema("child")
        childField.dataType shouldBe a[StructType]
        val childSchema = childField.dataType.asInstanceOf[StructType]
        childSchema.fieldNames should contain allOf ("id", "depth")
        childSchema.fieldNames should not contain "child"  // Second recursion at depth 2 > 1, DROPPED
        childSchema.fieldNames should not contain "children"
      }

      it("mode='' + depth=2 should allow 2 recursions") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor,
          recursiveFieldsMode = null,
          recursiveFieldMaxDepth = 2,
          allowRecursion = true,
          enumAsInt = true
        ).asInstanceOf[StructType]

        // depth=2: allows recursions at depth 1 and 2, drops when depth > 2
        // Root A (depth 0) → A.child (depth 1) → A.child.child (depth 2) → drop (depth 3 > 2)
        schema shouldBe a[StructType]
        schema should not be a[RecursiveStructType]
        schema.fieldNames should contain allOf ("id", "depth", "child", "children")

        // First child (at depth 1) should have recursive fields
        val childField = schema("child")
        childField.dataType shouldBe a[StructType]
        val childSchema = childField.dataType.asInstanceOf[StructType]
        childSchema.fieldNames should contain allOf ("id", "depth", "child", "children")

        // Second child (at depth 2) should have only primitives
        val nestedChildField = childSchema("child")
        nestedChildField.dataType shouldBe a[StructType]
        val nestedChildSchema = nestedChildField.dataType.asInstanceOf[StructType]
        nestedChildSchema.fieldNames should contain allOf ("id", "depth")
        nestedChildSchema.fieldNames should not contain "child"  // Third recursion at depth 3 > 2, DROPPED
        nestedChildSchema.fieldNames should not contain "children"
      }

      it("mode='' + depth=3 should allow 3 recursions") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor,
          recursiveFieldsMode = null,
          recursiveFieldMaxDepth = 3,
          allowRecursion = true,
          enumAsInt = true
        ).asInstanceOf[StructType]

        // depth=3: allows recursions at depth 1, 2, and 3, drops when depth > 3
        // Root A (depth 0) → A.child (depth 1) → ...child (depth 2) → ...child (depth 3) → drop (depth 4 > 3)
        schema shouldBe a[StructType]
        schema should not be a[RecursiveStructType]
        schema.fieldNames should contain allOf ("id", "depth", "child", "children")

        // First child (at depth 1)
        val childField = schema("child")
        childField.dataType shouldBe a[StructType]
        val childSchema = childField.dataType.asInstanceOf[StructType]
        childSchema.fieldNames should contain allOf ("id", "depth", "child", "children")

        // Second child (at depth 2)
        val nestedChildField = childSchema("child")
        nestedChildField.dataType shouldBe a[StructType]
        val nestedChildSchema = nestedChildField.dataType.asInstanceOf[StructType]
        nestedChildSchema.fieldNames should contain allOf ("id", "depth", "child", "children")

        // Third child (at depth 3) should have only primitives
        val thirdChildField = nestedChildSchema("child")
        thirdChildField.dataType shouldBe a[StructType]
        val thirdChildSchema = thirdChildField.dataType.asInstanceOf[StructType]
        thirdChildSchema.fieldNames should contain allOf ("id", "depth")
        thirdChildSchema.fieldNames should not contain "child"  // Fourth recursion at depth 4 > 3, DROPPED
        thirdChildSchema.fieldNames should not contain "children"
      }

      it("mode='binary' + maxDepth=0 should mock recursive fields as BinaryType") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor,
          recursiveFieldsMode = RecursionMode.MockAsBinary,
          recursiveFieldMaxDepth = 0, // depth=0: no recursions, mock on first recursion
          allowRecursion = true,
          enumAsInt = true
        ).asInstanceOf[StructType]

        // Should be regular StructType
        schema shouldBe a[StructType]
        schema should not be a[RecursiveStructType]
        schema.fieldNames should contain allOf ("id", "depth", "child", "children")

        // Child field should be BinaryType (mocked at first recursion)
        val childField = schema("child")
        childField.dataType shouldBe BinaryType
      }

      it("mode='drop' + maxDepth=0 should drop recursive fields") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor,
          recursiveFieldsMode = RecursionMode.Drop,
          recursiveFieldMaxDepth = 0, // depth=0: no recursions, drop on first recursion
          allowRecursion = true,
          enumAsInt = true
        ).asInstanceOf[StructType]

        // Should be regular StructType
        schema shouldBe a[StructType]
        schema should not be a[RecursiveStructType]

        // Should have non-recursive fields
        schema.fieldNames should contain allOf ("id", "depth")

        // Should NOT have recursive fields (maxDepth ignored)
        schema.fieldNames should not contain "child"
        schema.fieldNames should not contain "children"
      }

      it("mode='drop' + depth=-1 should drop on first recursion (same as depth=0)") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor,
          recursiveFieldsMode = RecursionMode.Drop,
          recursiveFieldMaxDepth = -1,
          allowRecursion = true,
          enumAsInt = true
        ).asInstanceOf[StructType]

        // depth=-1 + mode="drop" → drop on first recursion (same as depth=0)
        schema shouldBe a[StructType]
        schema should not be a[RecursiveStructType]

        // Should have non-recursive fields
        schema.fieldNames should contain allOf ("id", "depth")

        // Recursive fields should be dropped immediately
        schema.fieldNames should not contain "child"
        schema.fieldNames should not contain "children"
      }

      it("mode='binary' + depth=-1 should mock on first recursion (same as depth=0)") {
        val schema = RecursiveSchemaConverters.toSqlType(
          descriptor,
          recursiveFieldsMode = RecursionMode.MockAsBinary,
          recursiveFieldMaxDepth = -1,
          allowRecursion = true,
          enumAsInt = true
        ).asInstanceOf[StructType]

        // depth=-1 + mode="binary" → mock on first recursion (same as depth=0)
        schema shouldBe a[StructType]
        schema should not be a[RecursiveStructType]
        schema.fieldNames should contain allOf ("id", "depth", "child", "children")

        // Child field should be BinaryType (mocked at first recursion)
        val childField = schema("child")
        childField.dataType shouldBe BinaryType

        // Children field should be ArrayType(BinaryType)
        val childrenField = schema("children")
        childrenField.dataType shouldBe a[ArrayType]
        childrenField.dataType.asInstanceOf[ArrayType].elementType shouldBe BinaryType
      }

      it("mode='fail' + depth=-1 should fail on first recursion (same as depth=0)") {
        val thrown = intercept[IllegalArgumentException] {
          RecursiveSchemaConverters.toSqlType(
            descriptor,
            recursiveFieldsMode = RecursionMode.Fail,
            recursiveFieldMaxDepth = -1,
            allowRecursion = true,
            enumAsInt = true
          )
        }

        // depth=-1 + mode="fail" → fail on first recursion (same as depth=0)
        thrown.getMessage should include("Recursive field")
      }
    }
  }
}
