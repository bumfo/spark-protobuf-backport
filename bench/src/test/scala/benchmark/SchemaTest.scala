package benchmark

import fastproto.RecursiveSchemaConverters
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Simple test to verify schema generation works without stack overflow.
 */
class SchemaTest extends AnyFunSuite with Matchers {

  test("Schema generation should not cause stack overflow") {
    val shallowDom = DomTestDataGenerator.createDomDocument(maxDepth = 2, breadthFactor = 1)
    val descriptor = shallowDom.getDescriptorForType

    // Test string enum schema
    val stringEnumSchema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = false)
    stringEnumSchema should not be null

    // Test int enum schema
    val intEnumSchema = RecursiveSchemaConverters.toSqlTypeWithTrueRecursion(descriptor, enumAsInt = true)
    intEnumSchema should not be null

    println(s"String enum schema created successfully")
    println(s"Int enum schema created successfully")
  }
}