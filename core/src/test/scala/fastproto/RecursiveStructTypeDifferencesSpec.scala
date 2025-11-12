package fastproto

import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RecursiveStructTypeDifferencesSpec extends AnyFunSpec with Matchers {

  describe("RecursiveStructType vs StructType (no recursion)") {
    val fields = Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("nested", StructType(Array(
        StructField("value", LongType, nullable = true)
      )), nullable = true)
    )

    val structType = StructType(fields)
    val recursiveStructType = new RecursiveStructType(fields.clone(), "TestMessage")

    ignore("should have same toString") {
      // Intentionally different: RecursiveStructType uses simpleString to prevent infinite loops
      println(s"StructType.toString: ${structType.toString}")
      println(s"RecursiveStructType.toString: ${recursiveStructType.toString}")
      recursiveStructType.toString shouldEqual structType.toString
    }

    it("should have same simpleString") {
      println(s"StructType.simpleString: ${structType.simpleString}")
      println(s"RecursiveStructType.simpleString: ${recursiveStructType.simpleString}")
      recursiveStructType.simpleString shouldEqual structType.simpleString
    }

    it("should have same catalogString") {
      println(s"StructType.catalogString: ${structType.catalogString}")
      println(s"RecursiveStructType.catalogString: ${recursiveStructType.catalogString}")
      recursiveStructType.catalogString shouldEqual structType.catalogString
    }

    it("should have same sql") {
      println(s"StructType.sql: ${structType.sql}")
      println(s"RecursiveStructType.sql: ${recursiveStructType.sql}")
      recursiveStructType.sql shouldEqual structType.sql
    }

    ignore("should have same hashCode") {
      // TODO: Fix hashCode to satisfy equals/hashCode contract
      println(s"StructType.hashCode: ${structType.hashCode}")
      println(s"RecursiveStructType.hashCode: ${recursiveStructType.hashCode}")
      recursiveStructType.hashCode shouldEqual structType.hashCode
    }

    it("should be equal via equals") {
      println(s"structType == recursiveStructType: ${structType == recursiveStructType}")
      println(s"recursiveStructType == structType: ${recursiveStructType == structType}")
      recursiveStructType shouldEqual structType
    }

    it("should have same json serialization") {
      println(s"StructType.json: ${structType.json}")
      println(s"RecursiveStructType.json: ${recursiveStructType.json}")
      recursiveStructType.json shouldEqual structType.json
    }
  }
}
