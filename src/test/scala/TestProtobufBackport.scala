/*
 * Minimal test harness for the spark‑protobuf‑backport project.
 *
 * This program exercises the backported Protobuf functions using both a
 * compiled Java class and a descriptor file.  It expects that a
 * Protobuf message `Person` has been compiled into the `example.simple`
 * package and that a descriptor set containing this message has been
 * generated via protoc (see README for details).  The descriptor file
 * path should be supplied as the first command line argument when
 * running the program.
 *
 * To execute the test locally:
 *
 *  1. Compile the backport project and assemble the shaded jar.
 *  2. Compile the example Protobuf schema (Person) to Java and include
 *     the resulting classes on the classpath.
 *  3. Generate a descriptor file using `protoc --descriptor_set_out` and
 *     pass its path as an argument to this program.
 *  4. Run the program with a Spark driver, for example using
 *     `spark-submit --class TestProtobufBackport --master local[*] ...`.
 *
 * The test will construct a Person message, serialize it to bytes, and
 * use the backported `from_protobuf` function both with the class
 * reference and with the descriptor file.  It asserts that the
 * resulting rows match the original data.  If all assertions pass
 * "All tests passed" will be printed to stdout.
 */

package org.apache.spark.sql.protobuf.backport

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.protobuf.backport.functions

object TestProtobufBackport {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: TestProtobufBackport <descriptor_file>")
      System.exit(1)
    }
    val descriptorPath = args(0)
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TestProtobufBackport")
      .getOrCreate()
    import spark.implicits._

    // Construct an example Person message using the compiled Java class.
    // The Person class must be generated from the following proto:
    // syntax = "proto3";
    // package example.simple;
    // message Person {
    //   string name = 1;
    //   int32 id = 2;
    //   repeated string emails = 3;
    // }
    val person = example.simple.Person.newBuilder()
      .setName("Alice")
      .setId(1)
      .addEmails("alice@example.com")
      .build()
    val df = Seq(person.toByteArray).toDF("data")

    // Use the compiled Java class for schema inference.
    val structDf = df.select(
      functions.from_protobuf($"data", classOf[example.simple.Person].getName).as("struct")
    )
    val row1 = structDf.select("struct.*").as[(String, Int, Seq[String])].head()
    assert(row1 == ("Alice", 1, Seq("alice@example.com")), s"Expected (Alice,1,[alice@example.com]) but got $row1")

    // Use the descriptor file for schema inference.  Broadcast the descriptor
    // to executors using SparkContext.addFile so that it is available during
    // task execution.
    spark.sparkContext.addFile(descriptorPath)
    val fileName = descriptorPath.split("/").last
    val structDf2 = df.select(
      functions.from_protobuf(
        $"data",
        "example.simple.Person",
        SparkFiles.get(fileName)
      ).as("struct")
    )
    val row2 = structDf2.select("struct.*").as[(String, Int, Seq[String])].head()
    assert(row2 == ("Alice", 1, Seq("alice@example.com")), s"Expected (Alice,1,[alice@example.com]) but got $row2")

    // Alternatively, read the descriptor file on the driver and pass its
    // contents directly as a binary descriptor set.  This avoids the need
    // to broadcast the file to executors and exercises the new API.  Note that
    // this requires java.nio.file.Files to be available on the driver.
    val descBytes = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(descriptorPath))
    val structDf3 = df.select(
      functions.from_protobuf($"data", "example.simple.Person", descBytes).as("struct")
    )
    val row3 = structDf3.select("struct.*").as[(String, Int, Seq[String])].head()
    assert(row3 == ("Alice", 1, Seq("alice@example.com")), s"Expected (Alice,1,[alice@example.com]) but got $row3")

    println("All tests passed")
    spark.stop()
  }
}