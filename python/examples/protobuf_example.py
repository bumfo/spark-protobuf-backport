#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
PySpark Protobuf Backport Usage Examples

This script demonstrates how to use the protobuf functions backported to PySpark 3.2.1.
It shows three different ways to work with protobuf data:

1. Using compiled protobuf classes (recommended for performance)
2. Using protobuf descriptor files  
3. Using binary descriptor sets

Prerequisites:
- Build the backport JAR: sbt assembly
- Install the Python package: pip install -e python/
- Have protobuf descriptor files or shaded JAR with compiled classes
"""

import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.protobuf.functions import from_protobuf, to_protobuf


def create_spark_session_with_protobuf_jar():
    """
    Create a Spark session with the protobuf backport JAR.
    
    In a real application, you would specify the JAR path or use spark-submit --jars.
    """
    # Path to the assembled JAR - adjust as needed
    jar_path = "../shaded/target/scala-2.12/spark-protobuf-backport-shaded-0.1.0-SNAPSHOT.jar"
    
    return SparkSession.builder \
        .appName("PySpark Protobuf Backport Example") \
        .master("local[2]") \
        .config("spark.jars", jar_path) \
        .getOrCreate()


def example_with_descriptor_file():
    """Example using protobuf descriptor files."""
    print("=== Example 1: Using Protobuf Descriptor File ===")
    
    spark = create_spark_session_with_protobuf_jar()
    
    try:
        # Sample data - in practice this would be your actual protobuf binary data
        data = [
            ("user1", b"\x08\x02\x12\x05Alice\x18\x90\xd5\x06"),  # Example protobuf bytes
            ("user2", b"\x08\x03\x12\x03Bob\x18\xa0\xd5\x06"),
        ]
        
        # Create DataFrame with binary protobuf data
        df = spark.createDataFrame(data, ["id", "protobuf_data"])
        df.show(truncate=False)
        
        # Convert from protobuf using descriptor file
        # Note: You need to create this descriptor file using protoc
        desc_file_path = "/path/to/your/descriptor.desc"
        message_name = "YourMessageType"
        
        # This would work if you have a real descriptor file:
        # result_df = df.select(
        #     df.id,
        #     from_protobuf(df.protobuf_data, message_name, desc_file_path).alias("decoded_data")
        # )
        # result_df.show()
        
        print("To use descriptor files, create them with:")
        print(f"protoc --include_imports --descriptor_set_out={desc_file_path} your_proto_file.proto")
        
    finally:
        spark.stop()


def example_with_compiled_class():
    """Example using compiled protobuf classes (recommended approach)."""
    print("\n=== Example 2: Using Compiled Protobuf Classes ===")
    
    spark = create_spark_session_with_protobuf_jar()
    
    try:
        # Sample structured data that represents what would be in your protobuf message
        data = [
            ("user1", (25, "Alice", 95000)),
            ("user2", (30, "Bob", 87000)),
            ("user3", (28, "Charlie", 92000)),
        ]
        
        # Create DataFrame
        schema = "id STRING, person STRUCT<age: INT, name: STRING, salary: LONG>"
        df = spark.createDataFrame(data, schema)
        df.show()
        
        # Convert to protobuf using compiled class name
        # This assumes you have a shaded protobuf class available
        message_class_name = "com.example.Person"  # Your actual protobuf class
        
        # This would work if you have compiled protobuf classes in your JAR:
        # protobuf_df = df.select(
        #     df.id,
        #     to_protobuf(df.person, message_class_name).alias("protobuf_bytes")
        # )
        # protobuf_df.show()
        
        # Convert back from protobuf
        # decoded_df = protobuf_df.select(
        #     protobuf_df.id,
        #     from_protobuf(protobuf_df.protobuf_bytes, message_class_name).alias("person")
        # )
        # decoded_df.show()
        
        print("To use compiled classes, ensure your protobuf classes are shaded:")
        print("com.google.protobuf.* -> org.sparkproject.spark.protobuf311.*")
        
    finally:
        spark.stop()


def example_with_binary_descriptor():
    """Example using binary descriptor sets."""
    print("\n=== Example 3: Using Binary Descriptor Sets ===")
    
    spark = create_spark_session_with_protobuf_jar()
    
    try:
        # Sample data
        data = [("record1", b"\x08\x01\x12\x04test")]
        df = spark.createDataFrame(data, ["id", "proto_data"])
        
        # In a real scenario, you would read the descriptor bytes from a file
        # created by protoc --descriptor_set_out=file.desc
        
        # Example of how you would use it:
        with tempfile.NamedTemporaryFile() as tmp_file:
            # This is just example bytes - use real descriptor file content
            example_descriptor_bytes = b"\x08\x01\x12\x04test"  
            
            # Convert from protobuf using binary descriptor
            # result_df = df.select(
            #     df.id,
            #     from_protobuf(
            #         df.proto_data, 
            #         "YourMessageType",
            #         binaryDescriptorSet=example_descriptor_bytes
            #     ).alias("decoded")
            # )
            
            print("Binary descriptor sets are useful when descriptor files")
            print("are not available on executor nodes.")
        
    finally:
        spark.stop()


def example_with_options():
    """Example showing how to pass options to protobuf functions."""
    print("\n=== Example 4: Using Options ===")
    
    spark = create_spark_session_with_protobuf_jar()
    
    try:
        # Sample data
        data = [("record1", (1, "test"))]
        df = spark.createDataFrame(data, ["id", "data STRUCT<num: INT, text: STRING>"])
        
        # Options for protobuf parsing
        options = {
            "recursive.fields.max.depth": "10",  # Maximum recursion depth
            "parser.mode": "PERMISSIVE"          # Parse mode: PERMISSIVE or FAILFAST
        }
        
        # Use options with from_protobuf
        message_class = "com.example.TestMessage"
        
        # This would work with real protobuf classes:
        # result_df = df.select(
        #     from_protobuf(df.data, message_class, options=options).alias("parsed")
        # )
        
        print("Available options:")
        print("- recursive.fields.max.depth: Control recursion depth for nested messages")
        print("- parser.mode: PERMISSIVE (return null on error) or FAILFAST (throw exception)")
        
    finally:
        spark.stop()


def main():
    """Run all examples."""
    print("PySpark Protobuf Backport Examples")
    print("=" * 50)
    
    example_with_descriptor_file()
    example_with_compiled_class()
    example_with_binary_descriptor()
    example_with_options()
    
    print("\n" + "=" * 50)
    print("Examples completed!")
    print("\nTo use in your own applications:")
    print("1. Build the JAR: sbt assembly")
    print("2. Install Python package: pip install -e python/")
    print("3. Add JAR to your Spark session")
    print("4. Import: from pyspark.sql.protobuf.functions import from_protobuf, to_protobuf")


if __name__ == "__main__":
    main()