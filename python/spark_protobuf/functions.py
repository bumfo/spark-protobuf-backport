#!/usr/bin/env python3
"""
PySpark Protobuf Functions Backport

Provides from_protobuf and to_protobuf functions for Spark < 3.4
by wrapping the Scala implementation via JVM bridge.
"""

import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BinaryType


def _check_spark_version():
    """Check if running on appropriate Spark version and warn if not."""
    spark = SparkSession.getActiveSession()
    if spark:
        version = spark.version
        major, minor = map(int, version.split('.')[:2])
        if major > 3 or (major == 3 and minor >= 4):
            warnings.warn(
                f"This protobuf backport is designed for Spark < 3.4, but you're running "
                f"PySpark {version}. Consider using the native protobuf functions in "
                f"Spark 3.4+ instead of this backport.",
                UserWarning,
                stacklevel=3
            )


def from_protobuf(data, messageType, descFilePath=None, options=None, binaryDescriptorSet=None):
    """
    Converts binary column of protobuf format into its corresponding catalyst value.

    This is a backport of Spark 3.4's from_protobuf function for use with Spark < 3.4.

    Args:
        data: Column containing binary protobuf data
        messageType: String name of the protobuf message type
        descFilePath: Path to protobuf descriptor file (optional if binaryDescriptorSet provided)
        options: Dict of parsing options (optional). Supported options:
            - "mode": Parse mode, either "PERMISSIVE" or "FAILFAST" (default: "FAILFAST")
            - "recursive.fields.max.depth": Maximum recursion depth for nested messages (default: "-1" disabled)
            - "recursive.fields.mode": How to handle recursive message types (default: "struct")
                - "struct": Use RecursiveStructType with true circular references
                - "binary": Mock recursive fields as BinaryType
                - "drop": Drop recursive fields from schema entirely
                Note: Only applies to WireFormat parser (binary descriptor set usage)
        binaryDescriptorSet: Binary descriptor set bytes (optional)

    Returns:
        Column with decoded protobuf data as Catalyst struct

    Examples:
        >>> # Using descriptor file
        >>> df.select(from_protobuf(df.data, "Person", "/path/to/person.desc"))

        >>> # Using binary descriptor set
        >>> with open("person.desc", "rb") as f:
        ...     desc_bytes = f.read()
        >>> df.select(from_protobuf(df.data, "Person", binaryDescriptorSet=desc_bytes))

        >>> # With parsing options
        >>> df.select(from_protobuf(df.data, "Person", "/path/to/person.desc",
        ...                        options={"mode": "PERMISSIVE"}))

        >>> # Handling recursive schemas
        >>> df.select(from_protobuf(df.data, "DomNode", binaryDescriptorSet=desc_bytes,
        ...                        options={"recursive.fields.mode": "binary"}))
    """
    _check_spark_version()
    
    spark = SparkSession.getActiveSession()
    if not spark:
        raise RuntimeError("No active SparkSession found")
    
    sc = spark.sparkContext
    
    # Access the Scala functions object
    try:
        backport_functions = getattr(
            sc._jvm, "org.apache.spark.sql.protobuf.backport.functions"
        )
    except AttributeError as e:
        raise RuntimeError(
            "Failed to access Scala functions. Ensure the protobuf backport JAR is loaded."
        ) from e
    
    # Convert options to Java map if provided
    j_options = None
    if options:
        j_options = sc._jvm.scala.collection.immutable.Map.from_(
            sc._jvm.scala.collection.JavaConverters.mapAsScalaMap(options)
        )
    
    # Get the Java column
    j_data = data._jc
    
    # Call appropriate Scala function based on parameters
    if binaryDescriptorSet is not None:
        # Convert bytes to Java byte array
        j_bytes = sc._jvm.PythonUtils.toArray(bytearray(binaryDescriptorSet))
        if j_options:
            j_col = backport_functions.from_protobuf(j_data, messageType, j_bytes, j_options)
        else:
            j_col = backport_functions.from_protobuf(j_data, messageType, j_bytes)
    elif descFilePath is not None:
        if j_options:
            j_col = backport_functions.from_protobuf(j_data, messageType, descFilePath, j_options)
        else:
            j_col = backport_functions.from_protobuf(j_data, messageType, descFilePath)
    else:
        # Try compiled class path
        if j_options:
            j_col = backport_functions.from_protobuf(j_data, messageType, j_options)
        else:
            j_col = backport_functions.from_protobuf(j_data, messageType)
    
    # Wrap Java column back to Python
    from pyspark.sql.column import Column
    return Column(j_col)


def to_protobuf(data, messageType, descFilePath=None, options=None, binaryDescriptorSet=None):
    """
    Converts catalyst column into its binary protobuf representation.
    
    This is a backport of Spark 3.4's to_protobuf function for use with Spark < 3.4.
    
    Args:
        data: Column containing struct data to encode
        messageType: String name of the protobuf message type
        descFilePath: Path to protobuf descriptor file (optional if binaryDescriptorSet provided)  
        options: Dict of serialization options (optional)
        binaryDescriptorSet: Binary descriptor set bytes (optional)
    
    Returns:
        Column with binary protobuf data
        
    Examples:
        >>> # Using descriptor file
        >>> df.select(to_protobuf(df.person_struct, "Person", "/path/to/person.desc"))
        
        >>> # Using binary descriptor set
        >>> with open("person.desc", "rb") as f:
        ...     desc_bytes = f.read()
        >>> df.select(to_protobuf(df.person_struct, "Person", binaryDescriptorSet=desc_bytes))
    """
    _check_spark_version()
    
    spark = SparkSession.getActiveSession()
    if not spark:
        raise RuntimeError("No active SparkSession found")
    
    sc = spark.sparkContext
    
    # Access the Scala functions object
    try:
        backport_functions = getattr(
            sc._jvm, "org.apache.spark.sql.protobuf.backport.functions"
        )
    except AttributeError as e:
        raise RuntimeError(
            "Failed to access Scala functions. Ensure the protobuf backport JAR is loaded."
        ) from e
    
    # Convert options to Java map if provided
    j_options = None
    if options:
        j_options = sc._jvm.scala.collection.immutable.Map.from_(
            sc._jvm.scala.collection.JavaConverters.mapAsScalaMap(options)
        )
    
    # Get the Java column
    j_data = data._jc
    
    # Call appropriate Scala function based on parameters
    if binaryDescriptorSet is not None:
        # Convert bytes to Java byte array
        j_bytes = sc._jvm.PythonUtils.toArray(bytearray(binaryDescriptorSet))
        if j_options:
            j_col = backport_functions.to_protobuf(j_data, messageType, j_bytes, j_options)
        else:
            j_col = backport_functions.to_protobuf(j_data, messageType, j_bytes)
    elif descFilePath is not None:
        if j_options:
            j_col = backport_functions.to_protobuf(j_data, messageType, descFilePath, j_options)
        else:
            j_col = backport_functions.to_protobuf(j_data, messageType, descFilePath)
    else:
        # Try compiled class path
        if j_options:
            j_col = backport_functions.to_protobuf(j_data, messageType, j_options)
        else:
            j_col = backport_functions.to_protobuf(j_data, messageType)
    
    # Wrap Java column back to Python
    from pyspark.sql.column import Column
    return Column(j_col)