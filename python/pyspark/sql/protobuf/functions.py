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
A collection of protobuf functions for PySpark backport to Spark 3.2.1

This module provides from_protobuf and to_protobuf functions that work with
Spark 3.2.1 by calling the backported Scala implementation.
"""

from typing import Dict, Optional, TYPE_CHECKING

from pyspark.sql.column import Column
from pyspark.sql.utils import get_active_spark_context
import warnings

if TYPE_CHECKING:
    try:
        from pyspark.sql._typing import ColumnOrName
    except ImportError:
        # For older PySpark versions that don't have _typing
        pass


def _check_spark_version():
    """Check if running on a compatible Spark version (< 3.4)."""
    try:
        from pyspark import __version__ as pyspark_version
        version_parts = pyspark_version.split('.')
        major = int(version_parts[0])
        minor = int(version_parts[1])
        
        if major > 3 or (major == 3 and minor >= 4):
            warnings.warn(
                f"This protobuf backport is designed for Spark < 3.4, but you're running "
                f"PySpark {pyspark_version}. Consider using the native protobuf functions "
                f"in Spark 3.4+ instead of this backport.",
                UserWarning,
                stacklevel=3
            )
    except Exception:
        # If we can't determine the version, just continue
        pass


def from_protobuf(
    data: "ColumnOrName",
    messageName: str,
    descFilePath: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    binaryDescriptorSet: Optional[bytes] = None,
) -> Column:
    """
    Converts a binary column of Protobuf format into its corresponding catalyst value.
    The Protobuf definition is provided in one of these ways:

       - Protobuf descriptor file: E.g. a descriptor file created with
          `protoc --include_imports --descriptor_set_out=abc.desc abc.proto`
       - Protobuf descriptor as binary: Rather than file path as in previous option,
         we can provide the binary content of the file. This allows flexibility in how the
         descriptor set is created and fetched.
       - Jar containing Protobuf Java class: The jar containing Java class should be shaded.
         Specifically, `com.google.protobuf.*` should be shaded to
         `org.sparkproject.spark.protobuf311.*`.

    .. note:: Backported from Spark 3.4.0 to work with Spark 3.2.1

    Parameters
    ----------
    data : :class:`~pyspark.sql.Column` or str
        the binary column.
    messageName: str
        the protobuf message name to look for in descriptor file, or
        The Protobuf class name when descFilePath parameter is not set.
        E.g. `com.example.protos.ExampleEvent`.
    descFilePath : str, optional
        The Protobuf descriptor file.
    options : dict, optional
        options to control how the protobuf record is parsed.
    binaryDescriptorSet: bytes, optional
        The Protobuf `FileDescriptorSet` serialized as binary.

    Notes
    -----
    This is a backport of Spark 3.4's protobuf functionality to Spark 3.2.1.
    The backported JAR must be added to your Spark session.

    Examples
    --------
    >>> # Usage with compiled protobuf class (shaded)
    >>> from pyspark.sql.protobuf.functions import from_protobuf
    >>> df = spark.createDataFrame([("binary_data",)], ["proto_data"])
    >>> result = df.select(from_protobuf(df.proto_data, "com.example.MyMessage"))
    
    >>> # Usage with descriptor file
    >>> result = df.select(from_protobuf(df.proto_data, "MyMessage", "/path/to/desc.desc"))
    
    >>> # Usage with binary descriptor set
    >>> with open("/path/to/desc.desc", "rb") as f:
    ...     desc_bytes = f.read()
    >>> result = df.select(from_protobuf(df.proto_data, "MyMessage", binaryDescriptorSet=desc_bytes))
    """
    from py4j.java_gateway import JVMView
    from pyspark.sql.column import _to_java_column

    # Check Spark version compatibility
    _check_spark_version()

    sc = get_active_spark_context()
    
    try:
        # Convert Column to Java Column if needed
        java_column = _to_java_column(data)
        
        # Access the backported functions via JVM
        backport_functions = getattr(
            sc._jvm, "org.apache.spark.sql.protobuf.backport.functions"
        )
        
        # Choose the appropriate overloaded method based on parameters
        if binaryDescriptorSet is not None:
            # Binary descriptor set path
            if options is not None:
                jc = backport_functions.from_protobuf(
                    java_column, messageName, binaryDescriptorSet, options
                )
            else:
                jc = backport_functions.from_protobuf(
                    java_column, messageName, binaryDescriptorSet
                )
        elif descFilePath is not None:
            # Descriptor file path
            if options is not None:
                # Create Java Map for options
                java_map = sc._jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(
                    sc._jvm.scala.collection.immutable.Map(*[
                        sc._jvm.scala.Tuple2(k, v) for k, v in options.items()
                    ])
                ).asJava()
                jc = backport_functions.from_protobuf(
                    java_column, messageName, descFilePath, java_map
                )
            else:
                jc = backport_functions.from_protobuf(
                    java_column, messageName, descFilePath
                )
        else:
            # Compiled class path
            if options is not None:
                # Create Java Map for options
                java_map = sc._jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(
                    sc._jvm.scala.collection.immutable.Map(*[
                        sc._jvm.scala.Tuple2(k, v) for k, v in options.items()
                    ])
                ).asJava()
                jc = backport_functions.from_protobuf(
                    java_column, messageName, java_map
                )
            else:
                jc = backport_functions.from_protobuf(java_column, messageName)
                
    except Exception as e:
        if "JavaPackage" in str(e) or "object is not callable" in str(e):
            raise RuntimeError(
                "Protobuf backport not found. Ensure the spark-protobuf-backport JAR "
                "is added to your Spark session using --jars or spark.jars.packages"
            ) from e
        raise

    return Column(jc)


def to_protobuf(
    data: "ColumnOrName",
    messageName: str,
    descFilePath: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    binaryDescriptorSet: Optional[bytes] = None,
) -> Column:
    """
    Converts a column into binary of protobuf format. The Protobuf definition is provided in one
    of these ways:

       - Protobuf descriptor file: E.g. a descriptor file created with
          `protoc --include_imports --descriptor_set_out=abc.desc abc.proto`
       - Protobuf descriptor as binary: Rather than file path as in previous option,
         we can provide the binary content of the file. This allows flexibility in how the
         descriptor set is created and fetched.
       - Jar containing Protobuf Java class: The jar containing Java class should be shaded.
         Specifically, `com.google.protobuf.*` should be shaded to
         `org.sparkproject.spark.protobuf311.*`.

    .. note:: Backported from Spark 3.4.0 to work with Spark 3.2.1

    Parameters
    ----------
    data : :class:`~pyspark.sql.Column` or str
        the data column.
    messageName: str
        the protobuf message name to look for in descriptor file, or
        The Protobuf class name when descFilePath parameter is not set.
        E.g. `com.example.protos.ExampleEvent`.
    descFilePath : str, optional
        the Protobuf descriptor file.
    options : dict, optional
        options to control how the protobuf is serialized.
    binaryDescriptorSet: bytes, optional
        The Protobuf `FileDescriptorSet` serialized as binary.

    Notes
    -----
    This is a backport of Spark 3.4's protobuf functionality to Spark 3.2.1.
    The backported JAR must be added to your Spark session.

    Examples
    --------
    >>> # Usage with compiled protobuf class (shaded)
    >>> from pyspark.sql.protobuf.functions import to_protobuf
    >>> df = spark.createDataFrame([((1, "Alice"),)], ["person"])
    >>> result = df.select(to_protobuf(df.person, "com.example.Person"))
    
    >>> # Usage with descriptor file
    >>> result = df.select(to_protobuf(df.person, "Person", "/path/to/desc.desc"))
    
    >>> # Usage with binary descriptor set  
    >>> with open("/path/to/desc.desc", "rb") as f:
    ...     desc_bytes = f.read()
    >>> result = df.select(to_protobuf(df.person, "Person", binaryDescriptorSet=desc_bytes))
    """
    from py4j.java_gateway import JVMView
    from pyspark.sql.column import _to_java_column

    # Check Spark version compatibility
    _check_spark_version()

    sc = get_active_spark_context()
    
    try:
        # Convert Column to Java Column if needed
        java_column = _to_java_column(data)
        
        # Access the backported functions via JVM
        backport_functions = getattr(
            sc._jvm, "org.apache.spark.sql.protobuf.backport.functions"
        )
        
        # Choose the appropriate overloaded method based on parameters
        if binaryDescriptorSet is not None:
            # Binary descriptor set path
            if options is not None:
                jc = backport_functions.to_protobuf(
                    java_column, messageName, binaryDescriptorSet, options
                )
            else:
                jc = backport_functions.to_protobuf(
                    java_column, messageName, binaryDescriptorSet
                )
        elif descFilePath is not None:
            # Descriptor file path
            if options is not None:
                # Create Java Map for options
                java_map = sc._jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(
                    sc._jvm.scala.collection.immutable.Map(*[
                        sc._jvm.scala.Tuple2(k, v) for k, v in options.items()
                    ])
                ).asJava()
                jc = backport_functions.to_protobuf(
                    java_column, messageName, descFilePath, java_map
                )
            else:
                jc = backport_functions.to_protobuf(
                    java_column, messageName, descFilePath
                )
        else:
            # Compiled class path
            if options is not None:
                # Create Java Map for options
                java_map = sc._jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(
                    sc._jvm.scala.collection.immutable.Map(*[
                        sc._jvm.scala.Tuple2(k, v) for k, v in options.items()
                    ])
                ).asJava()
                jc = backport_functions.to_protobuf(
                    java_column, messageName, java_map
                )
            else:
                jc = backport_functions.to_protobuf(java_column, messageName)
                
    except Exception as e:
        if "JavaPackage" in str(e) or "object is not callable" in str(e):
            raise RuntimeError(
                "Protobuf backport not found. Ensure the spark-protobuf-backport JAR "
                "is added to your Spark session using --jars or spark.jars.packages"
            ) from e
        raise

    return Column(jc)


def _read_descriptor_set_file(filePath: str) -> bytes:
    """Helper function to read descriptor set files."""
    with open(filePath, "rb") as f:
        return f.read()