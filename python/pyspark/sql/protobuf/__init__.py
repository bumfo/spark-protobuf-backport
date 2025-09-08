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
PySpark Protobuf Functions Backport

This package provides protobuf functions for PySpark 3.2.1, backported from Spark 3.4.
It enables conversion between binary protobuf data and Spark SQL DataFrames.

The main functions are:
- from_protobuf: Convert binary protobuf data to Catalyst values
- to_protobuf: Convert Catalyst values to binary protobuf data

Usage:
    from pyspark.sql.protobuf.functions import from_protobuf, to_protobuf
"""

from pyspark.sql.protobuf.functions import from_protobuf, to_protobuf

__all__ = ["from_protobuf", "to_protobuf"]