# PySpark Protobuf Backport

This directory contains the complete PySpark support for the Spark protobuf backport.

## 🚀 Quick Start

```bash
# Build JAR and test (from project root)
sbt uberJar/assembly

# Run the core functionality test
cd python
source .venv/bin/activate
python test_pyspark_protobuf.py
```

## 📁 Directory Structure

```
python/
├── setup.py                    # Package installation
├── test_pyspark_protobuf.py    # Core functionality test
├── examples/                   # Usage examples
│   └── protobuf_example.py
└── spark_protobuf/             # Python package
    ├── __init__.py
    └── functions.py            # from_protobuf, to_protobuf functions
```

## 🎯 Key Files

- **`functions.py`** - Main PySpark wrapper functions
- **`test_pyspark_protobuf.py`** - Core functionality test

## ⚡ Usage

### 1. Build JAR and Test
```bash
# Build the shaded JAR (from project root)
sbt uberJar/assembly

# Run the core functionality test
cd python
source .venv/bin/activate
python test_pyspark_protobuf.py
```

### 2. Manual Installation
```bash
pip install -e .
```

### 3. Interactive PySpark
```bash
pyspark \
    --jars ../uber/target/scala-2.12/spark-protobuf-backport-shaded-0.1.0-SNAPSHOT.jar \
    --py-files . \
    --conf spark.sql.extensions=org.apache.spark.sql.protobuf.backport.ProtobufExtensions
```

### 4. Use Functions
```python
# Import the functions
from spark_protobuf.functions import from_protobuf, to_protobuf

# Use directly with DataFrames
df.select(from_protobuf(df.data, 'MyMessage', '/path/to/schema.desc'))

# Via SQL (also works)
spark.sql("SELECT from_protobuf(data, 'MyMessage', '/path/to/schema.desc') FROM table")

# Via expr() in DataFrame API  
from pyspark.sql.functions import expr
df.select(expr("from_protobuf(data, 'MyMessage')").alias("decoded"))
```

## 📚 Documentation

- **[examples/](examples/)** - Working examples

## ✅ What Works

- ✅ **Spark 3.2.x and 3.3.x compatibility**
- ✅ **Clean Python imports**: `from spark_protobuf.functions import from_protobuf, to_protobuf`
- ✅ **SQL functions registration** (`from_protobuf`, `to_protobuf`)
- ✅ **DataFrame API via direct function calls**
- ✅ **DataFrame API via `expr()`**
- ✅ **All three modes**: compiled class, descriptor file, binary descriptor
- ✅ **Options support**: parse modes, recursion limits
- ✅ **Virtual environment support**
- ✅ **No namespace conflicts** with PySpark

## 🔧 Requirements

- **Python**: 3.6+
- **PySpark**: 3.2.0 to < 3.4.0  
- **Java**: 11 or 17
- **Scala**: 2.12.x

Perfect for users who need Spark 3.4's protobuf functionality on earlier Spark versions!