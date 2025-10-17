# read-source

Read source code from third-party dependency JARs (Protobuf, Spark) cached by Coursier.

## Description

Use to read source files from third-party dependencies when implementing features or debugging issues. Supports searching for files in JARs and extracting specific line ranges.

## Available JARs

```bash
COURSIER_CACHE=~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2

# Protobuf Java 3.21.7 sources
PROTOBUF_JAR=$COURSIER_CACHE/com/google/protobuf/protobuf-java/3.21.7/protobuf-java-3.21.7-sources.jar

# Spark 3.2.1 sources
SPARK_CATALYST_JAR=$COURSIER_CACHE/org/apache/spark/spark-catalyst_2.12/3.2.1/spark-catalyst_2.12-3.2.1-sources.jar
SPARK_SQL_JAR=$COURSIER_CACHE/org/apache/spark/spark-sql_2.12/3.2.1/spark-sql_2.12-3.2.1-sources.jar
SPARK_CORE_JAR=$COURSIER_CACHE/org/apache/spark/spark-core_2.12/3.2.1/spark-core_2.12-3.2.1-sources.jar
```

## Usage

**Search for files in JAR:**
```bash
jar tf $PROTOBUF_JAR | grep CodedInputStream
jar tf $SPARK_CATALYST_JAR | grep UnsafeWriter
```

**Read entire file:**
```bash
unzip -p $PROTOBUF_JAR com/google/protobuf/CodedInputStream.java
unzip -p $SPARK_CATALYST_JAR org/apache/spark/sql/catalyst/expressions/codegen/UnsafeWriter.java
```

**Read specific line range (e.g., lines 1-200):**
```bash
unzip -p $PROTOBUF_JAR com/google/protobuf/CodedInputStream.java | sed -n '1,200p'
unzip -p $SPARK_CATALYST_JAR org/apache/spark/sql/catalyst/expressions/codegen/UnsafeWriter.java | sed -n '113,120p'
```

**Pipe to grep for specific patterns:**
```bash
unzip -p $PROTOBUF_JAR com/google/protobuf/CodedInputStream.java | grep -A 5 'readRawVarint32'
```

## Examples

**Find UnsafeWriter class:**
```bash
jar tf $SPARK_CATALYST_JAR | grep UnsafeWriter
# org/apache/spark/sql/catalyst/expressions/codegen/UnsafeWriter.java
```

**Read UnsafeWriter methods:**
```bash
unzip -p $SPARK_CATALYST_JAR org/apache/spark/sql/catalyst/expressions/codegen/UnsafeWriter.java | sed -n '50,150p'
```

**Find CodedInputStream optimization methods:**
```bash
unzip -p $PROTOBUF_JAR com/google/protobuf/CodedInputStream.java | grep -E 'readRaw(Varint|LittleEndian)'
```

## Notes

- Inline JAR path variables when using bash commands
- Use `grep` for simple pattern matching, `sed -n 'start,endp'` for line ranges
- JAR files must exist in Coursier cache (run `sbt compile` first if missing)
