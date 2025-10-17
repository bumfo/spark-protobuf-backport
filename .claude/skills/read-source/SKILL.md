# read-source

Read source code from third-party dependency JARs (JDK, Scala, Protobuf, Spark) cached by Coursier or bundled with JDK.

## Description

Use to read source files from third-party dependencies when implementing features or debugging issues. Supports searching for files in JARs and extracting specific line ranges. Includes JDK sources (java.util, java.lang, etc.) and Scala standard library sources used by sbt.

## Available JARs

```bash
COURSIER_CACHE=~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2

# JDK sources - MUST use sbt to get the ACTUAL java.home used by sbt
# Do NOT use system JAVA_HOME or other Java installations - they may differ from sbt's JDK
# Step 1: Run this command ONCE to get **JDK_HOME** (takes ~2s)
sbt -no-colors 'eval System.getProperty("java.home")' 2>/dev/null | grep 'ans: String' | cut -d= -f2- | xargs
# Step 2: Inline the result from Step 1 directly to avoid sbt startup cost in subsequent uses
JDK_SRC=<JDK_HOME>/lib/src.zip

# Scala 2.12.15 sources
SCALA_LIBRARY_JAR=$COURSIER_CACHE/org/scala-lang/scala-library/2.12.15/scala-library-2.12.15-sources.jar

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
jar tf $SCALA_LIBRARY_JAR | grep Option
jar tf $PROTOBUF_JAR | grep CodedInputStream
jar tf $SPARK_CATALYST_JAR | grep UnsafeWriter
unzip -l $JDK_SRC | grep HashMap
```

**Read entire file:**
```bash
unzip -p $SCALA_LIBRARY_JAR scala/Option.scala
unzip -p $PROTOBUF_JAR com/google/protobuf/CodedInputStream.java
unzip -p $SPARK_CATALYST_JAR org/apache/spark/sql/catalyst/expressions/codegen/UnsafeWriter.java
unzip -p $JDK_SRC java.base/java/util/HashMap.java
```

**Read specific line range (e.g., lines 1-200):**
```bash
unzip -p $PROTOBUF_JAR com/google/protobuf/CodedInputStream.java | sed -n '1,200p'
unzip -p $SPARK_CATALYST_JAR org/apache/spark/sql/catalyst/expressions/codegen/UnsafeWriter.java | sed -n '113,120p'
unzip -p $JDK_SRC java.base/java/util/HashMap.java | sed -n '563,592p'
```

**Pipe to grep for specific patterns:**
```bash
unzip -p $PROTOBUF_JAR com/google/protobuf/CodedInputStream.java | grep -A 5 'readRawVarint32'
unzip -p $JDK_SRC java.base/java/util/HashMap.java | grep -A 10 'final Node<K,V> getNode'
```

## Examples

**Find HashMap class in JDK:**
```bash
unzip -l $JDK_SRC | grep 'java/util/HashMap.java'
# java.base/java/util/HashMap.java
```

**Read HashMap.get method:**
```bash
unzip -p $JDK_SRC java.base/java/util/HashMap.java | sed -n '563,592p'
```

**Find Scala Option class:**
```bash
jar tf $SCALA_LIBRARY_JAR | grep 'scala/Option.scala'
# scala/Option.scala
```

**Read Scala Option methods:**
```bash
unzip -p $SCALA_LIBRARY_JAR scala/Option.scala | grep -A 10 'def map'
```

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
- **JDK sources**: MUST use `sbt 'eval System.getProperty("java.home")'` to get sbt's actual JDK (not system JAVA_HOME)
- JDK source paths use module prefix (e.g., `java.base/java/util/HashMap.java`)
- **Prefer `unzip -p`**: Read files directly from archives without extracting to temp files (faster, cleaner)
