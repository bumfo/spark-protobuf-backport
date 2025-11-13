# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Scala/Spark project that backports Spark 3.4's protobuf connector to Spark 3.2.1. It provides `from_protobuf` and `to_protobuf` functions to convert between binary Protobuf data and Spark SQL DataFrames without requiring users to upgrade Spark or patch the runtime.

The project includes three key optimizations:
1. **Compiled message support**: When `messageName` refers to a compiled Java class, uses generated `Parser` via Janino for direct conversion to `UnsafeRow`, avoiding `DynamicMessage` overhead
2. **Wire format parser**: Direct binary parsing for binary descriptor sets using `WireFormatParser`
3. **Binary descriptor sets**: Allows passing descriptor bytes directly to avoid file distribution issues on executors

## Project Structure

- **`core/`** - Scala implementation of the protobuf backport (see `core/CLAUDE.md`)
- **`python/`** - PySpark wrapper with comprehensive testing (see `python/CLAUDE.md`) 
- **`shaded/`** - Shaded protobuf dependencies for conflict avoidance
- **`uber/`** - Assembled JAR with all dependencies

## Parser Architecture

The project features a three-tier parser interface hierarchy optimized for different use cases:

1. **`Parser`** - Base interface for simple protobuf binary → InternalRow conversion
2. **`BufferSharingParser`** - Base implementation with buffer sharing for efficient nested conversions
3. **`MessageParser[T]`** - Interface for compiled protobuf message → InternalRow conversion

See `core/CLAUDE.md` for detailed interface documentation and usage examples.

## Build Commands

```bash
# Compile the project
sbt compile

# Run tests (3-tier testing system, see tests/CLAUDE.md)
sbt unitTests           # Tier 1: Fast unit tests (<5s)
sbt propertyTests       # Tier 2: Property-based tests (<30s)
sbt integrationTests    # Tier 3: Spark integration tests (<60s)
sbt allTestTiers        # All tiers sequentially

# Run JMH benchmarks
sbt jmh                 # Full JMH benchmark suite
sbt jmhQuick            # Quick benchmark (fewer iterations)

# Build shaded JAR with all dependencies
sbt assembly

# Clean build artifacts
sbt clean
```

The assembled JAR includes shaded protobuf dependencies to avoid conflicts with Spark's own protobuf runtime.

## Nested Schema Pruning

The connector supports nested schema pruning to optimize deserialization performance. When enabled (default), only fields actually accessed in queries are parsed from protobuf binaries.

**Configuration**:
```scala
spark.conf.set("spark.sql.protobuf.nestedSchemaPruning.enabled", "true")  // default
```

**Example**:
```scala
// Only parses person.name field, skipping all other person fields
df.select(from_protobuf($"data", "MyMessage", descriptorBytes).as("proto"))
  .select($"proto.person.name")
```

**Limitations**:
- Only applies to WireFormat parser (binary descriptor set usage)
- Compiled message parsers and DynamicMessage parsers are not pruned
- Most effective for wide schemas with selective field access

## Performance Reporting

Use unambiguous terminology for performance improvements:
- **"1.5x speedup"** (ratio of old time to new time)
- **"33% reduction in execution time"** (percent time saved)
- **"Reduced from 2000ns to 1500ns"** (direct comparison)

Avoid ambiguous phrasing like "1.5x faster" which could mean either direction.

## Git Workflow

**IMPORTANT**: Never push directly to the `master` branch. Always work on feature branches.

### Creating Pull Requests

Before creating a PR, ensure you are on a feature branch:

1. **Check current branch**: `git branch --show-current`
2. **If on master, create feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes and commit**
4. **Push feature branch**: `git push -u origin feature/your-feature-name`
5. **Create PR**: `gh pr create --title "Your PR Title" --body "PR description"`

### Branch Naming Convention
- `feature/description` - for new features
- `fix/description` - for bug fixes
- `perf/description` - for performance improvements
- `test/description` - for test-only changes

### Commit Guidelines
**Always check what will be committed before running `git commit`**:
```bash
git -c color.status=never status -sb    # Check staged and unstaged files
git diff --cached    # Review staged changes
```

**Stage files in a single command with all changed files from current session** - avoid repeated `git add <file>`, `git add -A`, or `git add .`

## Module Documentation

For detailed implementation and development information:

- **Core Scala Implementation**: See `core/CLAUDE.md` for architecture, performance benchmarks, and development notes
- **Testing Framework**: See `tests/CLAUDE.md` for 3-tier testing strategy and developer notes
- **PySpark Support**: See `python/CLAUDE.md` for Python wrapper implementation and testing

## Usage Patterns

The backport supports three protobuf usage patterns:

1. **Compiled Java class**: `from_protobuf(col("data"), "com.example.MyMessage")`
2. **Descriptor file**: `from_protobuf(col("data"), "MyMessage", "/path/to/schema.desc")`
3. **Binary descriptor set**: `from_protobuf(col("data"), "MyMessage", descriptor_bytes)`

## Options

The `from_protobuf` function accepts an `options` parameter for customizing parsing behavior:

### Parse Mode
```scala
// Permissive mode: returns null on parsing errors
from_protobuf(col("data"), "MyMessage", descriptorBytes,
  Map("mode" -> "PERMISSIVE"))

// Fail-fast mode: throws exception on errors (default)
from_protobuf(col("data"), "MyMessage", descriptorBytes,
  Map("mode" -> "FAILFAST"))
```

### Recursive Fields Handling

The backport supports Spark 3.4+ compatible recursive field handling with two orthogonal configuration options:

**1. Recursion Depth (`recursive.fields.max.depth`)**

Spark-aligned depth semantics:
- **`-1`** (Spark default): Forbid recursive fields. WireFormat parser allows RecursiveStructType by default, Generated/Dynamic parsers throw error on recursion.
- **`0`** (Our extension): Unlimited recursion using RecursiveStructType. Not in Spark 3.4+.
- **`1`**: Drop all recursive fields (0 recursions allowed).
- **`2`**: Allow field to appear twice total (recursed once).
- **`3-10`**: Allow field to appear N times total (recursed N-1 times).

```scala
// Spark default: forbid recursion (WireFormat: RecursiveStructType, others: error)
from_protobuf(col("data"), "DomNode", descriptorBytes)

// Unlimited recursion (our extension, not in Spark)
from_protobuf(col("data"), "DomNode", descriptorBytes,
  Map("recursive.fields.max.depth" -> "0"))

// Spark-aligned: allow field to appear twice (recursed once)
from_protobuf(col("data"), "DomNode", descriptorBytes,
  Map("recursive.fields.max.depth" -> "2"))
```

**2. Recursion Mode (`recursive.fields.mode`)**

Controls how recursion is handled:
- **`""`** (default): Behavior depends on depth and parser (see precedence below)
- **`"drop"`**: Drop recursive fields from schema entirely
- **`"binary"`**: Replace recursive fields with BinaryType
- **`"fail"`**: Throw exception on recursion (explicit, not recommended for users)
- **`"recursive"`**: Use RecursiveStructType (explicit, only valid with depth=0 or depth=-1)

```scala
// Binary mode: Mock recursive fields as BinaryType
from_protobuf(col("data"), "DomNode", descriptorBytes,
  Map("recursive.fields.mode" -> "binary"))

// Drop mode: Omit recursive fields from schema entirely
from_protobuf(col("data"), "DomNode", descriptorBytes,
  Map("recursive.fields.mode" -> "drop"))
```

**Configuration Precedence**

When `depth=-1` (Spark default):
- `mode=""` → fail for Generated/Dynamic parsers, recursive for WireFormat parser
- `mode="drop"/"binary"/"recursive"` → override Spark default with explicit mode

When `depth=0` (unlimited, our extension):
- `mode=""` → recursive (RecursiveStructType)
- `mode="drop"/"binary"/"fail"` → override unlimited with explicit mode

When `depth≥1` (Spark-aligned depth limit):
- `mode=""` → drop (default for depth-limited)
- `mode="drop"/"binary"` → use specified mode
- `mode="recursive"` → ERROR (illegal combination)

**Internal Conversion**

Spark counts total field appearances, we count depth from recursion point:
- Spark depth N → Internal maxRecursiveDepth = N - 1
- Example: Spark depth=2 → Internal maxRecursiveDepth=1 (allow once after recursion detection)

## Documentation Style

Write concisely while staying accurate. Focus on practical usage patterns and examples over lengthy explanations.

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.