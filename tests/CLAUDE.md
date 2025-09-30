# tests/ Module

## Design Philosophy

**Three-tier testing strategy balances speed, coverage, and confidence:**

1. **Tier 1 (Unit)**: Fast feedback on each push (<5s). Tests all 4 parser implementations against identical test cases via shared behaviors in `ParserBehaviors` trait.

2. **Tier 2 (Property)**: Validates parser equivalence with ScalaCheck. Tests that `WireFormatParser` and `GeneratedWireFormatParser` produce identical results across 100 randomly generated inputs per property. Runs on PR merge.

3. **Tier 3 (Integration)**: Verifies distributed execution with Spark `local[2]` and 4 shuffle partitions. Tests parser serialization across JVM boundaries and multi-partition consistency. Runs before merge to master.

**Why three tiers**: Optimize CI feedback loop. Developers get instant feedback from Tier 1, while Tier 2/3 catch edge cases and distributed issues without slowing down iteration.

## Running Tests

```bash
sbt unitTests           # Tier 1: <5s
sbt propertyTests       # Tier 2: <30s
sbt integrationTests    # Tier 3: <60s
sbt allTestTiers        # All tiers sequentially
```

## Critical Developer Notes

### Parser Equivalence, Not Roundtrip
Tier 2 tests **parser equivalence** (do two parsers produce identical `InternalRow`s?) rather than roundtrip (does binary → struct → binary preserve data?). This catches parser bugs directly without confounding errors from serialization.

### Nullability Is Not Separate
Null handling is **integrated into each test spec**, not isolated. Protobuf default behavior (missing fields → defaults, not null) differs from Spark SQL, so every test must verify this explicitly.

### Packed vs Unpacked Must Both Be Tested
Repeated fields have two wire encodings: packed (length-delimited array) and unpacked (repeated tags). **Both must be tested** for each packable type. Proto3 defaults to packed, but proto2 files and explicit `[packed = false]` annotations produce unpacked. See `AllRepeatedTypes` (packed) vs `AllUnpackedRepeatedTypes` (unpacked).

### Integration Tests Use JavaSerializer, Not Kryo
Kryo serialization fails with Java module system restrictions (`java.nio.ByteBuffer.hb` not accessible). Integration tests use `JavaSerializer` explicitly. If you see module access errors, check serializer config.

### Deterministic Test Ordering Required
After `repartition()`, DataFrame row order is **not guaranteed**. Always add `.orderBy()` before `collect()` in integration tests to prevent flaky failures.

### Why local[2] Instead of local-cluster
`local[2]` provides parallel execution with 4 shuffle partitions, simulating distributed behavior without multi-JVM complexity. `local-cluster` adds overhead and brittle failure modes ("Master removed our application") without meaningful additional coverage.

### RowEquivalenceChecker Handles Enum Ambiguity
Protobuf enums can be represented as `IntegerType` or `StringType` depending on parser implementation. Use `RowEquivalenceChecker.assertRowsEquivalent()` for comparisons, not direct equality. It automatically handles enum int ↔ string equivalence.

### Proto Files Are Source, Not Test Fixtures
Proto files in `tests/src/main/protobuf/` are **compiled to Java classes**, not text fixtures. Changes require `sbt compile` to regenerate Java sources. Test data is created via `TestData` factory methods, not hand-written binary blobs.

## Coverage Summary

- **All 16 protobuf primitive types**: Including sint32/sint64 (ZigZag encoding), fixed32/fixed64 (little-endian)
- **Packed and unpacked repeated fields**: Both encodings tested for all 13 packable types
- **Nested and recursive messages**: CompleteMessage, Recursive, MutualA/MutualB
- **Edge cases**: Empty messages, sparse fields (randomly omitted), default values, oneofs
- **Distributed execution**: 1000+ rows across 4 partitions, parser serialization, roundtrip consistency

**Not yet implemented**: Maps (proto file exists, tests pending)