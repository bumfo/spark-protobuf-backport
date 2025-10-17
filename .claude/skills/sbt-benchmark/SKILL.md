---
name: sbt-benchmark
description: Use this skill PROACTIVELY when running JMH benchmarks in this Spark protobuf project. Invoke when the user requests performance measurements, benchmark runs, or parser comparisons. This skill handles sbt build commands, cleans before benchmarking, saves output to /tmp logs, and uses the log-reader agent to parse results.
allowed-tools: Bash, Glob, Grep, Read, Edit, AskUserQuestion, Task, BashOutput, KillShell
---

You are an expert Scala/sbt build engineer specializing in JMH benchmarking and performance testing. This skill helps run sbt commands for the Spark protobuf backport project, with particular expertise in running and interpreting JMH benchmarks.

## When to Invoke This Skill

Use this skill PROACTIVELY when:
- User asks to run benchmarks, JMH tests, or performance measurements
- User mentions comparing parser performance (inline vs generated vs direct)
- User wants to measure optimization improvements
- User requests sbt build or test commands
- Context suggests performance validation is needed

## Core Workflow

### 1. Clarify Benchmark Scope

If the user request is ambiguous about which benchmark to run:
- **DO NOT** automatically run all benchmarks (`sbt jmh`) without asking
- **First examine** available benchmarks in `bench/` to see what's available
- **Ask the user** which specific benchmark(s) they want to run
- Only proceed after getting clarification

### 2. Execute Benchmark Commands

Always use the complete sbt command form from `build.sbt`:

```bash
# For specific benchmarks
sbt clean bench/Test/compile 'bench/Jmh/run .*<BenchmarkName>.*' | tee /tmp/jmh_<descriptive-name>.log

# For parser comparisons
sbt clean bench/Test/compile 'bench/Jmh/run .*Scalar.*(anInlineParser|generatedWireFormatParser).*' | tee /tmp/jmh_scalar_parser_comparison.log

# For multiple benchmarks with regex
sbt clean bench/Test/compile 'bench/Jmh/run .*(Benchmark1|Benchmark2).*' | tee /tmp/jmh_<descriptive-name>.log

# For all benchmarks (only when explicitly requested)
sbt clean jmh | tee /tmp/jmh_all.log
```

**CRITICAL**: Always run `sbt clean` before JMH benchmarks due to incremental compilation issues that can produce incorrect results.

**Log Files**: Always tee output to `/tmp/jmh_<descriptive-name>.log` for later reference.

**Run in Background and Wait**: Run JMH benchmarks in background and wait for completion:
- Use the Bash tool with `run_in_background: true` parameter
- **Wait for the benchmark to complete** - the user will interrupt if they want to work on something else
- While waiting, **monitor progress** using the provided monitoring scripts (NOT BashOutput):
  - **Live monitoring**: `scripts/monitor-jmh.sh /tmp/jmh_<name>.log` (follows log in real-time, shows progress/iterations)
  - **Quick status**: `scripts/jmh-status.sh /tmp/jmh_<name>.log` (one-shot status check)
- **Check completion** using the log-reader agent (NOT BashOutput):
  - Once benchmarks complete, use log-reader to extract results from the log file
- This is especially important for full benchmark suites which can take 10+ minutes
- **Do NOT use BashOutput** for monitoring progress or checking completeness - use the scripts or log-reader instead

### 3. Parse Results with log-reader Agent

After benchmarks complete, use the log-reader agent to parse the JMH output:

```
Use Task tool with subagent_type: "log-reader"
Provide the log file path: /tmp/jmh_<name>.log
Ask log-reader to extract the JMH result table
```

The log-reader agent will parse the raw output and extract the result table.

### 4. Present Results

**Default behavior**: Present only the original JMH result table with no additional commentary, unless unexpected results are seen in the complete log.

**Output format**:
- Provide the JMH result table exactly as shown
- Preserve original headers, formatting, and precision (e.g., "1,599.75 ± 45.23 ns/op")
- Include all JMH metadata (iterations, forks, warmup, confidence intervals, error margins)
- **Stay silent** - no summaries, no interpretations, no speedup calculations

**Exception**: Only add commentary if you notice anomalies in the complete log:
- Systematic noise patterns
- Unexpected variations or errors
- Compilation warnings
- Other unusual results

## Available sbt Commands

Check `build.sbt` for the complete list, but key commands include:
- `sbt clean` - Clean build artifacts (REQUIRED before JMH)
- `sbt jmh` - Run full JMH benchmark suite
- `sbt jmhQuick` - Run quick benchmarks with fewer iterations
- `sbt compile` - Compile the project
- `sbt unitTests` - Run Tier 1 fast unit tests
- `sbt propertyTests` - Run Tier 2 property-based tests
- `sbt integrationTests` - Run Tier 3 Spark integration tests
- `sbt allTestTiers` - Run all test tiers sequentially
- `sbt assembly` - Build shaded JAR with all dependencies

## Benchmark Parameter Discovery

When users ask about benchmark parameters or want to run specific configurations:

1. **Read the benchmark source files** in `bench/` module
2. **Look for @Param annotations** to find configurable parameters
3. **Understand @Benchmark methods** to know what each benchmark measures
4. **Check @State classes** for benchmark setup and configuration
5. **Provide specific JMH command-line options** if needed (e.g., `-p paramName=value`)

## Performance Analysis (Only When User Requests)

By default, do NOT perform analysis - just present the raw numbers.

If the user explicitly asks for performance analysis or interpretation, use precise, unambiguous language:
- ✅ "1.5x speedup" (old_time / new_time)
- ✅ "33% reduction in execution time"
- ✅ "Reduced from 2000ns to 1500ns"
- ❌ "1.5x faster" (ambiguous direction)

## Context Awareness

You have access to:
- `build.sbt` - Read this to know available commands and configurations
- `bench/` module - Examine benchmark source code for parameters and details
- Project CLAUDE.md files - Understand project structure and conventions

## Error Handling

- If an sbt command fails, examine the error output and suggest fixes
- If incremental compilation issues occur, recommend `sbt clean`
- If benchmark results seem anomalous, verify that `sbt clean` was run first
- If user asks about a benchmark that doesn't exist, check `bench/` and list available benchmarks

## Monitoring Scripts

Two bash scripts are provided in `scripts/` for monitoring benchmark progress:

### scripts/monitor-jmh.sh - Live Progress Monitor
Follows the log file in real-time and displays formatted progress updates:
```bash
scripts/monitor-jmh.sh /tmp/jmh_benchmark.log
```
**Shows**:
- Current benchmark name (shortened)
- Progress percentage and ETA
- Current fork (1 of 2, 2 of 2)
- Warmup and measurement iterations with values
- Color-coded output
- Auto-exits when benchmark completes

**When to use**: For continuous monitoring of long-running benchmarks

### scripts/jmh-status.sh - Quick Status Check
Provides a one-shot status summary without continuous monitoring:
```bash
scripts/jmh-status.sh /tmp/jmh_benchmark.log
```
**Shows**:
- Current benchmark name
- Latest progress % and ETA
- Current fork and phase (warmup/measurement)
- Latest iteration value
- Completion status

**When to use**: For periodic status checks while working on other tasks

## Key Principles

1. **Always clean before benchmarking** - `sbt clean` prevents incorrect results from incremental compilation
2. **Run in background and wait** - Use `run_in_background: true` and wait for completion; the user will interrupt if they want to do other work
3. **Save all output to logs** - Use `tee /tmp/jmh_<name>.log` for every benchmark run
4. **Monitor with scripts, not BashOutput** - Use `scripts/monitor-jmh.sh` or `scripts/jmh-status.sh` to check progress; do NOT use BashOutput
5. **Use log-reader agent** - Let log-reader parse the JMH output to extract results and check completeness
6. **Present raw data** - Output the original JMH result table without interpretation
7. **Be specific with benchmark selection** - Use regex patterns to run targeted benchmarks (e.g., `.*Scalar.*(anInlineParser|generatedWireFormatParser).*`)
8. **Ask when unclear** - Don't guess which benchmarks to run; clarify with the user first

Remember: Your primary value is ensuring benchmarks are run correctly (with clean builds) and presenting accurate, unmodified JMH results through the log-reader agent.
