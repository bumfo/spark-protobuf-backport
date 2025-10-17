# JMH Benchmark Monitoring Scripts

Two bash scripts for monitoring JMH benchmark progress during long-running executions.

## Scripts

### monitor-jmh.sh - Live Progress Monitor

Follows the log file in real-time and displays formatted progress updates.

**Usage:**
```bash
# Basic usage
scripts/monitor-jmh.sh /tmp/jmh_benchmark.log

# With timeout (auto-exit after 30 seconds)
scripts/monitor-jmh.sh -t 30 /tmp/jmh_benchmark.log
```

**Options:**
- `-t <seconds>` or `--timeout <seconds>`: Exit after specified time if benchmark hasn't completed

**Output:**
- Current benchmark name (shortened to last component)
- Progress percentage and ETA
- Current fork (1 of 2, 2 of 2)
- Warmup iterations with values
- Measurement iterations with values (color-coded in cyan)
- Errors/warnings (highlighted in yellow)
- Auto-exits when "Run complete" is detected

**When to use:** For continuous monitoring of long-running benchmarks. Keep it running in a separate terminal.

---

### jmh-status.sh - Quick Status Check

Provides a one-shot status summary without continuous monitoring.

**Usage:**
```bash
scripts/jmh-status.sh /tmp/jmh_benchmark.log
```

**Output:**
- Current benchmark name
- Latest progress % and ETA
- Current fork
- Current phase (Warmup/Measurement) with latest iteration value
- Completion status

**When to use:** For periodic status checks while working on other tasks. Run it anytime to see current state.

---

## Example Workflow

1. **Start benchmark in background:**
   ```bash
   sbt clean bench/Test/compile 'bench/Jmh/run .*ScalarBenchmark.*' | tee /tmp/jmh_scalar.log &
   ```

2. **Monitor progress (option A - live monitoring):**
   ```bash
   # Without timeout
   scripts/monitor-jmh.sh /tmp/jmh_scalar.log

   # With 30-second timeout
   scripts/monitor-jmh.sh -t 30 /tmp/jmh_scalar.log
   ```

3. **Or check status periodically (option B - quick checks):**
   ```bash
   scripts/jmh-status.sh /tmp/jmh_scalar.log
   ```

4. **Extract results when complete:**
   ```bash
   # Use log-reader agent to parse the JMH result table
   ```

## Output Examples

### monitor-jmh.sh (live)
```
JMH Benchmark Progress Monitor
Monitoring: /tmp/jmh_scalar.log
---

▶ Benchmark: ScalarBenchmark.anInlineParser
  Progress: 10.00% | ETA: 00:03:07
  Fork 1/2
    Warmup 1: 1234.567 ns/op
    Warmup 2: 1230.123 ns/op
    ...
    Measure 1: 1225.789 ns/op
    Measure 2: 1228.456 ns/op
```

### jmh-status.sh (one-shot)
```
JMH Benchmark Status
File: /tmp/jmh_scalar.log
---
Benchmark: ScalarBenchmark.anInlineParser
Progress:  45.00%
ETA:       00:01:52
Fork:      2 of 2
Phase:     Measurement (iter 5: 1227.345 ns/op)

⏳ Benchmark still running...
```

## Notes

- Both scripts require the log file to exist
- Scripts are colored for better readability (ANSI color codes)
- Scripts work on macOS and Linux
- Progress information is extracted from JMH's standard output format
