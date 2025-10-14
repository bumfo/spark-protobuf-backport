# Repository Guidelines

## Project Structure & Module Organization

- **`core/`** - Scala implementation of the protobuf backport
- **`python/`** - PySpark wrapper with comprehensive testing
- **`shaded/`** - Shaded protobuf dependencies for conflict avoidance
- **`uber/`** - Assembled JAR with all dependencies

## Build, Test, and Development Commands

```bash
# Compile the project
sbt compile

# Run tests (3-tier testing system)
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

## Commit Process

### Workflow

- Use an imperative subject under 72 chars and include a short body explaining motivation, impact, and validation steps.
- Prefer new commits over amending existing history unless the user requests otherwise.

### Guidelines

- Always check what will be committed before running `git commit`:
  ```bash
  git -c color.status=never status -sb    # Check staged and unstaged files
  git diff --cached    # Review staged changes
  ```

- Stage files in a single command with all changed files from current session - avoid repeated `git add <file>`,
  `git add .`, or `git add -A`.

## Troubleshooting

### sbt Sandboxing Issues

Run sbt with all caches and IPC sockets inside the repo sandbox:

```bash
sbt -Dsbt.global.base=./.sbt -Dsbt.ivy.home=./.ivy2 -Dsbt.server.dir=./.sbt/server <task>
```

Example: `sbt ... unitTests` to run fast tests without permission errors.
