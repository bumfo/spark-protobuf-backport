# Repository Guidelines

## Project Structure & Module Organization
- Scala core: `core/src/main/scala/...`
- Tests: `core/src/test/scala/**/*Spec.scala`
- Uber (shaded) JAR: `uber/` via sbt-assembly
- Python wrapper/tests: `python/` with a PySpark functional test

## Build, Test, and Development Commands
- `sbt core/test` — run ScalaTest suite (benchmarks excluded by build)
- `sbt uber/assembly` — build shaded JAR at `uber/target/scala-2.12/spark-protobuf-backport-shaded-<ver>.jar`
- `cd python && ./run_test.sh` — run PySpark functional test (expects venv with PySpark 3.2–3.3)

## Testing Guidelines
- Scala tests live under `core/src/test/scala` with `*Spec.scala` naming.
- Python test runner validates the shaded JAR works with PySpark; ensure the JAR is built first.

## Commit Workflow
- Use an imperative subject under 72 chars and include a short body explaining motivation, impact, and validation steps.
- Append `Co-Authored-By: Codex <codex@users.noreply.github.com>` to every commit message.
- Prefer new commits over amending existing history unless the user requests otherwise.
