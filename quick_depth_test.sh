#!/bin/bash

# Quick test with fewer iterations
test_depth() {
    local canonical=$1

    # Set canonical depth
    sed -i '' "s/CANONICAL_KEY_DEPTH = .*/CANONICAL_KEY_DEPTH = $canonical/" core/src/main/scala/fastproto/InlineParserToRowGenerator.scala

    echo canonical="$canonical"
    # Run benchmark
    sbt clean "jmhBinaryTree -p accessDepth=2,4,6 -p payloadSize=100" 2>&1 | grep -E "(Benchmark.*Mode.*Units|inlineParser.*avgt)"
    # sbt clean "jmhMultiwayTree -p depth=5 -p branchingFactor=4 -p accessDepth=5" | grep -E "(Benchmark.*Mode.*Units|inlineParser.*avgt)"
    echo
}

test_depth 0
test_depth 1
test_depth 100

# Restore
sed -i '' 's/CANONICAL_KEY_DEPTH = .*/CANONICAL_KEY_DEPTH = 1/' core/src/main/scala/fastproto/InlineParserToRowGenerator.scala
