package org.apache.spark.sql.protobuf.backport.jmh

import fastproto.{IntList, LongList, PrimitiveArrayWriter}
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeArrayWriter, UnsafeRowWriter}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * JMH benchmark comparing array writing strategies for UnsafeArrayData output:
 *
 * 1. FastList (IntList/LongList) + UnsafeArrayWriter loop (two-phase)
 * 2. PrimitiveArrayWriter with append-only writes (single-phase)
 *
 * Both approaches produce identical UnsafeArrayData output.
 *
 * Usage:
 *   sbt "bench/Jmh/run .*ArrayWriterBenchmark.*"
 *   sbt "bench/Jmh/run .*ArrayWriterBenchmark.* -wi 5 -i 10"
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = Array("-Xms2G", "-Xmx2G"))
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
class ArrayWriterBenchmark {

  // Test data sizes
  @Param(Array("10", "11"))
  var arraySize: Int = _

  // Reusable test data
  var intValues: Array[Int] = _
  var longValues: Array[Long] = _

  @Setup
  def setup(): Unit = {
    // Initialize test data with sequential values
    intValues = Array.tabulate(arraySize)(i => i)
    longValues = Array.tabulate(arraySize)(i => i.toLong)
  }

  // ========== Int Array (4-byte elements) ==========

  /**
   * FastList + UnsafeArrayWriter: Traditional two-phase approach
   * 1. Accumulate values in IntList during parsing
   * 2. Copy to UnsafeArrayData via UnsafeArrayWriter loop
   */
  // @Benchmark
  def intArrayFastList(bh: Blackhole): Unit = {
    val rowWriter = new UnsafeRowWriter(1, 8192)

    // Phase 1: Accumulate values in IntList (simulates parsing)
    val list = new IntList()
    var i = 0
    while (i < arraySize) {
      list.add(intValues(i))
      i += 1
    }

    // Phase 2: Convert IntList to UnsafeArrayData
    val offset = rowWriter.cursor()
    val arrayWriter = new UnsafeArrayWriter(rowWriter, 4)
    arrayWriter.initialize(list.count)

    i = 0
    while (i < list.count) {
      arrayWriter.write(i, list.array(i))
      i += 1
    }

    // Verify output is valid UnsafeArrayData
    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    bh.consume(arrayData.numElements())
  }


  // ========== Long Array (8-byte elements) ==========

  /**
   * FastList + UnsafeArrayWriter: Traditional two-phase approach for longs
   */
  @Benchmark
  def longArrayFastList(bh: Blackhole): Unit = {
    val rowWriter = new UnsafeRowWriter(1, 8192)

    // Phase 1: Accumulate values in LongList (simulates parsing)
    val list = new LongList()
    var i = 0
    while (i < arraySize) {
      list.add(longValues(i))
      i += 1
    }

    // Phase 2: Convert LongList to UnsafeArrayData
    val offset = rowWriter.cursor()
    val arrayWriter = new UnsafeArrayWriter(rowWriter, 8)
    arrayWriter.initialize(list.count)

    i = 0
    while (i < list.count) {
      arrayWriter.write(i, list.array(i))
      i += 1
    }

    // Verify output is valid UnsafeArrayData
    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData()
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    bh.consume(arrayData.numElements())
  }



  // ========== PrimitiveArrayWriter Benchmarks ==========

  /**
   * PrimitiveArrayWriter: Optimized append-only writer with size hint
   * - No null support for maximum performance
   * - Pre-allocates based on hint
   * - Zero data movement for arrays ≤64 elements
   */
  @Benchmark
  def longArrayPrimitive(bh: Blackhole): Unit = {
    val rowWriter = new UnsafeRowWriter(1, 8192)

    // Single phase: Direct append-only writes
    val writer = new PrimitiveArrayWriter(rowWriter, 8, arraySize)

    var i = 0
    while (i < arraySize) {
      writer.writeLong(longValues(i))
      i += 1
    }

    val offset = writer.getStartingOffset
    val count = writer.complete()

    // Verify output is valid UnsafeArrayData
    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    bh.consume(arrayData.numElements())
  }

  /**
   * PrimitiveArrayWriter without size hint - tests automatic growth
   */
  @Benchmark
  def longArrayPrimitiveNoHint(bh: Blackhole): Unit = {
    val rowWriter = new UnsafeRowWriter(1, 8192)

    // No size hint - starts with default 64 element capacity
    val writer = new PrimitiveArrayWriter(rowWriter, 8, 0)

    var i = 0
    while (i < arraySize) {
      writer.writeLong(longValues(i))
      i += 1
    }

    val offset = writer.getStartingOffset
    val count = writer.complete()

    // Verify output is valid UnsafeArrayData
    val size = rowWriter.cursor() - offset
    val arrayData = new UnsafeArrayData
    arrayData.pointTo(rowWriter.getBuffer, offset, size)
    bh.consume(arrayData.numElements())
  }
}
