/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Exercises the actual iterator with CPU-only batches. */
class GpuCoalesceYieldBeforeLookaheadSuite extends AnyFunSuite {
  private case class BatchSpec(rows: Int, bytes: Long)

  private class TestBatch(val spec: BatchSpec) extends ColumnarBatch(Array.empty, spec.rows)

  private def specOf(batch: ColumnarBatch): BatchSpec = batch.asInstanceOf[TestBatch].spec

  private def batches(bytes: Long*): Seq[BatchSpec] =
    bytes.map(size => BatchSpec(rows = 1, bytes = size))

  /**
   * Each input has an independent row count and simulated byte size. A TestBatch stores
   * that size explicitly; no GPU buffers are allocated.
   * The batches helper gives size-only tests one row per input. Row-boundary tests provide
   * explicit row counts instead. This tests batching and lookahead, not GPU memory usage.
   * The upstream hasNext reads and buffers a batch; next transfers that batch to the coalescer.
   * All inputs are available from the start; counters detect unwanted reads.
   */
  private class Fixture(
      inputs: Seq[BatchSpec],
      goal: CoalesceSizeGoal = TargetSize(100),
      enabled: Boolean = true,
      // Simulated cuDF row-count boundary; tests can lower it.
      cudfRowLimit: Int = Int.MaxValue,
      // Optional coalescing row limit; 0 means disabled.
      optionalBatchRowLimit: Int = 0) extends AutoCloseable {
    var inputBatchesRead = 0
    var inputBatchesDelivered = 0
    var upstreamHasNextCalls = 0
    var knownFinalInput: Option[Boolean] = None
    val readyBatchesYieldedMetric = new LocalGpuMetric
    private var deck: Option[ColumnarBatch] = None
    private var batchesToConcat = Vector.empty[ColumnarBatch]
    // Reader-owned buffer, separate from the coalescer's deck.
    private var bufferedInput: Option[ColumnarBatch] = None
    private val input = inputs.iterator
    private def bufferNextInput(): Unit = {
      if (bufferedInput.isEmpty && input.hasNext) {
        bufferedInput = Some(new TestBatch(input.next()))
        inputBatchesRead += 1
      }
    }
    val upstream: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        upstreamHasNextCalls += 1
        bufferNextInput()
        bufferedInput.nonEmpty
      }
      override def next(): ColumnarBatch = {
        bufferNextInput()
        val batch = bufferedInput.getOrElse(
          throw new NoSuchElementException("No more input batches"))
        bufferedInput = None
        inputBatchesDelivered += 1
        batch
      }
    }
    val iterator: AbstractGpuCoalesceIterator = new AbstractGpuCoalesceIterator(
        upstream, goal, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
        NoopMetric, NoopMetric, NoopMetric, "test",
        yieldBeforeLookahead = enabled, readyBatchesYielded = readyBatchesYieldedMetric) {
      override protected val supportsRetryIterator = false
      override protected val filteringModeRowsThreshold: Int = cudfRowLimit
      override def hasOnDeck: Boolean = deck.nonEmpty
      override def saveOnDeck(batch: ColumnarBatch): Unit = {
        assert(deck.isEmpty)
        deck = Some(batch)
      }
      override def clearOnDeck(): Unit = { deck.foreach(_.safeClose()); deck = None }
      override def popOnDeck(): ColumnarBatch = {
        val batch = deck.get
        deck = None
        batch
      }
      override def initNewBatch(batch: ColumnarBatch): Unit = {
        batchRowLimit = optionalBatchRowLimit
      }
      override def addBatchToConcat(batch: ColumnarBatch): Unit = {
        batchesToConcat :+= batch
      }
      override def hasAnyToConcat: Boolean = batchesToConcat.nonEmpty
      override def getBatchDataSize(batch: ColumnarBatch): Long = specOf(batch).bytes
      override def concatAllAndPutOnGPU(): ColumnarBatch = {
        val result = new TestBatch(BatchSpec(
          rows = batchesToConcat.map(_.numRows()).sum,
          bytes = batchesToConcat.map(getBatchDataSize).sum))
        cleanupConcatIsDone()
        result
      }
      override def getCoalesceRetryIterator: Iterator[ColumnarBatch] =
        throw new UnsupportedOperationException
      override def cleanupConcatIsDone(): Unit = {
        batchesToConcat.safeClose()
        batchesToConcat = Vector.empty
      }
      override protected def populateCandidateBatches(): Boolean = {
        val known = super.populateCandidateBatches()
        knownFinalInput = Some(known)
        known
      }
    }
    def nextOutputSpec(): BatchSpec = {
      assert(iterator.hasNext)
      Arm.withResource(iterator.next())(specOf)
    }
    override def close(): Unit = {
      val heldBatches = bufferedInput.toSeq ++ deck.toSeq ++ batchesToConcat
      bufferedInput = None
      deck = None
      batchesToConcat = Vector.empty
      heldBatches.safeClose()
    }
  }

  private def requiredGoals: Seq[CoalesceSizeGoal] = Seq(
    RequireSingleBatch, RequireSingleBatchWithFilter(GpuLiteral(true, BooleanType)))

  test("The feature is disabled by default, accepts true or false, and rejects invalid " +
      "settings.") {
    val entry = RapidsConf.COALESCE_BATCHES_YIELD_BEFORE_LOOKAHEAD
    val conf = new SQLConf
    assert(entry.key == "spark.rapids.sql.coalesceBatches.yieldBeforeLookahead")
    assert(!entry.get(conf))
    conf.setConfString(entry.key, "true")
    assert(entry.get(conf))
    conf.setConfString(entry.key, "false")
    assert(!entry.get(conf))
    conf.setConfString(entry.key, "invalid")
    intercept[IllegalArgumentException] { entry.get(conf) }
  }

  test("Once the current output batch is big enough, return it without asking " +
      "the upstream reader for more input.") {
    val inputs = batches(100, 40)
    Arm.withResource(new Fixture(inputs, enabled = false)) { f =>
      assert(f.iterator.hasNext)
      val before = f.upstreamHasNextCalls
      assert(f.nextOutputSpec() == BatchSpec(rows = 1, bytes = 100))
      assert(f.inputBatchesRead == 2 && f.inputBatchesDelivered == 2)
      assert(f.upstreamHasNextCalls == before + 1)
      assert(f.nextOutputSpec() == BatchSpec(rows = 1, bytes = 40) && !f.iterator.hasNext)
      assert(f.inputBatchesRead == 2 && f.inputBatchesDelivered == 2)
    }
    Arm.withResource(new Fixture(inputs, enabled = true)) { f =>
      assert(f.iterator.hasNext)
      val before = f.upstreamHasNextCalls
      assert(f.nextOutputSpec() == BatchSpec(rows = 1, bytes = 100))
      assert(f.inputBatchesRead == 1 && f.inputBatchesDelivered == 1)
      assert(f.upstreamHasNextCalls == before)
      assert(f.readyBatchesYieldedMetric.value == 1)
      assert(f.nextOutputSpec() == BatchSpec(rows = 1, bytes = 40) && !f.iterator.hasNext)
      assert(f.inputBatchesRead == 2 && f.inputBatchesDelivered == 2)
    }
  }

  test("Fixture sanity check (not yieldBeforeLookahead): " +
      "upstream hasNext reads and buffers one batch until next consumes it.") {
    val inputs = batches(100, 40)
    Arm.withResource(new Fixture(inputs)) { f =>
      assert(f.upstream.hasNext)
      assert(f.inputBatchesRead == 1 && f.inputBatchesDelivered == 0)
      assert(f.upstream.hasNext && f.inputBatchesRead == 1 && f.inputBatchesDelivered == 0)
      Arm.withResource(f.upstream.next()) { batch => assert(specOf(batch) == inputs.head) }
      assert(f.inputBatchesRead == 1 && f.inputBatchesDelivered == 1)
      assert(f.upstream.hasNext)
      assert(f.inputBatchesRead == 2 && f.inputBatchesDelivered == 1)
      Arm.withResource(f.upstream.next()) { batch => assert(specOf(batch) == inputs.last) }
      assert(!f.upstream.hasNext && f.inputBatchesRead == 2 && f.inputBatchesDelivered == 2)
      intercept[NoSuchElementException] { f.upstream.next() }
    }
  }

  test("Report the ready final batch as known last when disabled, but leave it unknown " +
      "when enabled.") {
    val inputs = Seq(BatchSpec(rows = 10, bytes = 100), BatchSpec(rows = 5, bytes = 100))
    // The row boundary stops the collection loop, leaving only the trailing finality probe.
    Arm.withResource(new Fixture(inputs, enabled = false, cudfRowLimit = 10)) { f =>
      assert(f.nextOutputSpec() == inputs.head)
      assert(f.upstreamHasNextCalls == 2 && f.inputBatchesRead == 2 && f.inputBatchesDelivered == 1)
      assert(f.knownFinalInput.contains(false))
      assert(f.nextOutputSpec() == inputs.last)
      assert(f.knownFinalInput.contains(true))
      assert(!f.iterator.hasNext)
      assert(f.inputBatchesRead == 2 && f.inputBatchesDelivered == 2)
    }
    Arm.withResource(new Fixture(inputs, enabled = true, cudfRowLimit = 10)) { f =>
      assert(f.nextOutputSpec() == inputs.head)
      assert(f.upstreamHasNextCalls == 1 && f.inputBatchesRead == 1 && f.inputBatchesDelivered == 1)
      assert(f.readyBatchesYieldedMetric.value == 1 && f.knownFinalInput.contains(false))
      assert(f.nextOutputSpec() == inputs.last)
      // B is actually last, but yielding skips the EOF probe.
      assert(f.knownFinalInput.contains(false))
      assert(!f.iterator.hasNext)
      assert(f.inputBatchesRead == 2 && f.inputBatchesDelivered == 2)
    }
  }

  test("If a batch saved for the next output already exceeds the target size, return it " +
      "without reading more input.") {
    Arm.withResource(new Fixture(batches(30, 120, 40))) { f =>
      assert(f.nextOutputSpec().bytes == 30 && f.inputBatchesDelivered == 2)
      assert(f.readyBatchesYieldedMetric.value == 0)
      val before = f.upstreamHasNextCalls
      assert(f.nextOutputSpec().bytes == 120 && f.inputBatchesDelivered == 2 &&
          f.upstreamHasNextCalls == before)
      assert(f.readyBatchesYieldedMetric.value == 1 && f.knownFinalInput.contains(false))
      assert(f.nextOutputSpec().bytes == 40 && !f.iterator.hasNext)
    }
  }

  test("Combine small inputs that fit together; save an input that would exceed the target " +
      "for the next output.") {
    Arm.withResource(new Fixture(batches(30, 40, 50))) { f =>
      assert(f.nextOutputSpec() == BatchSpec(rows = 2, bytes = 70) && f.inputBatchesDelivered == 3)
      assert(f.nextOutputSpec() == BatchSpec(rows = 1, bytes = 50) && !f.iterator.hasNext)
    }
  }

  test("When several inputs add up exactly to the target size, return them without reading " +
      "the next input.") {
    Arm.withResource(new Fixture(batches(30, 70, 20))) { f =>
      assert(f.nextOutputSpec() == BatchSpec(rows = 2, bytes = 100) &&
          f.inputBatchesDelivered == 2 && f.upstreamHasNextCalls == 2)
      assert(f.readyBatchesYieldedMetric.value == 1 && f.knownFinalInput.contains(false))
      assert(f.nextOutputSpec().bytes == 20 && !f.iterator.hasNext)
    }
  }

  test("Return an input bigger than the target unchanged, then return the remaining input " +
      "on the next request.") {
    Arm.withResource(new Fixture(batches(120, 40))) { f =>
      assert(f.nextOutputSpec().bytes == 120 && f.inputBatchesDelivered == 1 &&
          f.upstreamHasNextCalls == 1)
      assert(f.readyBatchesYieldedMetric.value == 1 && f.knownFinalInput.contains(false))
      assert(f.nextOutputSpec().bytes == 40 && !f.iterator.hasNext)
    }
  }

  test("Return a ready batch without checking whether it is last, even when it actually is " +
      "the last input.") {
    Arm.withResource(new Fixture(batches(120))) { f =>
      assert(f.iterator.hasNext)
      val before = f.upstreamHasNextCalls
      assert(f.nextOutputSpec().bytes == 120)
      assert(f.knownFinalInput.contains(false) && f.upstreamHasNextCalls == before)
      assert(!f.iterator.hasNext)
    }
  }

  test("A final 20-byte input returns below the 100-byte target and is recognized " +
      "as the last batch.") {
    Arm.withResource(new Fixture(batches(20))) { f =>
      assert(f.nextOutputSpec().bytes == 20 && f.knownFinalInput.contains(true))
      assert(!f.iterator.hasNext)
    }
  }

  test("An empty input produces no output, and input batches with zero rows are skipped.") {
    Arm.withResource(new Fixture(Seq.empty)) { f => assert(!f.iterator.hasNext) }
    val inputs = Seq(BatchSpec(0, 500), BatchSpec(1, 120), BatchSpec(0, 500), BatchSpec(1, 60))
    Arm.withResource(new Fixture(inputs)) { f =>
      assert(f.nextOutputSpec() == BatchSpec(rows = 1, bytes = 120) && f.inputBatchesDelivered == 2)
      assert(f.nextOutputSpec() == BatchSpec(rows = 1, bytes = 60) && !f.iterator.hasNext)
    }
  }

  test("Having many rows does not mean a batch is big enough to return; " +
      "check its byte size.") {
    val inputs = Seq(BatchSpec(rows = 120, bytes = 20), BatchSpec(rows = 80, bytes = 30))
    Arm.withResource(new Fixture(inputs)) { f =>
      assert(f.nextOutputSpec() == BatchSpec(rows = 200, bytes = 50))
      assert(f.readyBatchesYieldedMetric.value == 0 && f.knownFinalInput.contains(true))
      assert(!f.iterator.hasNext)
    }
  }

  test("Keep all rows even when their simulated byte size is zero.") {
    val inputs = Seq(BatchSpec(rows = 20, bytes = 0), BatchSpec(rows = 30, bytes = 0))
    Arm.withResource(new Fixture(inputs)) { f =>
      assert(f.nextOutputSpec() == BatchSpec(rows = 50, bytes = 0))
      assert(f.readyBatchesYieldedMetric.value == 0 && f.knownFinalInput.contains(true))
      assert(!f.iterator.hasNext)
    }
  }

  test("When a single output batch is required, consume all input without yielding early, " +
      "including when the final input reaches the row limit.") {
    requiredGoals.foreach { goal =>
      Arm.withResource(new Fixture(batches(100, 40), goal)) { f =>
        assert(f.nextOutputSpec() == BatchSpec(rows = 2, bytes = 140) &&
            f.inputBatchesDelivered == 2)
        assert(f.readyBatchesYieldedMetric.value == 0 && f.knownFinalInput.contains(true))
        assert(!f.iterator.hasNext)
      }
      val inputs = Seq(BatchSpec(rows = 100, bytes = 1))
      Arm.withResource(new Fixture(inputs, goal, cudfRowLimit = 100)) { f =>
        assert(f.nextOutputSpec() == inputs.head && f.knownFinalInput.contains(true))
        assert(f.readyBatchesYieldedMetric.value == 0 && !f.iterator.hasNext)
      }
    }
  }

  test("When a single output batch is required, reject leftover input whether it is " +
      "unread or already saved on deck.") {
    requiredGoals.foreach { goal =>
      val inputs = Seq(BatchSpec(rows = 100, bytes = 1), BatchSpec(rows = 40, bytes = 1))
      Arm.withResource(new Fixture(inputs, goal, cudfRowLimit = 100)) { f =>
        assert(f.iterator.hasNext)
        val before = f.upstreamHasNextCalls
        intercept[IllegalStateException] { f.nextOutputSpec() }
        assert(f.upstreamHasNextCalls == before + 1 && f.inputBatchesRead == 2 &&
            f.inputBatchesDelivered == 1)
        assert(f.readyBatchesYieldedMetric.value == 0 && f.knownFinalInput.isEmpty)
      }
      val onDeckInputs = Seq(BatchSpec(rows = 40, bytes = 1), BatchSpec(rows = 30, bytes = 1))
      Arm.withResource(new Fixture(onDeckInputs, goal, optionalBatchRowLimit = 50)) { f =>
        intercept[IllegalStateException] { f.nextOutputSpec() }
        assert(f.upstreamHasNextCalls == 2 && f.inputBatchesDelivered == 2)
        assert(f.readyBatchesYieldedMetric.value == 0)
      }
    }
  }
}
