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

package com.nvidia.spark.rapids.delta

import ai.rapids.cudf.{ColumnVector, Scalar}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuLeafExpression, GpuNondeterministic}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.TaskContext
import org.apache.spark.sql.rapids.catalyst.expressions.GpuExpressionRetryable
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of Delta's `GenerateIdentityValues`, which fills a Delta identity column. It
 * mirrors the CPU `PartitionIdentityValueGenerator`: partition `p` of an `n`-partition task
 * yields `base + p * step`, then advances by `n * step` per row, where `base` is `start`, or
 * the high water mark plus `step` when a high water mark exists. Values are unique and
 * increasing within a partition and interleaved across partitions.
 *
 * It takes the generator's fields rather than the Databricks Delta type; the rule extracts
 * them. Arithmetic overflow raises `ArithmeticException`, as on the CPU.
 *
 * It is evaluated on every row of a batch, like `monotonically_increasing_id`, rather than
 * only on the rows of the `CASE WHEN` branch that uses it. The conditional would otherwise
 * filter the whole, wide, input batch just to count rows, which cost far more than the
 * expression. The consequence is gaps in the values (rows that keep their key still advance
 * the sequence), which an identity column allows: values are unique and increasing, not
 * consecutive.
 *
 * Evaluating every row can also overflow where the CPU would not, so the planner keeps the
 * expression on the CPU when the sequence starts within the outer half of the Long range or the
 * step is large (see `unsafeToAccelerate`).
 *
 * It is retryable: a checkpoint saves the next value and a restore rewinds to it, so a batch
 * retried after an OOM gets the same values.
 */
case class GpuGenerateIdentityValues(
    start: Long,
    step: Long,
    highWaterMarkOpt: Option[Long],
    doContextCheck: Boolean)
  extends GpuLeafExpression with GpuNondeterministic with GpuExpressionRetryable {

  override lazy val deterministic: Boolean = false
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
  override def prettyName: String = "gpu_generateidentityvalues"

  @transient private var nextValue: Long = _
  @transient private var increment: Long = _
  @transient private var checkpointed: Long = _

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    val base = highWaterMarkOpt.map(Math.addExact(_, step)).getOrElse(start)
    // Like the CPU generator, the stride is the task's partition count (1 outside a task).
    val numPartitions = Option(TaskContext.get()).map(_.numPartitions()).getOrElse(1)
    nextValue = Math.addExact(base, Math.multiplyExact(partitionIndex.toLong, step))
    increment = Math.multiplyExact(numPartitions.toLong, step)
  }

  override def doCheckpoint(): Unit = {
    // A checkpoint taken before the first evaluation must save the initial value, not 0.
    ensureInitialized()
    checkpointed = nextValue
  }

  override def doRestore(): Unit = nextValue = checkpointed

  override def doColumnarEval(batch: ColumnarBatch): GpuColumnVector = {
    ensureInitialized()
    val numRows = batch.numRows()
    val first = nextValue
    // Validate the whole batch up front: the CPU also fails once the value after the last row
    // would overflow.
    val next = if (numRows > 0) {
      val last = Math.addExact(first, Math.multiplyExact(numRows - 1L, increment))
      Math.addExact(last, increment)
    } else {
      first
    }
    val result = withResource(Scalar.fromLong(first)) { init =>
      withResource(Scalar.fromLong(increment)) { inc =>
        GpuColumnVector.from(ColumnVector.sequence(init, inc, numRows), dataType)
      }
    }
    // Advance only once the column exists, so a failure above leaves the state untouched.
    nextValue = next
    result
  }
}

object GpuGenerateIdentityValues {
  /**
   * Largest |step| accelerated. The sequence advances for every row a task evaluates, across
   * all its batches, so the range it spans is `rowsPerTask * numPartitions * |step|`; neither
   * factor is known at planning time. The planner also keeps the first value within +-2^62,
   * leaving 2^62 of headroom. With |step| <= 2^20 that headroom is exhausted only when
   * `rowsPerTask * numPartitions` reaches 2^42 (about 4.4e12), far above any realistic task, so
   * generating values for rows the CPU would skip cannot overflow where the CPU would not.
   */
  val MaxAcceleratedStep: Long = 1L << 20

  /**
   * Returns a fallback reason when generating a value for rows the CPU would skip could
   * overflow spuriously: the sequence starts close to the Long limits, or the step is large.
   */
  def unsafeToAccelerate(
      start: Long,
      step: Long,
      highWaterMarkOpt: Option[Long]): Option[String] = {
    val first = highWaterMarkOpt.getOrElse(start)
    if (first > Long.MaxValue / 2 || first < Long.MinValue / 2) {
      Some(s"identity values starting at $first are too close to the Long limits; the GPU " +
        "generates a value for every row and could overflow where the CPU would not")
    } else if (step == Long.MinValue || Math.abs(step) > MaxAcceleratedStep) {
      Some(s"identity step $step is larger than $MaxAcceleratedStep; the GPU generates a " +
        "value for every row and could overflow where the CPU would not")
    } else {
      None
    }
  }
}
