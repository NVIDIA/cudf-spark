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

/*** spark-rapids-shim-json-lines
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.delta

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{GpuColumnVector, RmmSparkRetrySuiteBase}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuGenerateIdentityValuesSuite extends RmmSparkRetrySuiteBase {

  private def batch(rows: Int): ColumnarBatch = {
    new ColumnarBatch(Array(GpuColumnVector.from(
      ColumnVector.fromInts(Seq.fill(rows)(0): _*), IntegerType)), rows)
  }

  private def gen(
      start: Long = 1, step: Long = 1, hwm: Option[Long] = None): GpuGenerateIdentityValues =
    GpuGenerateIdentityValues(start, step, hwm, doContextCheck = false)

  private def eval(e: GpuGenerateIdentityValues, rows: Int): Seq[Long] = {
    withResource(batch(rows)) { b =>
      withResource(e.columnarEval(b).asInstanceOf[GpuColumnVector]) { col =>
        withResource(col.getBase.copyToHost()) { h =>
          (0 until rows).map(i => h.getLong(i.toLong))
        }
      }
    }
  }

  test("values continue across batches and skip zero-row batches") {
    val e = gen(start = 10, step = 5)
    e.initialize(0)
    assert(eval(e, 3) == Seq(10L, 15L, 20L))
    assert(eval(e, 0).isEmpty)
    assert(eval(e, 2) == Seq(25L, 30L))
  }

  test("negative step and high water mark") {
    val e = gen(start = 100, step = -3, hwm = Some(50))
    e.initialize(0)
    assert(eval(e, 3) == Seq(47L, 44L, 41L))
  }

  test("checkpoint and restore replay the same values") {
    val e = gen()
    e.initialize(0)
    assert(eval(e, 4) == Seq(1L, 2L, 3L, 4L))
    e.checkpoint()
    val first = eval(e, 3)
    e.restore()
    assert(eval(e, 3) == first)
    assert(first == Seq(5L, 6L, 7L))
  }

  test("checkpoint before the first evaluation restores the initial value") {
    val e = gen(start = 7, step = 2)
    // Deliberately not initialized: the checkpoint must not save the unset state (0).
    e.checkpoint()
    val first = eval(e, 2)
    e.restore()
    assert(eval(e, 2) == first)
    assert(first == Seq(7L, 9L))
  }

  test("partition index offsets the first value") {
    val e = gen(start = 1, step = 10)
    e.initialize(3)
    // Outside a task there is one partition, so the stride is the step.
    assert(eval(e, 2) == Seq(31L, 41L))
  }

  test("overflow raises ArithmeticException") {
    val e = gen(start = Long.MaxValue - 1, step = 1)
    e.initialize(0)
    assertThrows[ArithmeticException](eval(e, 3))
  }

  test("overflow while initializing raises ArithmeticException") {
    // The high water mark plus the step overflows.
    val hwm = gen(step = 1, hwm = Some(Long.MaxValue))
    assertThrows[ArithmeticException](hwm.initialize(0))
    // partitionIndex * step overflows.
    val stride = gen(start = 0, step = Long.MaxValue / 2)
    assertThrows[ArithmeticException](stride.initialize(3))
    // The base plus the partition offset overflows.
    val offset = gen(start = Long.MaxValue, step = 1)
    assertThrows[ArithmeticException](offset.initialize(1))
  }

  test("a failed evaluation leaves the state untouched") {
    val e = gen(start = Long.MaxValue - 1, step = 1)
    e.initialize(0)
    assertThrows[ArithmeticException](eval(e, 3))
    // The failed batch must not have advanced the sequence. One row is the most that fits: the
    // value after the last row must not overflow either.
    assert(eval(e, 1) == Seq(Long.MaxValue - 1))
  }

  test("planning guard keeps near-limit starts and large steps on the CPU") {
    val limit = Long.MaxValue / 2
    assert(GpuGenerateIdentityValues.unsafeToAccelerate(1, 1, None).isEmpty)
    assert(GpuGenerateIdentityValues.unsafeToAccelerate(1, 1, Some(limit)).isEmpty)
    assert(GpuGenerateIdentityValues.unsafeToAccelerate(1, 1, Some(limit + 1)).isDefined)
    assert(GpuGenerateIdentityValues.unsafeToAccelerate(Long.MinValue / 2, -1, None).isEmpty)
    assert(GpuGenerateIdentityValues.unsafeToAccelerate(Long.MinValue / 2 - 1, -1, None)
      .isDefined)
    val maxStep = GpuGenerateIdentityValues.MaxAcceleratedStep
    assert(GpuGenerateIdentityValues.unsafeToAccelerate(1, maxStep, None).isEmpty)
    assert(GpuGenerateIdentityValues.unsafeToAccelerate(1, -maxStep, None).isEmpty)
    assert(GpuGenerateIdentityValues.unsafeToAccelerate(1, maxStep + 1, None).isDefined)
    assert(GpuGenerateIdentityValues.unsafeToAccelerate(1, Long.MinValue, None).isDefined)
  }
}
