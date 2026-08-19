/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuGreaterThan
import org.apache.spark.sql.rapids.catalyst.expressions.GpuRand
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class NonDeterministicRetrySuite extends RmmSparkRetrySuiteBase {
  private val NUM_ROWS = 500
  private val RAND_SEED = 10
  private val batchAttrs = Seq(AttributeReference("int", IntegerType)(ExprId(10)))

  private def buildBatch(ints: Seq[Int] = 0 until NUM_ROWS): ColumnarBatch = {
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)), ints.length)
  }

  private def newGpuRand(ctxCheck: Boolean=false) =
    GpuRand(GpuLiteral(RAND_SEED, IntegerType), ctxCheck)

  test("GPU rand outputs the same sequence with checkpoint and restore") {
    val gpuRand = newGpuRand()
    withResource(buildBatch()) { inputCB =>
      // checkpoint the state
      gpuRand.checkpoint()
      val randHCol1 = withResource(gpuRand.columnarEval(inputCB)) { randCol1 =>
        randCol1.copyToHost()
      }
      withResource(randHCol1) { _ =>
        assert(randHCol1.getRowCount.toInt == NUM_ROWS)
        // Restore the state, and generate data again
        gpuRand.restore()
        val randHCol2 = withResource(gpuRand.columnarEval(inputCB)) { randCol2 =>
          randCol2.copyToHost()
        }
        withResource(randHCol2) { _ =>
          // check the two random columns are equal.
          assert(randHCol1.getRowCount == randHCol2.getRowCount)
          (0 until randHCol1.getRowCount.toInt).foreach { pos =>
            assert(randHCol1.getDouble(pos) == randHCol2.getDouble(pos))
          }
        }
      }
    }
  }

  test("GPU project retry with GPU rand") {
    def projectRand(): Seq[GpuExpression] = Seq(GpuAlias(newGpuRand(), "rand")())

    Seq(true, false).foreach { useTieredProject =>
      val conf = new SQLConf()
      conf.setConfString(RapidsConf.ENABLE_TIERED_PROJECT.key, useTieredProject.toString)
      // expression should be retryable
      val boundProjectRand = GpuBindReferences.bindGpuReferencesTiered(projectRand(),
        batchAttrs, conf, Map.empty)
      assert(boundProjectRand.areAllRetryable)
      // project with and without retry
      val batches = Seq(true, false).safeMap { forceRetry =>
        val boundProjectList = GpuBindReferences.bindGpuReferencesTiered(
          projectRand() ++ batchAttrs, batchAttrs, conf, Map.empty)
        assert(boundProjectList.areAllRetryable)

        val sb = closeOnExcept(buildBatch()) { cb =>
          SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
        closeOnExcept(sb) { _ =>
          if (forceRetry) {
            RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
              RmmSpark.OomInjectionType.GPU.ordinal, 0)
          }
        }
        boundProjectList.projectAndCloseWithRetrySingleBatch(sb)
      }
      // check the random columns
      val randCols = withResource(batches) { case Seq(retriedBatch, batch) =>
        assert(retriedBatch.numRows() == batch.numRows())
        assert(retriedBatch.numCols() == batch.numCols())
        batches.safeMap(_.column(0).asInstanceOf[GpuColumnVector].copyToHost())
      }
      withResource(randCols) { case Seq(retriedRand, rand) =>
        (0 until rand.getRowCount.toInt).foreach { pos =>
          assert(retriedRand.getDouble(pos) == rand.getDouble(pos))
        }
      }
    }
  }

  test("GPU filter retry with GPU rand") {
    def filterRand(): Seq[GpuExpression] = Seq(
      GpuGreaterThan(
        newGpuRand(),
        GpuLiteral.create(0.1d, DoubleType)))

    Seq(true, false).foreach { useTieredProject =>
      val conf = new SQLConf()
      conf.setConfString(RapidsConf.ENABLE_TIERED_PROJECT.key, useTieredProject.toString)
      // filter with and without retry
      val tables = Seq(true, false).safeMap { forceRetry =>
        val boundCondition = GpuBindReferences.bindGpuReferencesTiered(filterRand(),
          batchAttrs, conf, Map.empty)
        assert(boundCondition.areAllRetryable)

        val cb = buildBatch()
        if (forceRetry) {
          RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
            RmmSpark.OomInjectionType.GPU.ordinal, 0)
        }
        val batchSeq = GpuFilter.filterAndClose(cb, boundCondition,
          NoopMetric, NoopMetric, NoopMetric).toSeq
        withResource(batchSeq) { _ =>
          val tables = batchSeq.safeMap(GpuColumnVector.from)
          if (tables.size == 1) {
            tables.head
          } else {
            withResource(tables) { _ =>
              assert(tables.size > 1)
              Table.concatenate(tables: _*)
            }
          }
        }
      }

      // check the outputs
      val cols = withResource(tables) { case Seq(retriedTable, table) =>
        assert(retriedTable.getRowCount == table.getRowCount)
        assert(retriedTable.getNumberOfColumns == table.getNumberOfColumns)
        tables.safeMap(_.getColumn(0).copyToHost())
      }
      withResource(cols) { case Seq(retriedInts, ints) =>
        (0 until ints.getRowCount.toInt).foreach { pos =>
          assert(retriedInts.getInt(pos) == ints.getInt(pos))
        }
      }
    }
  }

  test("GPU project with GPU rand for context check enabled") {
    // We dont check the output correctness, so it is ok to reuse the bound expressions.
    val boundCheckExprs = GpuBindReferences.bindGpuReferences(
      Seq(GpuAlias(newGpuRand(true), "rand")()),
      batchAttrs, Map.empty)

    // 1) Context check + no-retry + no checkpoint-restore
    assertThrows[IllegalStateException] {
      GpuProjectExec.projectAndClose(buildBatch(), boundCheckExprs, NoopMetric)
    }

    // 2) Context check + retry + no checkpoint-restore
    assertThrows[IllegalStateException] {
      RmmRapidsRetryIterator.withRetryNoSplit(buildBatch()) { cb =>
        GpuProjectExec.project(cb, boundCheckExprs)
      }
    }

    // 3) Context check + retry + checkpoint-restore
    //    This is the expected usage for the GPU Rand.
    Seq(true, false).foreach { forceOOM =>
      val scb = SpillableColumnarBatch(buildBatch(), SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      if (forceOOM) { // make a retrying really happen during the projection
        RmmSpark.forceRetryOOM(
          RmmSpark.getCurrentThreadId, 1, RmmSpark.OomInjectionType.GPU.ordinal, 0)
      }
      GpuProjectExec.projectAndCloseWithRetrySingleBatch(scb, boundCheckExprs).close()
    }
  }

  test("GPU project with GPU rand for context check disabled") {
    // We dont check the output correctness, so it is ok to reuse the bound expressions.
    val boundExprs = GpuBindReferences.bindGpuReferences(
      Seq(GpuAlias(newGpuRand(false), "rand")()),
      batchAttrs, Map.empty)

    // 1) No context check + no retry + no checkpoint-restore
    //    It works but not the expected usage for the GPU Rand
    GpuProjectExec.projectAndClose(buildBatch(), boundExprs, NoopMetric).close()

    // 2) No context check + retry (no real retrying) + no checkpoint-restore
    //    It works but not the expected usage for the GPU Rand
    RmmRapidsRetryIterator.withRetryNoSplit(buildBatch()) { cb =>
      GpuProjectExec.project(cb, boundExprs)
    }.close()

    // 3) No context check + retry (A retrying happens) + no checkpoint-restore
    assertThrows[IllegalStateException] {
      val cb = buildBatch()
      // Make a retrying really happen
      RmmSpark.forceRetryOOM(
        RmmSpark.getCurrentThreadId, 1, RmmSpark.OomInjectionType.GPU.ordinal, 0)
      RmmRapidsRetryIterator.withRetryNoSplit(cb)(GpuProjectExec.project(_, boundExprs))
    }

    // 4) No context check + retry + checkpoint-restore
    //    This is the expected usage for the GPU Rand
    Seq(true, false).foreach { forceOOM =>
      val scb = SpillableColumnarBatch(buildBatch(), SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      if (forceOOM) { // make a retrying really happen during the projection
        RmmSpark.forceRetryOOM(
          RmmSpark.getCurrentThreadId, 1, RmmSpark.OomInjectionType.GPU.ordinal, 0)
      }
      GpuProjectExec.projectAndCloseWithRetrySingleBatch(scb, boundExprs).close()
    }
  }
}
