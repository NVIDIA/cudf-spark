/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.Rmm
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector.GpuArrowColumnarBatchBuilder
import com.nvidia.spark.rapids.jni.RmmSpark
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.IntVector

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

class HostColumnToGpuRetrySuite extends RmmSparkRetrySuiteBase {

  private val schema = StructType(Seq(StructField("a", IntegerType)))
  private val NUM_ROWS = 50

  private def buildArrowIntColumn(allocator: BufferAllocator): ColumnVector = {
    val intVector = new IntVector("intVector", allocator)
    intVector.allocateNew(NUM_ROWS)
    (0 until NUM_ROWS).foreach { pos =>
      intVector.set(pos, pos * 10)
    }
    intVector.setValueCount(NUM_ROWS)
    new ArrowColumnVector(intVector)
  }

  test("Arrow column builder with retry OOM") {
    withResource(new RootAllocator()) { allocator =>
      val batch = withResource(new GpuArrowColumnarBatchBuilder(schema)) { builder =>
        withResource(buildArrowIntColumn(allocator)) { arrowColumn =>
          builder.copyColumnar(arrowColumn, 0, NUM_ROWS)
        }
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
        RmmRapidsRetryIterator.withRetryNoSplit[ColumnarBatch] {
          builder.tryBuild(NUM_ROWS)
        }
      }
      withResource(batch) { _ =>
        assertResult(NUM_ROWS)(batch.numRows())
        assertResult(1)(batch.numCols())
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
          withResource(buildArrowIntColumn(allocator)) { arrowCol =>
            (0 until NUM_ROWS).foreach { pos =>
              assert(hostCol.getInt(pos) == arrowCol.getInt(pos))
            }
          }
        }
      }
    }
  }

  // Each piece takes one device allocation, so skipping one fails copying the second piece and
  // skipping two fails the concatenation.
  Seq(1 -> "the copy of a later one", 2 -> "their concatenation").foreach { case (skip, step) =>
    test(s"Arrow column builder frees copied pieces when a GPU OOM hits $step") {
      withResource(new RootAllocator()) { allocator =>
        val allocatedBefore = Rmm.getTotalBytesAllocated
        RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1)
        val batch = withResource(new GpuArrowColumnarBatchBuilder(schema)) { builder =>
          // Two input batches leave the column in two host pieces.
          (0 until 2).foreach { _ =>
            withResource(buildArrowIntColumn(allocator)) { arrowColumn =>
              builder.copyColumnar(arrowColumn, 0, NUM_ROWS)
            }
          }
          RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
            RmmSpark.OomInjectionType.GPU.ordinal, skip)
          RmmRapidsRetryIterator.withRetryNoSplit[ColumnarBatch] {
            builder.tryBuild(2 * NUM_ROWS)
          }
        }
        withResource(batch) { _ =>
          assertResult(2 * NUM_ROWS)(batch.numRows())
          withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
            (0 until 2 * NUM_ROWS).foreach { pos =>
              assertResult((pos % NUM_ROWS) * 10)(hostCol.getInt(pos))
            }
          }
        }
        assertResult(1)(RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1))
        assertResult(allocatedBefore)(Rmm.getTotalBytesAllocated)
      }
    }
  }

}
