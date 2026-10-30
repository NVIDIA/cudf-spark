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

import ai.rapids.cudf.{ColumnVector, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.sql.types.{DataType, LongType}

class CachedGpuBatchIteratorSuite extends SparkQueryCompareTestSuite {
  private val dataTypes = Array[DataType](LongType)

  test("later producer next failure closes cached output and preserves the error") {
    withGpuSparkSession { _ =>
      withResource(ColumnVector.fromLongs(1L, 2L)) { heldColumn =>
        val references = heldColumn.getRefCount
        val handles = SpillFramework.stores.deviceStore.numHandles
        val failure = new IllegalStateException("injected producer next failure")
        var nextCalls = 0
        var closeCalls = 0
        val producer = new GpuDataProducer[Table] {
          override def hasNext: Boolean = true

          override def next: Table = {
            nextCalls += 1
            if (nextCalls > 1) throw failure
            new Table(heldColumn)
          }

          override def close(): Unit = closeCalls += 1
        }
        val thrown = intercept[IllegalStateException] {
          CachedGpuBatchIterator(producer, dataTypes)
        }
        assert(thrown eq failure)
        assert(thrown.getSuppressed.isEmpty)
        assert(nextCalls == 2)
        assert(closeCalls == 1)
        assert(heldColumn.getRefCount == references)
        assert(SpillFramework.stores.deviceStore.numHandles == handles)
      }
    }
  }

  Seq(1, 3).foreach { chunks =>
    test(s"$chunks constructed producer chunks close when producer finalization fails") {
      withGpuSparkSession { _ =>
        withResource(ColumnVector.fromLongs(1L, 2L)) { heldColumn =>
          val references = heldColumn.getRefCount
          val handles = SpillFramework.stores.deviceStore.numHandles
          val failure = new IllegalStateException("injected producer close failure")
          var produced = 0
          var closeCalls = 0
          var referencesBeforeClose = 0
          var handlesBeforeClose = 0
          val producer = new GpuDataProducer[Table] {
            override def hasNext: Boolean = produced < chunks

            override def next: Table = {
              if (!hasNext) throw new NoSuchElementException()
              produced += 1
              new Table(heldColumn)
            }

            override def close(): Unit = {
              closeCalls += 1
              referencesBeforeClose = heldColumn.getRefCount
              handlesBeforeClose = SpillFramework.stores.deviceStore.numHandles
              throw failure
            }
          }
          val thrown = intercept[IllegalStateException] {
            CachedGpuBatchIterator(producer, dataTypes)
          }
          assert(thrown eq failure)
          assert(thrown.getSuppressed.isEmpty)
          assert(produced == chunks)
          assert(closeCalls == 1)
          // A real output existed when finalization failed, so these checks exercise
          // cleanup of the otherwise-unreturned single batch or pending spillable queue.
          assert(referencesBeforeClose > references)
          if (chunks > 1) assert(handlesBeforeClose > handles)
          assert(heldColumn.getRefCount == references)
          assert(SpillFramework.stores.deviceStore.numHandles == handles)
        }
      }
    }
  }
}
