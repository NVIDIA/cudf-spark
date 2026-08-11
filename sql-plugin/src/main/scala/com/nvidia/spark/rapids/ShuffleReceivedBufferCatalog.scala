/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{DeviceMemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableColumn
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.spill.SpillableDeviceBufferHandle

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class RapidsShuffleHandle(
    spillable: SpillableDeviceBufferHandle, tableMeta: TableMeta) extends AutoCloseable {
  override def close(): Unit = {
    spillable.safeClose()
  }
}

/** Catalog for lookup of shuffle buffers by block ID */
class ShuffleReceivedBufferCatalog() extends Logging {

  /**
   * Adds a buffer to the device storage, taking ownership of the buffer.
   *
   * This method associates a new `bufferId` which is tracked internally in this catalog.
   *
   * @param buffer buffer that will be owned by the store
   * @param tableMeta metadata describing the buffer layout
   * @param initialSpillPriority starting spill priority value for the buffer
   * @param needsSync tells the store a synchronize in the current stream is required
   *                  before storing this buffer
   * @return RapidsShuffleHandle associated with this buffer
   */
  def addBuffer(
      buffer: DeviceMemoryBuffer,
      tableMeta: TableMeta,
      initialSpillPriority: Long): RapidsShuffleHandle = {
    RapidsShuffleHandle(SpillableDeviceBufferHandle(buffer), tableMeta)
  }

  /**
   * Adds a degenerate batch (zero rows or columns), described only by metadata.
   *
   * @param meta metadata describing the buffer layout
   * @return RapidsShuffleHandle associated with this buffer
   */
  def addDegenerateBatch(meta: TableMeta): RapidsShuffleHandle  = {
    RapidsShuffleHandle(null, meta)
  }

  def getColumnarBatchAndRemove(handle: RapidsShuffleHandle,
                                sparkTypes: Array[DataType]): (ColumnarBatch, Long) = {
    withResource(handle) { _ =>
      val spillable = handle.spillable
      var memoryUsedBytes = 0L
      val cb = if (spillable != null) {
        memoryUsedBytes = spillable.sizeInBytes
        withResource(spillable.materialize()) { buff =>
          MetaUtils.getBatchFromMeta(buff, handle.tableMeta, sparkTypes)
        }
      } else {
        val rowCount = handle.tableMeta.rowCount
        val packedMeta = handle.tableMeta.packedMetaAsByteBuffer()
        if (packedMeta != null) {
          withResource(DeviceMemoryBuffer.allocate(0)) { deviceBuffer =>
            withResource(Table.fromPackedTable(
              handle.tableMeta.packedMetaAsByteBuffer(), deviceBuffer)) { table =>
              GpuColumnVectorFromBuffer.from(
                table, deviceBuffer, handle.tableMeta, sparkTypes)
            }
          }
        } else {
          // no packed metadata, must be a table with zero columns
          new ColumnarBatch(Array.empty, rowCount.toInt)
        }
      }
      (cb, memoryUsedBytes)
    }
  }
}
