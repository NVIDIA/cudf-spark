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

package org.apache.spark.sql.delta.deletionvectors

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.closeOnExcept
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor

// scalastyle:off line.size.limit
/**
 * RAPIDS version of [[DeletionVectorStoredBitmap]]. It is simplified and modified to only include
 * the APIs needed to load serialized deletion vectors into host memory.
 *
 * This version does not support inline deletion vectors as they are used only for CDC in Delta IO.
 * See here for details: https://github.com/delta-io/delta/blob/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/commands/cdc/CDCReader.scala#L1076-L1083
 */
// scalastyle:on line.size.limit
case class RapidsDeletionVectorStoredBitmap(
    dvDescriptor: DeletionVectorDescriptor,
    tableDataPath: Path
) {
  require(dvDescriptor.isOnDisk, "Only on-disk deletion vectors are supported")

  def load(dvStore: RapidsDeletionVectorStore): HostMemoryBuffer = {
    val buffer = if (isEmpty) {
      RapidsDeletionVectorStoredBitmap.serializedEmptyBitmap()
    } else {
      dvStore.load(onDiskPath, dvDescriptor.offset.getOrElse(0), dvDescriptor.sizeInBytes)
    }

    buffer
  }

  private def isEmpty: Boolean = dvDescriptor.isEmpty

  /** The absolute path for on-disk deletion vectors. */
  private lazy val onDiskPath: Path = dvDescriptor.absolutePath(tableDataPath)
}

object RapidsDeletionVectorStoredBitmap {

  // scalastyle:off line.size.limit
  /**
   * Return a serialized empty bitmap in host memory buffer. For details of the serialization
   * format, see:
   * https://github.com/RoaringBitmap/RoaringFormatSpec/blob/8c4f7c7087c2a3a4fa560a34c669be673264f3ad/README.md#extension-for-64-bit-implementations
   */
  // scalastyle:on line.size.limit
  def serializedEmptyBitmap(): HostMemoryBuffer = {
    closeOnExcept(HostMemoryBuffer.allocate(8)) { buffer =>
      buffer.setLong(0, 0L)
      buffer
    }
  }
}
