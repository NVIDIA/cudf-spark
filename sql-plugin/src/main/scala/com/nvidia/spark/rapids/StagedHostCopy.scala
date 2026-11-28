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

import java.util.{ArrayList, Optional}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ColumnView, Cuda, HostColumnVector, HostColumnVectorCore, HostMemoryBuffer, PageableMemoryPool}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

/** Explicit column materialization above the staged buffer-copy pipeline. */
private[rapids] object StagedHostCopy {
  // The caller retains the device column until this synchronous operation returns.
  // Each buffer copy drains accepted work before returning, including on failure.
  def copyToHost(column: ColumnView, stream: Cuda.Stream): HostColumnVector = {
    closeOnExcept(new ArrayBuffer[AutoCloseable]()) { owned =>
      def copyBuffer(device: BaseDeviceMemoryBuffer): HostMemoryBuffer = {
        withResource(device) { source =>
          if (source == null) {
            null
          } else {
            val host = HostAlloc.allocPageableForDeviceToHost(source.getLength)
            owned += host
            PageableMemoryPool.copyDeviceToPageableHost(host, 0, source, 0,
              source.getLength, stream)
            host
          }
        }
      }

      val nullCount = Optional.of[java.lang.Long](column.getNullCount)
      val data = copyBuffer(column.getData)
      val validity = copyBuffer(column.getValid)
      val offsets = copyBuffer(column.getOffsets)
      val children = new ArrayList[HostColumnVectorCore]()
      for (i <- 0 until column.getNumChildren) {
        val child = withResource(column.getChildColumnView(i)) { view =>
          copyToHost(view, stream)
        }
        owned += child
        children.add(child)
      }
      // Successful construction transfers ownership of all buffers and children.
      new HostColumnVector(column.getType, column.getRowCount, nullCount,
        data, validity, offsets, children)
    }
  }
}
