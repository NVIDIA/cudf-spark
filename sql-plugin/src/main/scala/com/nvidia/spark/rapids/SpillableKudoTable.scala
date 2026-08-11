/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.jni.kudo.KudoTable
import com.nvidia.spark.rapids.jni.kudo.KudoTableHeader


class SpillableKudoTable(val header: KudoTableHeader,
    val length: Long,
    shb: SpillableHostBuffer)
  extends AutoCloseable {

  def makeKudoTable: KudoTable = {
    if (shb == null) {
      new KudoTable(header, null)
    } else {
      new KudoTable(header, shb.getHostBuffer())
    }
  }

  override def toString: String =
    "SpillableKudoTable{header=" + this.header + ", shb=" + this.shb + '}'

  override def close(): Unit = {
    if (shb != null) shb.close()
  }
}

object SpillableKudoTable {
  def apply(header: KudoTableHeader, buffer: HostMemoryBuffer): SpillableKudoTable = {
    if (buffer == null) {
      new SpillableKudoTable(header, 0, null)
    } else {
      new SpillableKudoTable(
        header,
        buffer.getLength,
        SpillableHostBuffer.apply(
          buffer,
          buffer.getLength,
          SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      )
    }
  }
}