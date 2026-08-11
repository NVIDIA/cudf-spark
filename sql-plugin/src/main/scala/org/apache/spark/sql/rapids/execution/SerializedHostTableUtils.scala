/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import java.io.{DataInputStream, InputStream}

import ai.rapids.cudf.{HostMemoryBuffer, JCudfSerialization}
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.RapidsHostColumnVector
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.types.DataType

object SerializedHostTableUtils {
  /**
   * Read in a cuDF serialized table into host memory from an input stream.
   */
  def readTableHeaderAndBuffer(
      in: InputStream): (JCudfSerialization.SerializedTableHeader, HostMemoryBuffer) = {
    val din = new DataInputStream(in)
    val header = new JCudfSerialization.SerializedTableHeader(din)
    if (!header.wasInitialized()) {
      throw new IllegalStateException("Could not read serialized table header")
    }
    closeOnExcept(HostMemoryBuffer.allocate(header.getDataLen)) { buffer =>
      JCudfSerialization.readTableIntoBuffer(din, header, buffer)
      if (!header.wasDataRead()) {
        throw new IllegalStateException("Could not read serialized table data")
      }
      (header, buffer)
    }
  }

  /**
   * Deserialize a cuDF serialized table to host build column vectors
   */
  def buildHostColumns(
      header: JCudfSerialization.SerializedTableHeader,
      buffer: HostMemoryBuffer,
      dataTypes: Array[DataType]): Array[RapidsHostColumnVector] = {
    assert(dataTypes.length == header.getNumColumns)
    closeOnExcept(JCudfSerialization.unpackHostColumnVectors(header, buffer)) { hostColumns =>
      assert(hostColumns.length == dataTypes.length)
      dataTypes.zip(hostColumns).safeMap { case (dataType, hostColumn) =>
        new RapidsHostColumnVector(dataType, hostColumn)
      }
    }
  }
}
