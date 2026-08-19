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
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.GpuScan

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

class GpuBatchScanExecHashSuite extends AnyFunSuite {
  private val scan = new GpuScan {
    override def readSchema(): StructType = new StructType()
    override def toBatch: Batch = EmptyBatch
    override def withInputFile(): GpuScan = this
    override def description(): String = "hash-test-scan"
  }

  private def exec(
      commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
      applyPartialClustering: Boolean = false,
      replicatePartitions: Boolean = false): GpuBatchScanExec = {
    GpuBatchScanExec(
      output = Nil,
      scan = scan,
      table = null,
      commonPartitionValues = commonPartitionValues,
      applyPartialClustering = applyPartialClustering,
      replicatePartitions = replicatePartitions)
  }

  test("hashCode includes Spark 3.4 SPJ fields") {
    val base = exec()
    assert(base.hashCode() != exec(commonPartitionValues = Some(Seq((InternalRow.empty, 1)))).hashCode())
    assert(base.hashCode() != exec(replicatePartitions = true).hashCode())
    assert(base.hashCode() != exec(applyPartialClustering = true).hashCode())
  }
}

private object EmptyBatch extends Batch {
  override def planInputPartitions(): Array[InputPartition] = Array.empty
  override def createReaderFactory(): PartitionReaderFactory = null
}
