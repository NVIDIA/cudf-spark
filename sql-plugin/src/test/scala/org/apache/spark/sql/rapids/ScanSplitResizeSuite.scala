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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.perf.ScanSplitHeuristic
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}

/** Needs a local SparkSession. */
class ScanSplitResizeSuite extends AnyFunSuite {

  private def chunk(path: String, start: Long, length: Long): PartitionedFile =
    PartitionedFileUtilsShim.newPartitionedFile(InternalRow(7), path, start, length)

  test("resizeSplits re-packs planned partitions at the learned split") {
    val spark = SparkSession.builder().master("local[1]")
      .config("spark.sql.files.openCostInBytes", "0")
      .getOrCreate()
    try {
      val planned: Array[InputPartition] = Array(
        FilePartition(0, Array(chunk("file:/d/a", 0, 40), chunk("file:/d/b", 0, 30))),
        FilePartition(1, Array(chunk("file:/d/a", 40, 40))))
      val parts = ScanSplitHeuristic.resizeSplits(spark, planned, 100L, _ => true)
        .map(_.asInstanceOf[FilePartition])
      assert(parts.map(_.index).toSeq == parts.indices)
      assert(parts.map(_.files.map(f => (f.filePath.toString, f.start, f.length)).toSeq).toSeq ==
        Seq(Seq(("file:/d/a", 0L, 80L)), Seq(("file:/d/b", 0L, 30L))))
      assert(parts.flatMap(_.files).forall(_.partitionValues.getInt(0) == 7))
    } finally {
      spark.stop()
    }
  }
}
