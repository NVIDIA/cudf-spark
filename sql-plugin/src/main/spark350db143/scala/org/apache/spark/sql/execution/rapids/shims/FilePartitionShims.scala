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


/*** spark-rapids-shim-json-lines
{"spark": "350db143"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.rapids.shims

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._

object FilePartitionShims extends SplitFiles {
  def getPartitions(selectedPartitions: Array[PartitionDirectory]): Array[PartitionedFile] = {
    selectedPartitions.flatMap { p =>
      p.files.map { f =>
        PartitionedFileUtil.getPartitionedFile(f, p.values, Some(SparkPath.fromPath(f.getPath)))
      }
    }
  }

  def getFiles(p: FilePartition): Array[PartitionedFile] = p.files

  def copyWithFiles(p: FilePartition, newFiles: Array[PartitionedFile]): FilePartition = {
    p.copy(files = newFiles)
  }

  def getFilePartitions(
      relation: HadoopFsRelation,
      splitFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)
  }
}
