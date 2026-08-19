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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.rapids.shims

import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._

object FilePartitionShims extends SplitFiles {
  def getPartitions(selectedPartitions: Array[PartitionDirectory]): Array[PartitionedFile] = {
    selectedPartitions.flatMap { p =>
      p.files.map { f =>
        PartitionedFileUtil.getPartitionedFile(f, f.getPath, p.values, 0, f.getLen)
      }
    }
  }

  def getFiles(p: FilePartition): Array[PartitionedFile] = p.filesWithAbsolutePaths

  def copyWithFiles(p: FilePartition, newFiles: Array[PartitionedFile]): FilePartition = {
    // The caller obtains newFiles through filesWithAbsolutePaths, so any relative paths have
    // already been resolved. Clear pathPrefix to avoid re-applying a relation root if this
    // partition is processed again.
    p.copy(innerFiles = newFiles, pathPrefix = None)
  }

  // Delta tables in DB-17.3, including UC-managed tables, may store relative file names in
  // FilePartition.innerFiles and resolve them with pathPrefix. When GPU planning rebuilds
  // partitions from split files, carry the single relation root forward so Spark keeps the
  // same resolution behavior.
  def getFilePartitions(
      relation: HadoopFsRelation,
      splitFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    val partitions =
      FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)
    val rootPaths = relation.location.rootPaths
    if (rootPaths.size == 1) {
      val prefix = rootPaths.head.toString
      partitions.map { p =>
        if (p.pathPrefix.isEmpty) p.copy(pathPrefix = Some(prefix)) else p
      }
    } else {
      // With multiple roots there is no single pathPrefix that can resolve every file safely.
      // Leave Spark's partitions unchanged.
      partitions
    }
  }
}
