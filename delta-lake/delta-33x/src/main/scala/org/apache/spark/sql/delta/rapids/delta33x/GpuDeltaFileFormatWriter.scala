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

package org.apache.spark.sql.delta.rapids.delta33x

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.sql.delta.files.DeltaFileFormatWriter.PartitionedTaskAttemptContextImpl
import org.apache.spark.sql.rapids.{GpuFileFormatWriterBase, GpuWriteJobDescription}

object GpuDeltaFileFormatWriter extends GpuFileFormatWriterBase {

  override def createTaskAttemptContext(description: GpuWriteJobDescription,
      hadoopConf: Configuration,
      taskAttemptId: TaskAttemptID): TaskAttemptContext = {
    if (description.partitionColumns.isEmpty) {
      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    } else {
      val partitionColumnToDataType = description.partitionColumns
        .map(attr => (attr.name, attr.dataType)).toMap
      new PartitionedTaskAttemptContextImpl(hadoopConf, taskAttemptId, partitionColumnToDataType)
    }
  }
}
