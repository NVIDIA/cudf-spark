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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.shims

import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.io.FileCommitProtocol

/**
 * Shim for FileCommitProtocol.newTaskTempFile API.
 * In Spark <= 4.0.x, we use the deprecated (ext: String) signature.
 * In Spark 4.1.0+, we use the new (spec: FileNameSpec) signature.
 */
object FileCommitProtocolShims {
  @scala.annotation.nowarn(
    "msg=method newTaskTempFile in class FileCommitProtocol is deprecated"
  )
  def newTaskTempFile(
      committer: FileCommitProtocol,
      taskContext: TaskAttemptContext,
      dir: Option[String],
      ext: String): String = {
    committer.newTaskTempFile(taskContext, dir, ext)
  }

  @scala.annotation.nowarn(
    "msg=method newTaskTempFileAbsPath in class FileCommitProtocol is deprecated"
  )
  def newTaskTempFileAbsPath(
      committer: FileCommitProtocol,
      taskContext: TaskAttemptContext,
      absoluteDir: String,
      ext: String): String = {
    committer.newTaskTempFileAbsPath(taskContext, absoluteDir, ext)
  }
}
