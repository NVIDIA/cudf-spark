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
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec}

/**
 * Shim for FileCommitProtocol.newTaskTempFile API in Spark 4.1.0+.
 * Uses the new (spec: FileNameSpec) signature instead of deprecated (ext: String).
 */
object FileCommitProtocolShims {
  def newTaskTempFile(
      committer: FileCommitProtocol,
      taskContext: TaskAttemptContext,
      dir: Option[String],
      ext: String): String = {
    // FileNameSpec(prefix, suffix) - we put ext as suffix with empty prefix
    committer.newTaskTempFile(taskContext, dir, FileNameSpec("", ext))
  }

  def newTaskTempFileAbsPath(
      committer: FileCommitProtocol,
      taskContext: TaskAttemptContext,
      absoluteDir: String,
      ext: String): String = {
    // FileNameSpec(prefix, suffix) - we put ext as suffix with empty prefix
    committer.newTaskTempFileAbsPath(taskContext, absoluteDir, FileNameSpec("", ext))
  }
}
