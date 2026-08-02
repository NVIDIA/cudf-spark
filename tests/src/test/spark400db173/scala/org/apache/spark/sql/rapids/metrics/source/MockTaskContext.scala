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
package org.apache.spark.sql.rapids.metrics.source

import org.apache.spark.TaskContext
import org.apache.spark.util.TaskInterruptListener

class MockTaskContext(taskAttemptId: Long, partitionId: Int)
    extends MockTaskContextBase(taskAttemptId, partitionId) {

  override def addTaskInterruptListener(
      listener: TaskInterruptListener): TaskContext = this

  // Databricks 17.3 uses a parenthesized method unlike Apache Spark 4.2's getter.
  override private[spark] def getTaskFailure(): Option[Throwable] = None
}
