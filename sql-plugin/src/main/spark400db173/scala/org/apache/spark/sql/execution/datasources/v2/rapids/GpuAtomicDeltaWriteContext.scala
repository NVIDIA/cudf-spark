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
package org.apache.spark.sql.execution.datasources.v2.rapids

import java.util.UUID

import org.apache.spark.SparkContext

/** Limits DBR's nested Delta data-writing command to a validated atomic CTAS/RTAS call stack. */
object GpuAtomicDeltaWriteContext {
  private val activeKey = "spark.rapids.sql.delta.atomicWrite.active"
  private val activeToken = UUID.randomUUID().toString

  def isActive: Boolean = SparkContext.getActive
    .exists(_.getLocalProperty(activeKey) == activeToken)

  def withAtomicWrite[T](body: => T): T = {
    val sparkContext = SparkContext.getActive.getOrElse(
      throw new IllegalStateException("No active SparkContext for atomic Delta write"))
    val previous = sparkContext.getLocalProperty(activeKey)
    sparkContext.setLocalProperty(activeKey, activeToken)
    try {
      body
    } finally {
      sparkContext.setLocalProperty(activeKey, previous)
    }
  }
}
