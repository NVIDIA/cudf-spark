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

import org.apache.spark.scheduler.SparkListener

/**
 * Hands the GPU shuffles that a SQL execution wrote to the UCX reclaim manager once the
 * execution ends.
 *
 * @param isCandidate whether a shuffle id is a GPU shuffle that the reclaim may free
 * @param reclaim hands an ended execution's id, its shuffles and their watermark to the manager
 */
class UcxSqlEndListener(
    isCandidate: Int => Boolean,
    reclaim: (Long, Seq[Int], Long) => Unit) extends SparkListener {

  /** A SQL execution started. */
  def onExecutionStart(executionId: Long): Unit = {}

  /** A SQL execution ended. */
  def onExecutionEnd(executionId: Long): Unit = {}

  /** Called when the driver plugin shuts down. */
  def shutdown(): Unit = {}
}
