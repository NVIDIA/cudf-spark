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

package org.apache.spark.sql.rapids.execution

sealed trait BuildStatus

object BuildStatus {
  case object Cold extends BuildStatus
  case object Building extends BuildStatus
  case object Ready extends BuildStatus
  case object Evicted extends BuildStatus
}

private[execution] case class BuildDemand(probeCount: Long, probeRows: Long)

private[execution] object BuildDemand {
  val Empty: BuildDemand = BuildDemand(0L, 0L)
}

/** Identifies one executor-local run of a join to track build demands. */
case class HashBuildDemandId(joinId: Int, stageId: Int, stageAttempt: Int)
