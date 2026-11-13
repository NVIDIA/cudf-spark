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

package org.apache.spark.sql.rapids

import org.apache.spark.{MapOutputTrackerMaster, SparkEnv}
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

object TestTrampolineUtil {
  def toLogicalPlan(df: DataFrame): LogicalPlan = df.logicalPlan

  def toDataFrame(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    createDataFrame(spark, plan)
  }

  /**
   * How many map outputs of a shuffle the driver holds as available, or None when the driver
   * holds no status for the shuffle.
   */
  def availableMapOutputs(shuffleId: Int): Option[Int] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    if (tracker.containsShuffle(shuffleId)) {
      Some(tracker.getNumAvailableOutputs(shuffleId))
    } else {
      None
    }
  }

  /** The shuffle that a stage writes, or None for a result stage. */
  def shuffleDepId(stageInfo: StageInfo): Option[Int] = stageInfo.shuffleDepId
}
