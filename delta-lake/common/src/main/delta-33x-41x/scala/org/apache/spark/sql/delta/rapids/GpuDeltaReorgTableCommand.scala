/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from DeltaReorgTableCommand.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.rapids

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, LeafCommand, LogicalPlan}
import org.apache.spark.sql.delta.commands.{DeltaOptimizeContext, DeltaPurgeOperation,
  DeltaReorgOperation, OptimizeTableCommandBase}

case class GpuDeltaReorgTableCommand(target: LogicalPlan)(val predicates: Seq[String])
  extends OptimizeTableCommandBase with LeafCommand with IgnoreCachedData {

  override val otherCopyArgs: Seq[AnyRef] = predicates :: Nil

  protected def reorgOperation: DeltaReorgOperation = new DeltaPurgeOperation()

  def optimizeByReorg(sparkSession: SparkSession): Seq[Row] = {
    val command = GpuOptimizeTableCommand(
      target,
      predicates,
      optimizeContext = DeltaOptimizeContext(
        reorg = Some(reorgOperation),
        minFileSize = Some(0L),
        maxDeletedRowsRatio = Some(0d))
    )(zOrderBy = Nil)
    command.run(sparkSession)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = optimizeByReorg(sparkSession)
}
