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

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.rapids.DeltaCommandShims

/**
 * Delta 3.3.x implementation of command shims.
 * Uses the original Spark 3.x APIs.
 */
trait Delta33xCommandShims extends DeltaCommandShims {
  override type ShimSparkSession = SparkSession
  override type OperationSparkSession = SparkSession

  override def toOperationSparkSession(spark: ShimSparkSession): OperationSparkSession = spark

  override def getActiveOperationSparkSession: OperationSparkSession = SparkSession.active

  override def createDataFrame(
      spark: OperationSparkSession,
      logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, logicalPlan)
  }

  override def exprToColumn(expr: Expression): Column = new Column(expr)

  override def recacheByPlan(spark: ShimSparkSession, plan: LogicalPlan): Unit = {
    spark.sharedState.cacheManager.recacheByPlan(spark, plan)
  }

  // GpuOptimisticTransaction helper methods for Spark 3.3.x
  protected def getActiveSparkSession: SparkSession = SparkSession.active

  protected def createDataFrameForStats(
      spark: SparkSession,
      plan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, plan)
  }

  protected def postProcessStatsExpr(expr: Expression): Expression = expr
}

object Delta33xCommandShims extends Delta33xCommandShims
