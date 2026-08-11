/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType

trait PlanShims {
  def extractExecutedPlan(plan: SparkPlan): SparkPlan
  def isAnsiCast(e: Expression): Boolean
  def isAnsiCastOptionallyAliased(e: Expression): Boolean

  /**
   * Extra Ansi Cast's source's data type and target's data type
   * @param e should be AnsiCast type or Cast under Ansi mode
   * @return (source data type, target data type)
   */
  def extractAnsiCastTypes(e: Expression): (DataType, DataType)
}

object PlanShims {
  private lazy val shims = ShimLoaderTemp.newPlanShims()

  def extractExecutedPlan(plan: SparkPlan): SparkPlan = {
    shims.extractExecutedPlan(plan)
  }

  def isAnsiCast(e: Expression): Boolean = {
    shims.isAnsiCast(e)
  }

  def isAnsiCastOptionallyAliased(e: Expression): Boolean = {
    shims.isAnsiCastOptionallyAliased(e)
  }

  def extractAnsiCastTypes(e: Expression): (DataType, DataType) = {
    shims.extractAnsiCastTypes(e)
  }
}