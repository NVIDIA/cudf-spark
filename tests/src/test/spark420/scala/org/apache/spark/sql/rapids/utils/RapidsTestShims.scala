/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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
{"spark":["420"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.execution.SQLExecution

object RapidsTestShims {
  def logicalPlan(df: DataFrame): LogicalPlan = df.logicalPlan

  def analysisPlan(exception: AnalysisException): Option[LogicalPlan] = exception match {
    case extended: ExtendedAnalysisException => extended.plan
    case _ => None
  }

  def withSQLConfPropagated[T](df: DataFrame)(body: => T): T = {
    SQLExecution.withSQLConfPropagated(df.sparkSession)(body)
  }
}
