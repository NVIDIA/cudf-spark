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
import org.apache.spark.sql.execution.command.{DataWritingCommand, RunnableCommand}

/**
 * The subclass of HiveProvider imports spark-hive classes. This file should not imports
 * spark-hive because `class not found` exception may throw if spark-hive does not exist at
 * runtime. Details see: https://github.com/NVIDIA/spark-rapids/issues/5648
 */
trait HiveProvider {
  def getDataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]]

  def getRunnableCmds: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]]

  def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]]

  /**
   * Getter for Execs that are specific to Hive.
   */
  def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]]
}
