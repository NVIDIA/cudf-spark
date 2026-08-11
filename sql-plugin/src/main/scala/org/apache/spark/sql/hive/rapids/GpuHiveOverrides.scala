/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.hive.rapids

import com.nvidia.spark.rapids.{DataWritingCommandRule, ExecRule, ExprRule, HiveProvider, RunnableCommandRule, ShimLoaderTemp, ShimReflectionUtils}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{DataWritingCommand, RunnableCommand}

object GpuHiveOverrides {
  val isSparkHiveAvailable: Boolean = {
    try {
      ShimReflectionUtils.loadClass("org.apache.spark.sql.hive.HiveSessionStateBuilder")
      ShimReflectionUtils.loadClass("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  private lazy val hiveProvider: HiveProvider = {
    if (isSparkHiveAvailable) {
      ShimLoaderTemp.newHiveProvider()
    } else {
      new HiveProvider() {
        override def getDataWriteCmds: Map[Class[_ <: DataWritingCommand],
            DataWritingCommandRule[_ <: DataWritingCommand]] = Map.empty

        override def getRunnableCmds: Map[Class[_ <: RunnableCommand],
            RunnableCommandRule[_ <: RunnableCommand]] = Map.empty
        override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Map.empty
        override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Map.empty
      }
    }
  }


  /**
   * Builds the data writing command rules that are specific to spark-hive Catalyst nodes.
   * This will return an empty mapping if spark-hive is unavailable
   */
  def dataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = hiveProvider.getDataWriteCmds

  def runnableCmds = hiveProvider.getRunnableCmds

  /**
   * Builds the expression rules that are specific to spark-hive Catalyst nodes
   * This will return an empty mapping if spark-hive is unavailable.
   */
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = hiveProvider.getExprs
  def execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = hiveProvider.getExecs
}
