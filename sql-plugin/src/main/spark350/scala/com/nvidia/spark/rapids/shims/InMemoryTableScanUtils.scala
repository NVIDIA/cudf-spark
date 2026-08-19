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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExecRule, GpuOverrides}

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.TableCacheQueryStageExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec

/**
 * Utility object for handling InMemoryTableScan version differences.
 * For Spark 3.5.0 and 3.5.1, we disable InMemoryTableScan by default due to 
 * missing InMemoryTableScanLike trait.
 */
object InMemoryTableScanUtils {
  
  /**
   * Modifies the InMemoryTableScan rule to be disabled by default for Spark 3.5.0-3.5.1.
   */
  def getInMemoryTableScanExecRule: ExecRule[_ <: SparkPlan] = {
    val imtsKey = classOf[InMemoryTableScanExec].asSubclass(classOf[SparkPlan])
    GpuOverrides.commonExecs.getOrElse(imtsKey,
        throw new IllegalStateException("InMemoryTableScan should be overridden by default before" +
        " Spark 3.5.0")).
      disabledByDefault(
        """there could be complications when using it with AQE with Spark-3.5.0 and Spark-3.5.1.
          |For more details please check
          |https://github.com/NVIDIA/spark-rapids/issues/10603""".stripMargin.replaceAll("\n", " "))
  }

  def canTableCacheWrapGpuInMemoryTableScan: Boolean = false

  /**
   * Gets the TableCacheQueryStageExec rule for this Spark version.
   * For Spark 3.5.0-3.5.1: neverReplaceExec because of missing InMemoryTableScanLike trait
   */
  def getTableCacheQueryStageExecRule: ExecRule[_ <: SparkPlan] = {
    GpuOverrides.neverReplaceExec[TableCacheQueryStageExec]("Table cache query stage")
  }
}
