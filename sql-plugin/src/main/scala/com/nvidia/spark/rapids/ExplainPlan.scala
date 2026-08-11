/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import scala.util.control.NonFatal

import org.apache.spark.sql.DataFrame

// Base trait visible publicly outside of parallel world packaging.
// It can't be named the same as ExplainPlan object to allow calling from PySpark.
trait ExplainPlanBase {
  def explainPotentialGpuPlan(df: DataFrame, explain: String = "ALL"): String
}

object ExplainPlan {
  /**
   * Looks at the CPU plan associated with the dataframe and outputs information
   * about which parts of the query the RAPIDS Accelerator for Apache Spark
   * could place on the GPU. This only applies to the initial plan, so if running
   * with adaptive query execution enable, it will not be able to show any changes
   * in the plan due to that.
   *
   * This is very similar output you would get by running the query with the
   * Rapids Accelerator enabled and with the config `spark.rapids.sql.enabled` enabled.
   *
   * Requires the RAPIDS Accelerator for Apache Spark jar and RAPIDS cudf jar be included
   * in the classpath but the RAPIDS Accelerator for Apache Spark should be disabled.
   *
   * {{{
   *   val output = com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df)
   * }}}
   *
   * Calling from PySpark:
   *
   * {{{
   *   output = sc._jvm.com.nvidia.spark.rapids.ExplainPlan.explainPotentialGpuPlan(df._jdf, "ALL")
   * }}}
   *
   * @param df The Spark DataFrame to get the query plan from
   * @param explain If ALL returns all the explain data, otherwise just returns what does not
   *                work on the GPU. Default is ALL.
   * @return String containing the explained plan.
   * @throws java.lang.IllegalArgumentException if an argument is invalid or it is unable to
   *         determine the Spark version
   * @throws java.lang.IllegalStateException if the plugin gets into an invalid state while trying
   *         to process the plan or there is an unexepected exception.
   */
  @throws[IllegalArgumentException]
  @throws[IllegalStateException]
  def explainPotentialGpuPlan(df: DataFrame, explain: String = "ALL"): String = {
    try {
      ShimLoaderTemp.newExplainPlan.explainPotentialGpuPlan(df, explain)
    } catch {
      case ia: IllegalArgumentException => throw ia
      case is: IllegalStateException => throw is
      case NonFatal(e) =>
        val msg = "Unexpected exception trying to run explain on the plan!"
        throw new IllegalStateException(msg, e)
    }
  }
}
