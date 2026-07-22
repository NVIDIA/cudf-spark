/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.python.GpuWindowInPandasExecBase
import org.apache.spark.sql.vectorized.ColumnarBatch

/*
 * This GpuWindowInPandasExec aims at accelerating the data transfer between
 * JVM and Python, and scheduling GPU resources for Python processes
 */
case class GpuWindowInPandasExec(
    windowExpression: Seq[Expression],
    gpuPartitionSpec: Seq[Expression],
    cpuOrderSpec: Seq[SortOrder],
    child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression]) extends GpuWindowInPandasExecBase {

  override def otherCopyArgs: Seq[AnyRef] = cpuPartitionSpec :: Nil

  override final def pythonModuleKey: String = "spark"

  // Apache Spark expects input columns before the result columns
  override def output: Seq[Attribute] = child.output ++ windowExpression
    .map(_.asInstanceOf[NamedExpression].toAttribute)

  // Return the join batch directly per Apache Spark's expectation.
  override def projectResult(joinedBatch: ColumnarBatch): ColumnarBatch = joinedBatch
}
