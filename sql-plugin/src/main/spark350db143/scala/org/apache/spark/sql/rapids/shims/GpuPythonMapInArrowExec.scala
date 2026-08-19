/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PythonUDF}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.MapInArrowExec
import org.apache.spark.sql.rapids.execution.python.GpuMapInBatchExec

class GpuPythonMapInArrowExecMetaBase(
    mapArrow: MapInArrowExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[MapInArrowExec](mapArrow, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  protected val udf: BaseExprMeta[PythonUDF] = GpuOverrides.wrapExpr(
    mapArrow.func.asInstanceOf[PythonUDF], conf, Some(this))
  protected val resultAttrs: Seq[BaseExprMeta[Attribute]] =
    mapArrow.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = resultAttrs :+ udf

  override def convertToGpu(): GpuExec =
    GpuPythonMapInArrowExec(
      udf.convertToGpu(),
      resultAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      childPlans.head.convertIfNeeded(),
      isBarrier = false,
    )
}

/*
 * A relation produced by applying a function that takes an iterator of PyArrow's record
 * batches and outputs an iterator of PyArrow's record batches.
 *
 * This GpuMapInPandasExec aims at accelerating the data transfer between
 * JVM and Python, and scheduling GPU resources for its Python processes.
 *
 */
case class GpuPythonMapInArrowExec(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    override val isBarrier: Boolean) extends GpuMapInBatchExec {

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_ARROW_ITER_UDF
}
