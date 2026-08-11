/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.databricks.sql.execution.UnionWithLocalDataExec
import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.{SparkPlan, UnionExec}

trait Spark350db143Shims extends Spark341PlusDBShims {
  private val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[UnionExec](
      "The backend for the union operator",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT).nested()
        .withPsNote(TypeEnum.STRUCT,
          "unionByName will not optionally impute nulls for missing struct fields " +
          "when the column is a struct and there are non-overlapping fields"), TypeSig.all),
      (union, conf, p, r) => new SparkPlanMeta[UnionExec](union, conf, p, r) {
        private def hasUnionWithLocalDataExecParent: Boolean =
          p.exists(_.wrapped.isInstanceOf[UnionWithLocalDataExec])

        override def tagPlanForGpu(): Unit = {
          // DBR 14.3's local-source union path executes direct UnionExec children as rows.
          if (hasUnionWithLocalDataExecParent) {
            willNotWorkOnGpu("UnionWithLocalDataExec requires row-based UnionExec children")
          }
        }

        override def convertToCpu(): SparkPlan = {
          if (hasUnionWithLocalDataExecParent) {
            // Keep the original CPU UnionExec subtree; transition insertion is not guaranteed on
            // this Databricks-local row execution path.
            union
          } else {
            super.convertToCpu()
          }
        }

        override def convertToGpu(): GpuExec =
          GpuUnionExec(childPlans.map(_.convertIfNeeded()))
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ shimExecs
}
