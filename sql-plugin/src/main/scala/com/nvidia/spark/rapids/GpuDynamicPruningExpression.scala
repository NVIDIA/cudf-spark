/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.sql.catalyst.expressions.{DynamicPruning, Expression}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuDynamicPruningExpression(child: Expression)
  extends ShimUnaryExpression with GpuExpression with DynamicPruning {

  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    child.columnarEvalAny(batch)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    child.columnarEval(batch)
  }
}
