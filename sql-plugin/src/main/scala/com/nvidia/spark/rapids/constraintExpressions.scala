/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.sql.catalyst.expressions.{Expression, TaggingExpression}
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.vectorized.ColumnarBatch


trait ShimTaggingExpression extends TaggingExpression with ShimUnaryExpression

/**
 * This is a TaggingExpression in spark, which gets matched in NormalizeFloatingNumbers (which is
 * a Rule).
 */
case class GpuKnownFloatingPointNormalized(child: Expression) extends ShimTaggingExpression
    with GpuExpression {
  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    child.columnarEvalAny(batch)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    child.columnarEval(batch)
  }
}

/**
 * GPU version of the 'KnownNotNull', a TaggingExpression in spark,
 * to tag an expression as known to not be null.
 */
case class GpuKnownNotNull(child: Expression) extends ShimTaggingExpression
    with GpuExpression {
  override def nullable: Boolean = false

  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    child.columnarEvalAny(batch)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    child.columnarEval(batch)
  }
}

/**
 * GPU version of KnownNotContainsNull, a TaggingExpression that marks an array expression
 * as known to not contain null elements (e.g. after array_compact). Used by Spark 4.0+
 * when rewriting ArrayCompact to ArrayFilter; the tag sets the result schema containsNull
 * to false.
 */
case class GpuKnownNotContainsNull(child: Expression) extends ShimTaggingExpression
    with GpuExpression {
  override def dataType: ArrayType =
    child.dataType.asInstanceOf[ArrayType].copy(containsNull = false)

  private def retag(cv: GpuColumnVector): GpuColumnVector = {
    withResource(cv) { input =>
      GpuColumnVector.from(input.getBase.incRefCount(), dataType)
    }
  }

  private def retag(scalar: GpuScalar): GpuScalar = {
    withResource(scalar) { input =>
      GpuScalar.wrap(input.getBase.incRefCount(), dataType)
    }
  }

  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    child.columnarEvalAny(batch) match {
      case cv: GpuColumnVector => retag(cv)
      case scalar: GpuScalar => retag(scalar)
      case other => other
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    retag(child.columnarEval(batch))
  }
}
