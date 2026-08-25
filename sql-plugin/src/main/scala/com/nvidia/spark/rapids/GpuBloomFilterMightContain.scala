/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RapidsPluginImplicits.ReallyAGpuExpression
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.ShimBinaryExpression

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, GetStructField, PlanExpression}
import org.apache.spark.sql.catalyst.trees.TreePattern.OUTER_REFERENCE
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuBloomFilterMightContain(
    bloomFilterExpression: Expression,
    valueExpression: Expression)
  extends ShimBinaryExpression with GpuExpression with AutoCloseable {

  @transient private lazy val bloomFilter: GpuBloomFilter = {
    val ret = withResourceIfAllowed(
      bloomFilterExpression.columnarEvalAny(new ColumnarBatch(Array.empty))) {
      case s: GpuScalar => GpuBloomFilter(s)
      case x => throw new IllegalStateException(s"Expected GPU scalar, found $x")
    }
    // Don't install the callback if in a unit test
    Option(TaskContext.get()).foreach { tc =>
      onTaskCompletion(tc) {
        close()
      }
    }
    ret
  }

  override def nullable: Boolean = true

  override def left: Expression = bloomFilterExpression

  override def right: Expression = valueExpression

  override def prettyName: String = "might_contain"

  override def dataType: DataType = BooleanType

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (BinaryType, NullType) | (NullType, LongType) | (NullType, NullType) |
           (BinaryType, LongType) =>
        bloomFilterExpression match {
          case e: Expression if e.foldable => TypeCheckResult.TypeCheckSuccess
          case subquery: PlanExpression[_] if !subquery.containsPattern(OUTER_REFERENCE) =>
            TypeCheckResult.TypeCheckSuccess
          case GetStructField(subquery: PlanExpression[_], _, _)
            if !subquery.containsPattern(OUTER_REFERENCE) =>
            TypeCheckResult.TypeCheckSuccess
          case _ =>
            TypeCheckResult.TypeCheckFailure(s"The Bloom filter binary input to $prettyName " +
              "should be either a constant value or a scalar subquery expression")
        }
      case _ => TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
        s"been ${BinaryType.simpleString} followed by a value with ${LongType.simpleString}, " +
        s"but it's [${left.dataType.catalogString}, ${right.dataType.catalogString}].")
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (bloomFilter == null) {
      GpuColumnVector.fromNull(batch.numRows(), dataType)
    } else {
      withResource(valueExpression.columnarEval(batch)) { value =>
        if (value == null || value.dataType == NullType) {
          GpuColumnVector.fromNull(batch.numRows(), dataType)
        } else {
          GpuColumnVector.from(bloomFilter.mightContainLong(value.getBase), BooleanType)
        }
      }
    }
  }

  // This is disabled until https://github.com/NVIDIA/spark-rapids/issues/8945 can be fixed
  override def disableTieredProjectCombine: Boolean = true

  override def close(): Unit = {
    if (bloomFilter != null) {
      bloomFilter.close()
    }
  }
}
