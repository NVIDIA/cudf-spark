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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NamedExpression, Predicate}
import org.apache.spark.sql.rapids.GpuEqualTo
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuIn(value: Expression, literals: Seq[Any], dynamicList: Seq[Expression])
    extends GpuExpression with Predicate with ShimExpression {
  require(dynamicList.nonEmpty, "dynamic list should not be empty")

  override def children: Seq[Expression] = value +: dynamicList

  override def nullable: Boolean = children.exists(_.nullable) || literals.contains(null)

  override def dataType: DataType = BooleanType

  private val resultCount = dynamicList.length + (if (literals.nonEmpty) 1 else 0)

  private val shouldFuseEqualities =
    TypeSig.comparisonAstTypes.isSupportedByPlugin(value.dataType) &&
      (literals.nonEmpty || dynamicList.length > 1)

  @transient private lazy val orAst = {
    val references = (0 until resultCount).map { index =>
      GpuBoundReference(index, BooleanType, nullable = true)(
        NamedExpression.newExprId, s"in_$index")
    }
    val orExpression = references.tail.foldLeft(references.head: GpuExpression) {
      case (left, right) => org.apache.spark.sql.rapids.GpuOr(left, right)
    }
    GpuProjectAstExpression(orExpression)
  }

  @transient private lazy val fusedAst = {
    val valueIndex = if (literals.nonEmpty) 1 else 0
    val valueReference = GpuBoundReference(valueIndex, value.dataType, value.nullable)(
      NamedExpression.newExprId, "in_value")
    val dynamicComparisons = dynamicList.indices.map { index =>
      val candidate = dynamicList(index)
      val candidateReference = GpuBoundReference(
        valueIndex + index + 1, candidate.dataType, candidate.nullable)(
        NamedExpression.newExprId, s"in_candidate_$index")
      GpuEqualTo(valueReference, candidateReference)
    }
    val comparisons = if (literals.nonEmpty) {
      val literalReference = GpuBoundReference(0, BooleanType, nullable)(
        NamedExpression.newExprId, "in_literals")
      literalReference +: dynamicComparisons
    } else {
      dynamicComparisons
    }
    val expression = comparisons.tail.foldLeft(comparisons.head: GpuExpression) {
      case (left, right) => org.apache.spark.sql.rapids.GpuOr(left, right)
    }
    GpuProjectAstExpression(expression)
  }

  private def reduceComparisons(
      comparisons: Seq[GpuColumnVector],
      numRows: Int): GpuColumnVector = {
    if (comparisons.length == 1) {
      comparisons.head.incRefCount()
    } else {
      val comparisonBatch = new ColumnarBatch(comparisons.toArray, numRows)
      orAst.columnarEval(comparisonBatch)
    }
  }

  private def evaluateFusedAst(
      projected: ColumnarBatch,
      literalResult: Option[GpuColumnVector]): GpuColumnVector = {
    val inputs = literalResult.toSeq ++
      (0 until projected.numCols()).map(projected.column)
    val astBatch = new ColumnarBatch(inputs.toArray, projected.numRows())
    fusedAst.columnarEval(astBatch)
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(GpuProjectExec.project(batch, children)) { projected =>
      val valueColumn = projected.column(0).asInstanceOf[GpuColumnVector]
      val literalComparison = if (literals.nonEmpty) {
        Some(closeOnExcept(GpuInSet(value, literals).doColumnar(valueColumn)) { result =>
          GpuColumnVector.from(result, BooleanType)
        })
      } else {
        None
      }
      withResource(literalComparison) { literalResult =>
        if (shouldFuseEqualities) {
          evaluateFusedAst(projected, literalResult)
        } else {
          withResource(dynamicList.indices.safeMap { index =>
            val left = projected.column(0).asInstanceOf[GpuColumnVector]
            val right = projected.column(index + 1).asInstanceOf[GpuColumnVector]
            closeOnExcept(GpuEqualTo(value, dynamicList(index)).doColumnar(left, right)) { result =>
              GpuColumnVector.from(result, BooleanType)
            }
          }) { dynamicComparisons =>
            reduceComparisons(literalResult.toSeq ++ dynamicComparisons, batch.numRows())
          }
        }
      }
    }
  }

  private def listString: String = {
    val literalExpressions = literals.map(Literal(_, value.dataType))
    (literalExpressions ++ dynamicList).mkString(", ")
  }

  override def toString: String = s"$value IN ($listString)"

  override def sql: String = {
    val literalSql = literals.map(Literal(_, value.dataType).sql)
    val dynamicSql = dynamicList.map(_.sql)
    s"(${value.sql} IN (${(literalSql ++ dynamicSql).mkString(", ")}))"
  }
}
