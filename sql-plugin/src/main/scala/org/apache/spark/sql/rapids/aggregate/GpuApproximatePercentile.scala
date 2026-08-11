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

package org.apache.spark.sql.rapids.aggregate

import ai.rapids.cudf
import ai.rapids.cudf.{DType, GroupByAggregation, ReductionAggregation}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuLiteral}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuCast.doCast
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The ApproximatePercentile function returns the approximate percentile(s) of a column at the given
 * percentage(s). A percentile is a watermark value below which a given percentage of the column
 * values fall. For example, the percentile of column `col` at percentage 50% is the median of
 * column `col`.
 *
 * This function supports partial aggregation.
 *
 * The GPU implementation uses t-digest to perform the initial aggregation (see
 * `updateExpressions` / `mergeExpressions`) and then applies the `ApproxPercentileFromTDigestExpr`
 * expression to compute percentiles from the final t-digest (see `evaluateExpression`).
 *
 * There are two different data types involved here. The t-digests are a map of centroids
 * (`Map[mean: Double -> weight: Double]`) represented as `List[Struct[Double, Double]]` and
 * the final output is either a single double or an array of doubles, depending on whether
 * the `percentageExpression` parameter is a single value or an array.
 *
 * @param child child expression that can produce column value with `child.eval()`
 * @param percentageExpression Expression that represents a single percentage value or
 *                             an array of percentage values. Each percentage value must be between
 *                             0.0 and 1.0.
 * @param accuracyExpression Integer literal expression of approximation accuracy. Higher value
 *                           yields better accuracy, the default value is
 *                           DEFAULT_PERCENTILE_ACCURACY.
 */
case class GpuApproximatePercentile (
    child: Expression,
    percentageExpression: GpuLiteral,
    accuracyExpression: GpuLiteral = GpuLiteral(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY))
  extends GpuAggregateFunction {

  override val inputProjection: Seq[Expression] = Seq(child)

  // Attributes of fields in the aggregation buffer.
  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(GpuLiteral(
    InternalRow(
      ArrayData.toArrayData(Array.empty), // centroids (mean, weight)
      0d, // min
      0d), // max
    CudfTDigest.dataType))

  // the update expression will create a t-digest (List[Struct[Double, Double])
  override lazy val updateAggregates: Seq[CudfAggregate] =
    new CudfTDigestUpdate(accuracyExpression) :: Nil

  // the merge expression will merge t-digests
  override lazy val mergeAggregates: Seq[CudfAggregate] =
    new CudfTDigestMerge(accuracyExpression) :: Nil

  // the evaluate expression will compute percentiles based on a t-digest
  override lazy val evaluateExpression: Expression = {
    ApproxPercentileFromTDigestExpr(outputBuf, percentiles, child.dataType)
  }

  // inputBuf represents the initial t-digest aggregation buffer
  protected final lazy val inputBuf: AttributeReference =
    AttributeReference("inputBuf", CudfTDigest.dataType)()

  // outputBuf represents the merged t-digest aggregation buffer
  protected final lazy val outputBuf: AttributeReference =
    inputBuf.copy("outputBuf", CudfTDigest.dataType)(inputBuf.exprId, inputBuf.qualifier)

  override def nullable: Boolean = false

  // Mark as lazy so that percentageExpression is not evaluated during tree transformation.
  private lazy val (returnPercentileArray, percentiles) = makeArray(percentageExpression.value)

  private def makeArray(v: Any): (Boolean, Either[Double, Array[Double]])  = v match {
    // Rule ImplicitTypeCasts can cast other numeric types to double
    case null => (false, Right(Array()))
    case num: Double => (false, Left(num))
    case arrayData: ArrayData => (true, Right(arrayData.toDoubleArray()))
    case other => throw new IllegalStateException(s"Invalid percentile expression $other")
  }

  // The result type is the same as the input type.
  private lazy val internalDataType: DataType = {
    if (returnPercentileArray) {
      ArrayType(child.dataType, containsNull = false)
    } else {
      child.dataType
    }
  }

  override def dataType: DataType = internalDataType

  override def children: Seq[Expression] = Seq(child, percentageExpression, accuracyExpression)
}

/**
 * This expression computes an approximate percentile using a t-digest as input.
 *
 * @param child Expression that produces the t-digests.
 * @param percentiles Percentile scalar, or percentiles array to evaluate.
 * @param finalDataType Data type for results
 */
case class ApproxPercentileFromTDigestExpr(
    child: Expression,
    percentiles: Either[Double, Array[Double]],
    finalDataType: DataType)
  extends GpuExpression with ShimExpression {

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val expr = child.asInstanceOf[GpuExpression]
    withResource(expr.columnarEval(batch)) { cv =>
      percentiles match {
        case Left(p) =>
          // For the scalar case, we still pass cuDF an array of percentiles
          // (containing a single item) and then extract the child column from the resulting
          // array and return that (after converting from Double to finalDataType)
          withResource(cv.getBase.approxPercentile(Array(p))) { percentiles =>
            withResource(percentiles.extractListElement(0)) { childView =>
              withResource(doCast(childView, DataTypes.DoubleType, finalDataType)) { childCv =>
                GpuColumnVector.from(childCv.copyToColumnVector(), dataType)
              }
            }
          }

        case Right(p) =>
          // array case - cast cuDF Array[Double] to Array[finalDataType]
          withResource(cv.getBase.approxPercentile(p)) { percentiles =>
            if (finalDataType == DataTypes.DoubleType) {
              GpuColumnVector.from(percentiles.incRefCount(), dataType)
            } else {
              withResource(percentiles.getChildColumnView(0)) { childView =>
                withResource(doCast(childView, DataTypes.DoubleType, finalDataType)) { childCv =>
                  withResource(percentiles.replaceListChild(childCv)) { x =>
                    GpuColumnVector.from(x.copyToColumnVector(), dataType)
                  }
                }
              }
            }
          }

      }
    }
  }
  override def nullable: Boolean = false
  override def dataType: DataType = percentiles match {
    case Left(_) => finalDataType
    case Right(_) => ArrayType(finalDataType, containsNull = false)
  }
  override def children: Seq[Expression] = Seq(child)
}

class CudfTDigestUpdate(accuracyExpression: GpuLiteral)
  extends CudfAggregate {

  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) =>
      col.reduce(ReductionAggregation.createTDigest(CudfTDigest.accuracy(accuracyExpression)),
        DType.STRUCT)

  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.createTDigest(CudfTDigest.accuracy(accuracyExpression))
  override val name: String = "CudfTDigestUpdate"
  override def dataType: DataType = CudfTDigest.dataType
}

class CudfTDigestMerge(accuracyExpression: GpuLiteral)
  extends CudfAggregate {

  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) =>
      col.reduce(ReductionAggregation.mergeTDigest(CudfTDigest.accuracy(accuracyExpression)))

  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.mergeTDigest(CudfTDigest.accuracy(accuracyExpression))
  override val name: String = "CudfTDigestMerge"
  override def dataType: DataType = CudfTDigest.dataType
}

object CudfTDigest {
  val dataType: DataType = StructType(Array(
    StructField("centroids", ArrayType(StructType(Array(
      StructField("mean", DataTypes.DoubleType, nullable = false),
      StructField("weight", DataTypes.DoubleType, nullable = false)
    )), containsNull = false)),
    StructField("min", DataTypes.DoubleType, nullable = false),
    StructField("max", DataTypes.DoubleType, nullable = false)
  ))

  // Map Spark delta to cuDF delta
  def accuracy(accuracyExpression: GpuLiteral): Int = accuracyExpression.value match {
    case delta: Int => delta.max(1000)
    case _ => 1000
  }
}
