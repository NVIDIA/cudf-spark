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

package org.apache.spark.sql.rapids.aggregate

import scala.collection.immutable.Seq

import ai.rapids.cudf
import ai.rapids.cudf.{DType, GroupByAggregation, ReductionAggregation}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.ReallyAGpuExpression
import com.nvidia.spark.rapids.jni.HyperLogLogPlusPlusHostUDF
import com.nvidia.spark.rapids.jni.HyperLogLogPlusPlusHostUDF.AggregationType
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.rapids.{GpuCreateNamedStruct, GpuGetStructField}
import org.apache.spark.sql.rapids.shims.DataTypeUtilsShim
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CudfHLLPP(override val dataType: DataType,
    precision: Int) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (input: cudf.ColumnVector) => {
      if (input.getNullCount == input.getRowCount) {
        // For NullType column or all values are null,
        // return a struct scalar: struct(0L, 0L, ..., 0L)
        val numCols = GpuHyperLogLogPlusPlus.numWords(precision)
        withResource(cudf.ColumnVector.fromLongs(0L)) { cv =>
          // Underlying uses deep-copy, so we can reuse this `cv` and fill multiple times.
          val cvs: Array[cudf.ColumnView] = Array.fill(numCols)(cv)
          cudf.Scalar.structFromColumnViews(cvs: _*)
        }
      } else {
        val hll = new HyperLogLogPlusPlusHostUDF(AggregationType.Reduction, precision)
        input.reduce(ReductionAggregation.hostUDF(hll), DType.STRUCT)
      }
    }
  override lazy val groupByAggregate: GroupByAggregation = {
    val hll = new HyperLogLogPlusPlusHostUDF(AggregationType.GroupBy, precision)
    GroupByAggregation.hostUDF(hll)
  }
  override val name: String = "CudfHyperLogLogPlusPlus"
}

case class CudfMergeHLLPP(override val dataType: DataType,
    precision: Int)
  extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (input: cudf.ColumnVector) => {
      val hll = new HyperLogLogPlusPlusHostUDF(AggregationType.ReductionMerge, precision)
      input.reduce(ReductionAggregation.hostUDF(hll), DType.STRUCT)
    }

  override lazy val groupByAggregate: GroupByAggregation = {
    val hll = new HyperLogLogPlusPlusHostUDF(AggregationType.GroupByMerge, precision)
    GroupByAggregation.hostUDF(hll)
  }

  override val name: String = "CudfMergeHyperLogLogPlusPlus"
}

/**
 * Perform the final evaluation step to compute approximate count distinct from sketches.
 * Input is long columns, first construct struct of long then feed to cuDF
 */
case class GpuHyperLogLogPlusPlusEvaluation(childExpr: Expression,
    precision: Int)
  extends GpuExpression with ShimExpression {
  override def dataType: DataType = LongType

  override def nullable: Boolean = false

  override def prettyName: String = "HyperLogLogPlusPlus_evaluation"

  override def children: Seq[Expression] = Seq(childExpr)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(childExpr.columnarEval(batch)) { sketches =>
      val distinctValues = HyperLogLogPlusPlusHostUDF.estimateDistinctValueFromSketches(
        sketches.getBase, precision)
      GpuColumnVector.from(distinctValues, LongType)
    }
  }
}

/**
 * Gpu version of HyperLogLogPlusPlus
 * Spark APPROX_COUNT_DISTINCT on NULLs returns zero
 */
case class GpuHyperLogLogPlusPlus(childExpr: Expression, relativeSD: Double)
  extends GpuAggregateFunction with Serializable {

  // Consistent with Spark
  private lazy val precision: Int = GpuHyperLogLogPlusPlus.computePrecision(relativeSD)

  // Each long contains 10 register(max 6 bits)
  private lazy val numWords = GpuHyperLogLogPlusPlus.numWords(precision)

  // Spark agg buffer type: long array
  private lazy val sparkAggBufferAttributes: Seq[AttributeReference] = {
    Seq.tabulate(numWords) { i =>
      AttributeReference(s"MS[$i]", LongType)()
    }
  }

  /**
   * Spark uses long columns to save agg buffer, e.g.: long[52]
   * Each long compacts multiple registers to save memory
   */
  override val aggBufferAttributes: Seq[AttributeReference] = sparkAggBufferAttributes

  /**
   * init long array with all zero
   */
  override lazy val initialValues: Seq[Expression] = Seq.tabulate(numWords) { _ =>
    GpuLiteral(0L, LongType)
  }

  override lazy val inputProjection: Seq[Expression] = Seq(childExpr)

  /**
   * cuDF HLLPP sketch type: struct<long, ..., long>
   */
  private lazy val cuDFBufferType: DataType = DataTypeUtilsShim.fromAttributes(aggBufferAttributes)

  /**
   * cuDF uses Struct<long, ..., long> column to do aggregate
   */
  override lazy val updateAggregates: Seq[CudfAggregate] =
    Seq(CudfHLLPP(cuDFBufferType, precision))

  /**
   * Convert long columns to Struct<long, ..., long> column
   */
  private def genStruct: Seq[Expression] = {
    val names = Seq.tabulate(numWords) { i => GpuLiteral(s"MS[$i]", StringType) }
    Seq(GpuCreateNamedStruct(names.zip(aggBufferAttributes).flatten { case (a, b) => List(a, b) }))
  }

  /**
   * Convert Struct<long, ..., long> column to long columns
   */
  override lazy val postUpdate: Seq[Expression] = Seq.tabulate(numWords) {
    i => GpuGetStructField(postUpdateAttr.head, i)
  }

  /**
   * convert to Struct<long, ..., long>
   */
  override lazy val preMerge: Seq[Expression] = genStruct

  override lazy val mergeAggregates: Seq[CudfAggregate] =
    Seq(CudfMergeHLLPP(cuDFBufferType, precision))

  /**
   * Convert Struct<long, ..., long> column to long columns
   */
  override lazy val postMerge: Seq[Expression] = Seq.tabulate(numWords) {
    i => GpuGetStructField(postMergeAttr.head, i)
  }

  override lazy val evaluateExpression: Expression =
    GpuHyperLogLogPlusPlusEvaluation(genStruct.head, precision)

  override def dataType: DataType = LongType

  // Spark APPROX_COUNT_DISTINCT on NULLs returns zero
  override def nullable: Boolean = false

  override def prettyName: String = "approx_count_distinct"

  override def children: Seq[Expression] = Seq(childExpr)
}

object GpuHyperLogLogPlusPlus {

  def numWords(precision: Int): Int = (1 << precision) / 10 + 1

  def computePrecision(relativeSD: Double): Int =
    Math.ceil(2.0d * Math.log(1.106d / relativeSD) / Math.log(2.0d)).toInt;
}
