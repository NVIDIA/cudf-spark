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

package org.apache.iceberg.spark.functions

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, DType}
import com.nvidia.spark.rapids.{ExprMeta, GpuBinaryExpression, GpuColumnVector, GpuScalar}
import com.nvidia.spark.rapids.jni.iceberg.IcebergBucket

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types._

case class GpuBucketExpression(numBuckets: Expression, value: Expression)
  extends GpuBinaryExpression {

  private lazy val sanityCheckResult: Unit = sanityCheck()

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): CudfColumnVector = {
    throw new IllegalStateException("GpuBucketExpression requires first argument to be scalar, " +
      "second to be column, but both are columns")
  }

  override def doColumnar(numBuckets: GpuScalar, rhs: GpuColumnVector): CudfColumnVector = {
    sanityCheckResult
    val numBucketsInt = numBuckets.getValue.asInstanceOf[java.lang.Integer]
    require(numBucketsInt != null, "numBuckets must be valid")

    IcebergBucket.computeBucket(rhs.getBase, numBucketsInt.intValue())
  }

  private def sanityCheck(): Unit = {
    require(numBuckets.dataType == DataTypes.IntegerType,
      s"buckets number must be an integer, got ${numBuckets.dataType}")

    require(GpuBucketExpression.isSupportedValueType(value.dataType),
      s"Bucket function does not support type ${value.dataType} as values")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): CudfColumnVector = {
    throw new IllegalStateException("GpuBucketExpression requires first argument to be scalar, " +
      "second to be column, but first is column, second is scalar")
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): CudfColumnVector = {
    throw new IllegalStateException("GpuBucketExpression requires first argument to be scalar, " +
      "second to be column, but both are scalars")
  }

  override def left: Expression = numBuckets

  override def right: Expression = value

  override def dataType: DataType = DataTypes.IntegerType
}

object GpuBucketExpression {

  /**
   * Supported Iceberg Bucket function classes and their corresponding Iceberg types:
   *
   * - BucketInt: INTEGER, DATE
   * - BucketLong: LONG, TIMESTAMP
   * - BucketString: STRING
   * - BucketBinary: BINARY, FIXED, UUID
   * - BucketDecimal: DECIMAL
   *
   * See Iceberg's Bucket.java canTransform() for the full list of supported types.
   */
  private lazy val supportedFunctionClasses: Set[Class[_]] =
    Set(
      classOf[BucketFunction.BucketInt],
      classOf[BucketFunction.BucketLong],
      classOf[BucketFunction.BucketString],
      classOf[BucketFunction.BucketBinary],
      classOf[BucketFunction.BucketDecimal]
    )

  /**
   * Check if the Spark DataType is supported for bucket transform.
   *
   * Iceberg type to Spark type mapping:
   * - INTEGER -> IntegerType
   * - LONG -> LongType
   * - DATE -> DateType
   * - TIMESTAMP -> TimestampType
   * - STRING -> StringType
   * - BINARY -> BinaryType
   * - DECIMAL -> DecimalType (up to 128-bit precision)
   */
  def isSupportedValueType(dataType: DataType): Boolean = {
    dataType match {
      // Maps to BucketInt: INTEGER, DATE
      case IntegerType | DateType => true
      // Maps to BucketLong: LONG, TIMESTAMP
      case LongType | TimestampType => true
      // Maps to BucketString: STRING
      case StringType => true
      // Maps to BucketBinary: BINARY
      case BinaryType => true
      // Maps to BucketDecimal: DECIMAL
      case dt: DecimalType => dt.precision <= DType.DECIMAL128_MAX_PRECISION
      case _ => false
    }
  }

  def tagExprForGpu(meta: ExprMeta[StaticInvoke]): Unit = {
    require(meta.childExprs.length == 2,
      s"BucketFunction should have exactly two arguments, got ${meta.childExprs.length}")
    val exprCls = meta.wrapped.staticObject

    if (!supportedFunctionClasses.contains(exprCls)) {
      meta.willNotWorkOnGpu(s"Supported iceberg partition function classes are: " +
        s"${supportedFunctionClasses.mkString("[", ",", "]")},  actual: $exprCls")
    }

    val bucketExpr = meta.wrapped.arguments.head
    if (bucketExpr.dataType != DataTypes.IntegerType) {
      throw new IllegalStateException(
        s"BucketFunction number of buckets must be an integer, got ${bucketExpr.dataType}")
    }

    val valueExpr = meta.wrapped.arguments(1)
    if (!isSupportedValueType(valueExpr.dataType)) {
      meta.willNotWorkOnGpu(s"Gpu bucket function does not support type ${valueExpr.dataType} " +
        s"as values")
    }
  }
}
