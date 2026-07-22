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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.util.Optional

import ai.rapids.cudf.{ColumnVector, ColumnView, DType}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.VariantUtils

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class GpuVariantGetMeta(
    expr: VariantGet,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends BinaryExprMeta[VariantGet](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    if (!GpuColumnVector.isVariantType(expr.child.dataType)) {
      willNotWorkOnGpu(s"input type ${expr.child.dataType.simpleString} is not VariantType")
    }

    if (!GpuVariantGet.isSupportedTargetType(expr.targetType)) {
      willNotWorkOnGpu(s"target type ${expr.targetType.simpleString} is not supported; " +
        "only string targets are enabled until Spark-compatible Variant casts are implemented")
    }

    GpuVariantGet.parseSimplePath(expr.path) match {
      case Some(_) =>
      case None =>
        willNotWorkOnGpu("path must be a literal single-segment object path like $.field")
    }

    if (expr.failOnError) {
      willNotWorkOnGpu("strict variant_get is not supported; use try_variant_get")
    }

    if (!GpuVariantGet.isVariantJniAvailable) {
      willNotWorkOnGpu("spark-rapids-jni was built without cuDF Variant extraction APIs")
    }
  }

  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    val fieldName = GpuVariantGet.parseSimplePath(expr.path).get
    GpuVariantGet(lhs, fieldName, expr.targetType)
  }
}

case class GpuVariantGet(
    child: Expression,
    fieldName: String,
    override val dataType: DataType)
  extends GpuUnaryExpression
  with Retryable {

  override def nullable: Boolean = true

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    val variantStruct = input.getBase
    require(variantStruct.getType == DType.STRUCT,
      s"expected Variant struct input, got ${variantStruct.getType}")
    require(variantStruct.getNumChildren >= 2,
      s"expected Variant struct with value and metadata children, got " +
        s"${variantStruct.getNumChildren} children")

    withResource(variantStruct.getChildColumnView(0)) { value =>
      withResource(variantStruct.getChildColumnView(1)) { metadata =>
        GpuVariantGet.withCudfVariantView(metadata, value) { cudfVariant =>
          VariantUtils.extractVariantField(
            cudfVariant,
            fieldName,
            GpuVariantGet.toCudfTargetType(dataType))
        }
      }
    }
  }

  override def checkpoint(): Unit = ()

  override def restore(): Unit = ()
}

object GpuVariantGet {
  private val SimplePath = """^(?:\$\.)?([A-Za-z_][A-Za-z0-9_]*)$""".r

  def isSupportedTargetType(dt: DataType): Boolean = dt match {
    case StringType => true
    case _ => false
  }

  def isVariantJniAvailable: Boolean = {
    try {
      VariantUtils.isAvailable()
    } catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError |
           _: ExceptionInInitializerError | _: RuntimeException => false
    }
  }

  def toCudfTargetType(dt: DataType): DType = dt match {
    case StringType => DType.STRING
    case other => throw new IllegalArgumentException(s"unsupported variant target type: $other")
  }

  private def withCudfVariantView[T](
      metadata: ColumnView,
      value: ColumnView)(f: ColumnView => T): T = {
    if (metadata.getType == DType.LIST && value.getType == DType.LIST) {
      withResource(ColumnView.makeStructView(metadata, value))(f)
    } else {
      withResource(toByteList(metadata)) { metadataBytes =>
        withResource(toByteList(value)) { valueBytes =>
          withResource(ColumnView.makeStructView(metadataBytes, valueBytes))(f)
        }
      }
    }
  }

  private def toByteList(cv: ColumnView): ColumnVector = {
    require(cv.getType == DType.STRING,
      s"expected Variant physical binary child to be STRING or LIST, got ${cv.getType}")

    val dataBuf = Option(cv.getData)
    withResource(new ColumnView(DType.UINT8, dataBuf.map(_.getLength).getOrElse(0L),
      Optional.of(0L), dataBuf.orNull, null)) { data =>
      withResource(new ColumnView(DType.LIST, cv.getRowCount,
        Optional.of[java.lang.Long](cv.getNullCount),
        cv.getValid, cv.getOffsets, Array(data))) { byteList =>
        byteList.copyToColumnVector()
      }
    }
  }

  def parseSimplePath(pathExpr: Expression): Option[String] = pathExpr match {
    case Literal(path: UTF8String, _) => parseSimplePath(path.toString)
    case Literal(path: String, _) => parseSimplePath(path)
    case _ => None
  }

  def parseSimplePath(path: String): Option[String] = path match {
    case SimplePath(fieldName) => Some(fieldName)
    case _ => None
  }
}
