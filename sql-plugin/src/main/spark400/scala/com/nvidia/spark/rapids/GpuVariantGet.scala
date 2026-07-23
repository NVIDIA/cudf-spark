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
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.util.Optional

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, VariantUtils}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, LongType, ShortType,
  StringType}
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
        "supported types are tinyint, smallint, int, bigint, and string")
    }

    GpuVariantGet.parseSupportedPath(expr.path) match {
      case Some(_) =>
      case None =>
        willNotWorkOnGpu("path must be a literal object-field path like $.field or $.nested.field")
    }

    if (expr.failOnError) {
      willNotWorkOnGpu("strict variant_get is not supported; use try_variant_get")
    }

    if (!GpuVariantGet.isVariantCudfAvailable) {
      willNotWorkOnGpu("cuDF Java was built without Variant extraction APIs")
    }
  }

  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    val path = GpuVariantGet.parseSupportedPath(expr.path).get
    GpuVariantGet(lhs, path, expr.targetType)
  }
}

case class GpuVariantGet(
    child: Expression,
    path: String,
    override val dataType: DataType)
  extends GpuUnaryExpression
  with Retryable {

  override def nullable: Boolean = true

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    val variantStruct = input.getBase
    require(variantStruct.getType == DType.STRUCT,
      s"expected Variant struct input, got ${variantStruct.getType}")
    require(variantStruct.getNumChildren == 2,
      s"expected Variant struct with value and metadata children, got " +
        s"${variantStruct.getNumChildren} children")

    withResource(variantStruct.getChildColumnView(0)) { value =>
      withResource(variantStruct.getChildColumnView(1)) { metadata =>
        GpuVariantGet.withCudfVariantView(variantStruct, metadata, value) { cudfVariant =>
          GpuVariantGet.extractVariantField(cudfVariant, path, dataType)
        }
      }
    }
  }

  override def checkpoint(): Unit = ()

  override def restore(): Unit = ()
}

object GpuVariantGet {
  private val ObjectFieldPath = """^\$\.[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$""".r

  def isSupportedTargetType(dt: DataType): Boolean = dt match {
    case ByteType | ShortType | IntegerType | LongType | StringType => true
    case _ => false
  }

  def isVariantCudfAvailable: Boolean = {
    try {
      val variantUtils = Class.forName("ai.rapids.cudf.VariantUtils", true,
        Thread.currentThread().getContextClassLoader)
      variantUtils.getMethod("getVariantFieldValue", classOf[ColumnView], classOf[String])
      variantUtils.getMethod("castVariantValue", classOf[ColumnView], classOf[DType])
      variantUtils.getMethod("extractVariantField", classOf[ColumnView], classOf[String],
        classOf[DType])
      true
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException | _: LinkageError => false
    }
  }

  def toCudfTargetType(dt: DataType): DType = dt match {
    case ByteType => DType.INT8
    case ShortType => DType.INT16
    case IntegerType => DType.INT32
    case LongType => DType.INT64
    case StringType => DType.STRING
    case other => throw new IllegalArgumentException(s"unsupported variant target type: $other")
  }

  def extractVariantField(cudfVariant: ColumnView, path: String, dt: DataType): ColumnVector = {
    dt match {
      case ByteType | StringType =>
        VariantUtils.extractVariantField(cudfVariant, path, toCudfTargetType(dt))
      case ShortType =>
        extractAndWidenInteger(cudfVariant, path, DType.INT16, Seq(DType.INT8))
      case IntegerType =>
        extractAndWidenInteger(cudfVariant, path, DType.INT32, Seq(DType.INT16, DType.INT8))
      case LongType =>
        extractAndWidenInteger(cudfVariant, path, DType.INT64,
          Seq(DType.INT32, DType.INT16, DType.INT8))
      case other =>
        throw new IllegalArgumentException(s"unsupported variant target type: $other")
    }
  }

  private def extractAndWidenInteger(
      cudfVariant: ColumnView,
      path: String,
      targetType: DType,
      fallbackTypes: Seq[DType]): ColumnVector = {
    var result: ColumnVector = VariantUtils.extractVariantField(cudfVariant, path, targetType)
    try {
      fallbackTypes.foreach { fallbackType =>
        withResource(VariantUtils.extractVariantField(
            cudfVariant, path, fallbackType)) { fallback =>
          withResource(fallback.castTo(targetType)) { widened =>
            val next = result.replaceNulls(widened)
            result.safeClose()
            result = next
          }
        }
      }
      val ret = result
      result = null
      ret
    } finally {
      if (result != null) {
        result.safeClose()
      }
    }
  }

  private def withCudfVariantView[T](
      variantStruct: ColumnView,
      metadata: ColumnView,
      value: ColumnView)(f: ColumnView => T): T = {
    if (metadata.getType == DType.LIST && value.getType == DType.LIST) {
      withResource(makeCudfVariantView(variantStruct, metadata, value))(f)
    } else {
      withResource(toByteList(metadata)) { metadataBytes =>
        withResource(toByteList(value)) { valueBytes =>
          withResource(makeCudfVariantView(variantStruct, metadataBytes, valueBytes))(f)
        }
      }
    }
  }

  private def makeCudfVariantView(
      variantStruct: ColumnView,
      metadata: ColumnView,
      value: ColumnView): ColumnView = {
    new ColumnView(DType.STRUCT, variantStruct.getRowCount,
      Optional.of[java.lang.Long](variantStruct.getNullCount), variantStruct.getValid,
      null.asInstanceOf[ai.rapids.cudf.BaseDeviceMemoryBuffer],
      Array[ColumnView](metadata, value))
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

  def parseSupportedPath(pathExpr: Expression): Option[String] = pathExpr match {
    case Literal(path: UTF8String, _) => parseSupportedPath(path.toString)
    case Literal(path: String, _) => parseSupportedPath(path)
    case _ => None
  }

  def parseSupportedPath(path: String): Option[String] = {
    if (ObjectFieldPath.pattern.matcher(path).matches) {
      Some(path)
    } else {
      None
    }
  }
}
