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
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.util.Optional

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar, VariantUtils}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Literal,
  NamedExpression}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, LongType, ShortType,
  StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch
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

    if (!conf.isCpuBridgeEnabled) {
      willNotWorkOnGpu("Variant extraction requires the CPU bridge for runtime coercion fallback")
    }
  }

  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    val path = GpuVariantGet.parseSupportedPath(expr.path).get
    val cpuFallback = expr.copy(
      child = BoundReference(0, expr.child.dataType, expr.child.nullable))
    GpuVariantGet(lhs, path, expr.targetType, cpuFallback)
  }
}

case class GpuVariantGet(
    child: Expression,
    path: String,
    override val dataType: DataType,
    cpuFallback: VariantGet)
  extends GpuUnaryExpression
  with Retryable
  with GpuMetricsInjectable {

  override def nullable: Boolean = true
  override def hasSideEffects: Boolean = true

  private var bridgeMetrics: Map[String, GpuMetric] = Map.empty

  override def injectMetrics(metrics: Map[String, GpuMetric]): Unit = {
    bridgeMetrics = metrics
  }

  @transient private lazy val fallbackBridge = {
    val inputRef = GpuBoundReference(0, child.dataType, child.nullable)(
      NamedExpression.newExprId, "_variant")
    val bridge = GpuCpuBridgeExpression(Seq(inputRef), cpuFallback, dataType, nullable)
    bridge.injectMetrics(bridgeMetrics)
    bridge
  }

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
          GpuVariantGet.extractVariantField(cudfVariant, path, dataType, input, fallbackBridge)
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
      true
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException | _: LinkageError => false
    }
  }

  def extractVariantField(
      cudfVariant: ColumnView,
      path: String,
      dt: DataType,
      input: GpuColumnVector,
      fallbackBridge: GpuCpuBridgeExpression): ColumnVector = {
    withResource(VariantUtils.getVariantFieldValue(cudfVariant, path)) { rawValue =>
      dt match {
        case StringType =>
          withResource(VariantUtils.castVariantValue(rawValue, DType.STRING)) { decoded =>
            if (allRowsCovered(rawValue, Seq(decoded))) {
              decoded.incRefCount()
            } else {
              evaluateOnCpu(input, fallbackBridge)
            }
          }
        case ByteType | ShortType | IntegerType | LongType =>
          val decoded = Seq(DType.INT8, DType.INT16, DType.INT32, DType.INT64).safeMap {
            sourceType => VariantUtils.castVariantValue(rawValue, sourceType)
          }
          withResource(decoded) { decoded =>
            if (allRowsCovered(rawValue, decoded)) {
              withResource(normalizeIntegers(decoded)) { logicalLongs =>
                narrowInteger(logicalLongs, dt)
              }
            } else {
              evaluateOnCpu(input, fallbackBridge)
            }
          }
        case other =>
          throw new IllegalArgumentException(s"unsupported variant target type: $other")
      }
    }
  }

  private def allRowsCovered(rawValue: ColumnView, decoded: Seq[ColumnView]): Boolean = {
    withResource(new CloseableHolder(rawValue.isNull)) { covered =>
      decoded.foreach { value =>
        withResource(value.isNotNull) { decodedValue =>
          covered.setAndCloseOld(covered.get.or(decodedValue))
        }
      }
      BoolUtils.isAllValidTrue(covered.get)
    }
  }

  private def normalizeIntegers(decoded: Seq[ColumnVector]): ColumnVector = {
    require(decoded.length == 4, s"expected four integer decoders, got ${decoded.length}")

    withResource(new CloseableHolder(decoded(3).copyToColumnVector())) { result =>
      decoded.take(3).reverse.foreach { value =>
        withResource(value.castTo(DType.INT64)) { widened =>
          result.setAndCloseOld(result.get.replaceNulls(widened))
        }
      }
      result.get.incRefCount()
    }
  }

  private def narrowInteger(input: ColumnVector, dt: DataType): ColumnVector = dt match {
    case LongType => input.incRefCount()
    case ByteType =>
      nullifyOutOfRangeAndCast(input, Byte.MinValue.toLong, Byte.MaxValue.toLong, DType.INT8)
    case ShortType =>
      nullifyOutOfRangeAndCast(input, Short.MinValue.toLong, Short.MaxValue.toLong, DType.INT16)
    case IntegerType =>
      nullifyOutOfRangeAndCast(input, Int.MinValue.toLong, Int.MaxValue.toLong, DType.INT32)
    case other =>
      throw new IllegalArgumentException(s"unsupported integral Variant target type: $other")
  }

  private def nullifyOutOfRangeAndCast(
      input: ColumnView,
      minValue: Long,
      maxValue: Long,
      targetType: DType): ColumnVector = {
    withResource(Scalar.fromLong(minValue)) { min =>
      withResource(input.greaterOrEqualTo(min)) { aboveMin =>
        withResource(Scalar.fromLong(maxValue)) { max =>
          withResource(input.lessOrEqualTo(max)) { belowMax =>
            withResource(aboveMin.and(belowMax)) { inRange =>
              withResource(Scalar.fromNull(DType.INT64)) { nullValue =>
                withResource(inRange.ifElse(input, nullValue)) { masked =>
                  masked.castTo(targetType)
                }
              }
            }
          }
        }
      }
    }
  }

  private def evaluateOnCpu(
      input: GpuColumnVector,
      fallbackBridge: GpuCpuBridgeExpression): ColumnVector = {
    val inputColumns = Array[org.apache.spark.sql.vectorized.ColumnVector](input.incRefCount())
    withResource(new ColumnarBatch(inputColumns, input.getBase.getRowCount.toInt)) { batch =>
      withResource(fallbackBridge.columnarEval(batch)) { result =>
        result.getBase.incRefCount()
      }
    }
  }

  private def withCudfVariantView[T](
      variantStruct: ColumnView,
      metadata: ColumnView,
      value: ColumnView)(f: ColumnView => T): T = {
    withByteList(metadata) { metadataBytes =>
      withByteList(value) { valueBytes =>
        withResource(makeCudfVariantView(variantStruct, metadataBytes, valueBytes))(f)
      }
    }
  }

  private def withByteList[T](cv: ColumnView)(f: ColumnView => T): T = {
    cv.getType match {
      case DType.LIST => f(cv)
      case DType.STRING => withResource(toByteList(cv))(f)
      case other =>
        throw new IllegalArgumentException(
          s"expected Variant physical binary child to be STRING or LIST, got $other")
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
