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
{"spark":["400","420"],"carryForward":"all"}
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import scala.util.Try

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar, VariantOperationResult,
  VariantOperationStatus, VariantUtils}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Literal,
  NamedExpression}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType,
  IntegerType, LongType, ShortType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

case class GpuVariantGetMeta(
    expr: VariantGet,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends BinaryExprMeta[VariantGet](expr, conf, p, rule) {

  override def isTimeZoneSupported: Boolean = true

  override def tagExprForGpu(): Unit = {
    if (!GpuColumnVector.isVariantType(expr.child.dataType)) {
      willNotWorkOnGpu(s"input type ${expr.child.dataType.simpleString} is not VariantType")
    }

    if (!GpuVariantGet.isSupportedTargetType(expr.targetType)) {
      willNotWorkOnGpu(s"target type ${expr.targetType.simpleString} is not supported; " +
        "supported types are boolean, tinyint, smallint, int, bigint, float, double, and string")
    }

    GpuVariantGet.parseSupportedPath(expr.path) match {
      case Some(_) =>
      case None =>
        willNotWorkOnGpu("path must be a literal object-field/array-index path like " +
          "$.field, $.nested.field, or $.items[0].field")
    }

    if (expr.failOnError) {
      willNotWorkOnGpu("strict variant_get is not supported; use try_variant_get")
    }

    if (!GpuVariantGet.isVariantCudfAvailable) {
      willNotWorkOnGpu("cuDF Java was built without Variant status extraction APIs")
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
    GpuVariantExtraction.withRawExtraction(input.getBase, path) { extraction =>
      GpuVariantGet.decodeVariantField(extraction, dataType, input.getBase, path) {
        GpuVariantGet.evaluateOnCpu(input, fallbackBridge)
      }
    }
  }

  override def checkpoint(): Unit = ()

  override def restore(): Unit = ()
}

object GpuVariantGet {
  private val SupportedPath =
    """^\$(?:\.[A-Za-z_][A-Za-z0-9_]*|\[[0-9]+\])+$""".r
  private val ArrayIndex = """\[([0-9]+)\]""".r

  def isSupportedTargetType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
        DoubleType | StringType => true
    case _ => false
  }

  // The defining loader also resolves this expression's cuDF classes. Cache against that stable
  // loader rather than the tagging thread's potentially different context loader.
  lazy val isVariantCudfAvailable: Boolean = {
    GpuVariantExtraction.hasRequiredApis { name =>
      Class.forName(name, false, getClass.getClassLoader)
    }
  }

  /**
   * Decodes the extracted field on the GPU, or evaluates the whole batch with `fallback` when any
   * row needs Spark's CPU semantics.
   *
   * The batch stays on the GPU when every row is a SQL-null parent, a missing path, a Variant
   * null, or a value one of the exact-type decoders accepts. Spark returns NULL for a Variant null
   * whatever the target type, and no decoder accepts one. Extraction leaves the raw value null
   * only for ROW_NULL, MISSING_PATH and MALFORMED_VARIANT, and a decoder leaves a row valid only
   * when it decoded the row. Malformed bytes found during extraction, and rows that Spark's
   * Variant constructor rejects, fall back so that Spark raises its error.
   *
   * @param input the Variant input the extraction was computed from
   * @param path the extraction path, which decides whether cuDF validated the metadata
   */
  private[rapids] def decodeVariantField(
      extraction: VariantOperationResult,
      dt: DataType,
      input: ColumnView,
      path: String)(fallback: => ColumnVector): ColumnVector = {
    def covered(decoded: ColumnView) = isCovered(extraction, input.getNullCount, decoded,
      GpuVariantExtraction.failsVariantConstructor(input, skipsMetadataValidation(path)))
    dt match {
      case BooleanType => decodeExactType(extraction, DType.BOOL8, covered)(fallback)
      case FloatType => decodeExactType(extraction, DType.FLOAT32, covered)(fallback)
      case DoubleType => decodeExactType(extraction, DType.FLOAT64, covered)(fallback)
      case StringType => decodeExactType(extraction, DType.STRING, covered)(fallback)
      case ByteType | ShortType | IntegerType | LongType =>
        val decoded = Seq(DType.INT8, DType.INT16, DType.INT32, DType.INT64).safeMap {
          sourceType => VariantUtils.castVariantValue(extraction.getValue, sourceType)
        }
        // Coverage uses the combined column, so a row two decoders accept is counted once.
        withResource(withResource(decoded)(normalizeIntegers)) { logicalLongs =>
          if (covered(logicalLongs)) narrowInteger(logicalLongs, dt) else fallback
        }
      case other =>
        throw new IllegalArgumentException(s"unsupported variant target type: $other")
    }
  }

  private def decodeExactType(
      extraction: VariantOperationResult,
      targetType: DType,
      covered: ColumnView => Boolean)(fallback: => ColumnVector): ColumnVector = {
    withResource(VariantUtils.castVariantValue(extraction.getValue, targetType)) { decoded =>
      if (covered(decoded)) {
        decoded.incRefCount()
      } else {
        fallback
      }
    }
  }

  // cuDF validates the metadata when it resolves an object-key step. A path that starts with an
  // array index can return before reaching one.
  private def skipsMetadataValidation(path: String): Boolean = path.startsWith("$[")

  private def isCovered(
      extraction: VariantOperationResult,
      parentNullCount: Long,
      decoded: ColumnView,
      failsConstructor: => Boolean): Boolean = {
    val rows = extraction.getValue.getRowCount
    val rawNulls = extraction.getValue.getNullCount
    val undecoded = rows - rawNulls - (decoded.getRowCount - decoded.getNullCount)
    val status = extraction.getStatus
    // The status reductions run only when the null counts leave a question open. When every raw
    // null is a SQL-null parent, no row can be missing or malformed at extraction.
    (undecoded == 0 || undecoded == countStatus(status, VariantOperationStatus.VARIANT_NULL)) &&
      (rawNulls == parentNullCount ||
        !containsStatus(status, VariantOperationStatus.MALFORMED_VARIANT)) &&
      (parentNullCount == rows || !failsConstructor)
  }

  // The status column is non-nullable UINT8. If cuDF changes that, contains fails the query
  // rather than reading the wrong width.
  private def statusScalar(status: VariantOperationStatus): Scalar =
    Scalar.fromUnsignedByte(status.getNativeId.toByte)

  private def containsStatus(statuses: ColumnView, status: VariantOperationStatus): Boolean = {
    withResource(statusScalar(status))(needle => statuses.contains(needle))
  }

  private def countStatus(statuses: ColumnView, status: VariantOperationStatus): Long = {
    withResource(statusScalar(status)) { needle =>
      withResource(statuses.equalTo(needle)) { matches =>
        withResource(matches.sum(DType.INT64)) { count =>
          if (count.isValid) count.getLong else 0L
        }
      }
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
    val aboveMin = withResource(Scalar.fromLong(minValue)) { min =>
      input.greaterOrEqualTo(min)
    }
    val belowMax = closeOnExcept(aboveMin) { _ =>
      withResource(Scalar.fromLong(maxValue)) { max =>
        input.lessOrEqualTo(max)
      }
    }
    val inRange = withResource(Seq(aboveMin, belowMax)) { _ =>
      aboveMin.and(belowMax)
    }
    val masked = withResource(inRange) { inRange =>
      withResource(Scalar.fromNull(DType.INT64)) { nullValue =>
        inRange.ifElse(input, nullValue)
      }
    }
    withResource(masked)(_.castTo(targetType))
  }

  private[rapids] def evaluateOnCpu(
      input: GpuColumnVector,
      fallbackBridge: GpuCpuBridgeExpression): ColumnVector = {
    val inputColumns = Array[org.apache.spark.sql.vectorized.ColumnVector](input.incRefCount())
    withResource(new ColumnarBatch(inputColumns, input.getBase.getRowCount.toInt)) { batch =>
      withResource(fallbackBridge.columnarEval(batch)) { result =>
        result.getBase.incRefCount()
      }
    }
  }

  def parseSupportedPath(pathExpr: Expression): Option[String] = pathExpr match {
    case Literal(path: UTF8String, _) => parseSupportedPath(path.toString)
    case Literal(path: String, _) => parseSupportedPath(path)
    case _ => None
  }

  def parseSupportedPath(path: String): Option[String] = {
    val validIndexes =
      ArrayIndex.findAllMatchIn(path).forall(index => Try(index.group(1).toInt).isSuccess)
    if (SupportedPath.pattern.matcher(path).matches && validIndexes) {
      Some(path)
    } else {
      None
    }
  }
}
