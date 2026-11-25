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

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar, VariantOperationStatus,
  VariantUtils}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.SparkThrowable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Literal,
  NamedExpression}
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DecimalType, DoubleType,
  FloatType, IntegerType, LongType, ShortType, StringType, VariantType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

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
        "supported types are boolean, tinyint, smallint, int, bigint, float, double, string, " +
        "and decimal with nonnegative scale")
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
      willNotWorkOnGpu("cuDF Java lacks the Variant status extraction APIs " +
        "(VariantUtils.getVariantFieldValueWithStatus and castVariantValue)")
    } else if (expr.targetType.isInstanceOf[DecimalType] &&
        !GpuVariantGet.isVariantDecimalCudfAvailable) {
      willNotWorkOnGpu("decimal targets need VariantUtils.castVariantValueWithStatus and " +
        "VariantOperationStatus.ROW_NULL, which this cuDF Java lacks")
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
    if (!GpuVariantExtraction.supportsLayout(input.getBase)) {
      GpuVariantGet.evaluateOnCpu(input, fallbackBridge)
    } else {
      GpuVariantExtraction.extractOrElse(input.getBase, path) { extraction =>
        GpuVariantGet.decodeVariantField(extraction, dataType)
      } {
        // Extraction and decoding temporaries are released before CPU conversion starts.
        GpuVariantGet.evaluateOnCpu(input, fallbackBridge)
      }
    }
  }

  override def checkpoint(): Unit = ()

  override def restore(): Unit = ()
}

object GpuVariantGet extends Logging {
  // One grammar for both validating a path and splitting it into steps.
  private val PathStepPattern = """\.[A-Za-z_][A-Za-z0-9_]*|\[[0-9]+\]"""
  private val SupportedPath = ("^\\$(?:" + PathStepPattern + ")+$").r
  private val PathStep = PathStepPattern.r
  private val ArrayIndex = """\[([0-9]+)\]""".r

  def isSupportedTargetType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
        DoubleType | StringType => true
    case target: DecimalType => GpuVariantDecimal.isSupportedTargetType(target)
    case _ => false
  }

  // Checked against this class's defining loader, which also resolves the expression's cuDF
  // classes, rather than the tagging thread's context loader. Only a positive answer is cached; a
  // negative one is recomputed on each tagging pass, which costs a few reflective lookups.
  @volatile private var variantCudfAvailable = false
  @volatile private var variantDecimalCudfAvailable = false

  private def loadCudfClass(name: String): Class[_] =
    Class.forName(name, false, getClass.getClassLoader)

  def isVariantCudfAvailable: Boolean = variantCudfAvailable || {
    val available = GpuVariantExtraction.hasRequiredApis(loadCudfClass)
    variantCudfAvailable = available
    available
  }

  def isVariantDecimalCudfAvailable: Boolean = variantDecimalCudfAvailable || {
    val available = GpuVariantExtraction.hasDecimalApis(loadCudfClass)
    variantDecimalCudfAvailable = available
    available
  }

  /**
   * Decodes the extracted field on the GPU into an owning column, or returns None when any row
   * needs Spark's CPU semantics, so that the caller evaluates the whole batch on the CPU.
   *
   * The batch stays on the GPU when every row is a SQL-null parent, a missing path, a Variant null,
   * or a value one of the exact-type decoders accepts. Decimal targets are stricter: a missing path
   * or Variant null also falls back (see `GpuVariantDecimal.decode`). Spark returns NULL for a
   * Variant null whatever the target type, and no decoder accepts one. Extraction leaves the raw
   * value null only for ROW_NULL, MISSING_PATH and MALFORMED_VARIANT, and a decoder leaves a row
   * valid only when it decoded the row. Malformed bytes found during extraction, and rows that
   * Spark's Variant constructor rejects, fall back so that Spark raises its error. So do missing
   * paths where a step met a primitive type ID that Spark does not recognize.
   */
  private[rapids] def decodeVariantField(
      extraction: GpuVariantExtraction.Extraction,
      dt: DataType): Option[ColumnVector] = {
    def covered(decoded: ColumnView) = isCovered(extraction, decoded)
    dt match {
      case BooleanType => decodeExactType(extraction, DType.BOOL8, covered)
      case FloatType => decodeExactType(extraction, DType.FLOAT32, covered)
      case DoubleType => decodeExactType(extraction, DType.FLOAT64, covered)
      case StringType => decodeExactType(extraction, DType.STRING, covered)
      case target: DecimalType => GpuVariantDecimal.decode(extraction, target)
      case ByteType | ShortType | IntegerType | LongType =>
        val decoded = Seq(DType.INT8, DType.INT16, DType.INT32, DType.INT64).safeMap {
          sourceType => VariantUtils.castVariantValue(extraction.getValue, sourceType)
        }
        // Coverage uses the combined column, so a row two decoders accept is counted once.
        withResource(withResource(decoded)(normalizeIntegers)) { logicalLongs =>
          if (covered(logicalLongs)) Some(narrowInteger(logicalLongs, dt)) else None
        }
      case other =>
        throw new IllegalArgumentException(s"unsupported variant target type: $other")
    }
  }

  private def decodeExactType(
      extraction: GpuVariantExtraction.Extraction,
      targetType: DType,
      covered: ColumnView => Boolean): Option[ColumnVector] = {
    withResource(VariantUtils.castVariantValue(extraction.getValue, targetType)) { decoded =>
      if (covered(decoded)) Some(decoded.incRefCount()) else None
    }
  }

  private[rapids] def isCovered(
      extraction: GpuVariantExtraction.Extraction,
      decoded: ColumnView): Boolean = {
    val rows = extraction.rowCount
    val parentNullCount = extraction.parentNullCount
    val rawNulls = extraction.getValue.getNullCount
    val undecoded = rows - rawNulls - (decoded.getRowCount - decoded.getNullCount)
    val status = extraction.getStatus
    // The status reductions and byte checks run only when the null counts leave a question open.
    // When every raw null is a SQL-null parent, no row is missing or malformed at extraction.
    (undecoded == 0 || undecoded == countStatus(status, VariantOperationStatus.VARIANT_NULL)) &&
      (rawNulls == parentNullCount ||
        (!containsStatus(status, VariantOperationStatus.MALFORMED_VARIANT) &&
          !extraction.hasUnrecognizedPrimitiveStep)) &&
      (parentNullCount == rows || !extraction.failsVariantConstructor())
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

  /** The steps of a supported path, so `$.a[0]` gives `.a` and `[0]`; None for other paths. */
  def supportedPathSteps(path: String): Option[Seq[String]] =
    parseSupportedPath(path).map(p => PathStep.findAllIn(p.substring(1)).toSeq)

  /**
   * The primitive type IDs (0 to 63) that this JVM's Spark rejects, found by evaluating Spark's own
   * `try_variant_get` once per ID on a one-byte primitive at the root. A path step that meets a
   * primitive reads only its type, so Spark throws exactly for the IDs it does not recognize.
   * Spark 4.0 to 4.2 recognize 0 to 16 and 20. Asking Spark keeps the GPU in step with any build,
   * including Databricks, that recognizes a different set.
   */
  private[rapids] lazy val UnrecognizedPrimitiveTypeIds: Seq[Int] = {
    val probe = VariantGet(BoundReference(0, VariantType, nullable = true), Literal("$[0]"),
      IntegerType, failOnError = false, timeZoneId = Some("UTC"))
    val metadata = Array[Byte](1, 0, 0)
    probeUnrecognizedIds { id =>
      probe.eval(InternalRow(new VariantVal(Array((id << 2).toByte), metadata)))
    }
  }

  private val UnknownPrimitiveCondition = "UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT"

  /**
   * The IDs for which `eval` throws Spark's unknown-primitive error. Any other failure says nothing
   * about the ID, so it is logged and the ID is treated as unrecognized: that costs a CPU fallback
   * for batches with missing rows but never returns NULL where Spark would throw.
   */
  private[rapids] def probeUnrecognizedIds(eval: Int => Unit): Seq[Int] = {
    val unexpected = scala.collection.mutable.ArrayBuffer[(Int, Throwable)]()
    val ids = (0 until 64).filter { id =>
      try {
        eval(id)
        false
      } catch {
        case e: SparkThrowable if e.getCondition == UnknownPrimitiveCondition => true
        case e @ (_: Exception | _: LinkageError) =>
          unexpected += id -> e
          true
      }
    }
    unexpected.headOption.foreach { case (id, e) =>
      logWarning("Probing Spark's Variant primitive type IDs failed unexpectedly for " +
        s"${unexpected.size} of 64 IDs (${unexpected.map(_._1).mkString(", ")}). " +
        "try_variant_get batches with missing paths fall back to the CPU when they meet those " +
        s"IDs. First failure, for ID $id:", e)
    }
    ids
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
