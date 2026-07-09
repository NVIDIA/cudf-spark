/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.window

import java.util.concurrent.TimeUnit

import ai.rapids.cudf
import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, GroupByScanAggregation, RollingAggregation, RollingAggregationOnColumn, Scalar, ScanAggregation}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuOverrides.wrapExpr
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{GpuWindowUtil, ShimExpression}
import scala.util.{Left, Right}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, CollectList, CollectSet, Count, Max, Min, Sum}
import org.apache.spark.sql.rapids.{AddOverflowChecks, GpuAnsi, GpuCreateNamedStruct, GpuDivide, GpuSubtract}
import org.apache.spark.sql.rapids.aggregate.{GpuAggregateExpression, GpuAggregateFunction, GpuCount}
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

abstract class GpuWindowExpressionMetaBase(
    windowExpression: WindowExpression,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends ExprMeta[WindowExpression](windowExpression, conf, parent, rule) {

  private def getAndCheckRowBoundaryValue(boundary: Expression) : Int = boundary match {
    case literal: Literal =>
      literal.dataType match {
        case IntegerType =>
          literal.value.asInstanceOf[Int]
        case t =>
          willNotWorkOnGpu(s"unsupported window boundary type $t")
          -1
      }
    case UnboundedPreceding => Int.MinValue
    case UnboundedFollowing => Int.MaxValue
    case CurrentRow => 0
    case _ =>
      willNotWorkOnGpu("unsupported window boundary type")
      -1
  }

  /** Tag if RangeFrame expression is supported */
  def tagOtherTypesForRangeFrame(bounds: Expression): Unit = {
    willNotWorkOnGpu(s"the type of boundary is not supported in a window range" +
      s" function, found $bounds")
  }

  override def tagExprForGpu(): Unit = {

    // Must have two children:
    //  1. An AggregateExpression as the window function: SUM, MIN, MAX, COUNT
    //  2. A WindowSpecDefinition, defining the window-bounds, partitioning, and ordering.
    val windowFunction = wrapped.windowFunction

    wrapped.windowSpec.frameSpecification match {
      case spec: SpecifiedWindowFrame =>
        spec.frameType match {
          case RowFrame =>
            // Will also verify that the types are what we expect.
            val lower = getAndCheckRowBoundaryValue(spec.lower)
            val upper = getAndCheckRowBoundaryValue(spec.upper)
            windowFunction match {
              case _: Lead | _: Lag => // ignored we are good
              case _ =>
                // need to be sure that the lower/upper are acceptable
                // Negative bounds are allowed, so long as lower does not exceed upper.
                if (upper < lower) {
                  willNotWorkOnGpu("upper-bounds must equal or exceed the lower bounds. " +
                    s"Found lower=$lower, upper=$upper ")
                }
                // Also check for negative offsets.
                if (upper < 0 || lower > 0) {
                  windowFunction.asInstanceOf[AggregateExpression].aggregateFunction match {
                    case _: Average => // Supported
                    case _: CollectList => // Supported
                    case _: CollectSet => // Supported
                    case _: Count => // Supported
                    case _: Max => // Supported
                    case _: Min => // Supported
                    case _: Sum => // Supported
                    case f: AggregateFunction =>
                      willNotWorkOnGpu("negative row bounds unsupported for specified " +
                        s"aggregation: ${f.prettyName}")
                  }
                }
            }
          case RangeFrame =>
            // Spark by default does a RangeFrame if no RowFrame is given
            // even for columns that are not time type columns. We can switch this to row
            // based iff the ranges we are looking at both unbounded.
            if (spec.isUnbounded) {
              // this is okay because we will translate it to be a row query
            } else {
              // check whether order by column is supported or not
              val orderSpec = wrapped.windowSpec.orderSpec
              if (orderSpec.length > 1) {
                // We only support a single order by column
                willNotWorkOnGpu("only a single date/time or numeric (Boolean exclusive) " +
                  "based column in window range functions is supported")
              }
              val orderByTypeSupported = orderSpec.forall { so =>
                so.dataType match {
                  case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
                       DateType | TimestampType | StringType | DecimalType() => true
                  case _ => false
                }
              }
              if (!orderByTypeSupported) {
                willNotWorkOnGpu(s"the type of orderBy column is not supported in a window" +
                  s" range function, found ${orderSpec.head.dataType}")
              }

              def checkRangeBoundaryConfig(dt: DataType): Unit = {
                dt match {
                  case ByteType => if (!conf.isRangeWindowByteEnabled) willNotWorkOnGpu(
                    s"Range window frame is not 100% compatible when the order by type is " +
                      s"byte and the range value calculated has overflow. " +
                      s"To enable it please set ${RapidsConf.ENABLE_RANGE_WINDOW_BYTES} to true.")
                  case ShortType => if (!conf.isRangeWindowShortEnabled) willNotWorkOnGpu(
                    s"Range window frame is not 100% compatible when the order by type is " +
                      s"short and the range value calculated has overflow. " +
                      s"To enable it please set ${RapidsConf.ENABLE_RANGE_WINDOW_SHORT} to true.")
                  case IntegerType => if (!conf.isRangeWindowIntEnabled) willNotWorkOnGpu(
                    s"Range window frame is not 100% compatible when the order by type is " +
                      s"int and the range value calculated has overflow. " +
                      s"To enable it please set ${RapidsConf.ENABLE_RANGE_WINDOW_INT} to true.")
                  case LongType => if (!conf.isRangeWindowLongEnabled) willNotWorkOnGpu(
                    s"Range window frame is not 100% compatible when the order by type is " +
                      s"long and the range value calculated has overflow. " +
                      s"To enable it please set ${RapidsConf.ENABLE_RANGE_WINDOW_LONG} to true.")
                  case FloatType => if (!conf.isRangeWindowFloatEnabled) willNotWorkOnGpu(
                    s"Range window frame is currently disabled when the order by type is float. " +
                      s"To enable it please set ${RapidsConf.ENABLE_RANGE_WINDOW_FLOAT} to true.")
                  case DoubleType => if (!conf.isRangeWindowDoubleEnabled) willNotWorkOnGpu(
                    s"Range window frame is currently disabled when the order by type is double. " +
                      s"To enable it please set ${RapidsConf.ENABLE_RANGE_WINDOW_DOUBLE} to true.")
                  case DecimalType() => if (!conf.isRangeWindowDecimalEnabled) willNotWorkOnGpu(
                      s"To enable DECIMAL order by columns with Range window frames, " +
                      s"please set ${RapidsConf.ENABLE_RANGE_WINDOW_DECIMAL} to true.")
                  case _ => // never reach here
                }
              }

              // check whether the boundaries are supported or not.
              Seq(spec.lower, spec.upper).foreach {
                case l @ Literal(_, ByteType | ShortType | IntegerType |
                                    LongType | FloatType | DoubleType | DecimalType()) =>
                  checkRangeBoundaryConfig(l.dataType)
                case Literal(ci: CalendarInterval, CalendarIntervalType) =>
                  // interval is only working for TimeStampType
                  if (ci.months != 0) {
                    willNotWorkOnGpu("interval months isn't supported")
                  }
                case UnboundedFollowing | UnboundedPreceding | CurrentRow =>
                case anythings => tagOtherTypesForRangeFrame(anythings)
              }
            }
        }
      case other =>
        willNotWorkOnGpu(s"only SpecifiedWindowFrame is a supported window-frame specification. " +
            s"Found ${other.prettyName}")
    }
  }

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  override def convertToGpuImpl(): GpuExpression = {
    val Seq(left, right) = childExprs.map(_.convertToGpu())
    GpuWindowExpression(left, right.asInstanceOf[GpuWindowSpecDefinition])
  }
}

case class GpuWindowExpression(windowFunction: Expression, windowSpec: GpuWindowSpecDefinition)
  extends GpuUnevaluable with ShimExpression {

  override def children: Seq[Expression] = windowFunction :: windowSpec :: Nil

  override def dataType: DataType = windowFunction.dataType

  override def foldable: Boolean = windowFunction.foldable

  override def nullable: Boolean = windowFunction.nullable

  override def toString: String = s"$windowFunction $windowSpec"

  override def sql: String = windowFunction.sql + " OVER " + windowSpec.sql

  lazy val normalizedFrameSpec: GpuSpecifiedWindowFrame = {
    val fs = windowFrameSpec.canonicalized.asInstanceOf[GpuSpecifiedWindowFrame]
    fs.frameType match {
      case RangeFrame if fs.isUnbounded =>
        GpuSpecifiedWindowFrame(RowFrame, fs.lower, fs.upper)
      case _ => fs
    }
  }

  private val windowFrameSpec = windowSpec.frameSpecification.asInstanceOf[GpuSpecifiedWindowFrame]
  lazy val wrappedWindowFunc: GpuWindowFunction = windowFunction match {
    case func: GpuWindowFunction => func
    case agg: GpuAggregateExpression => agg.aggregateFunction match {
      case func: GpuWindowFunction => func
      case other =>
        throw new IllegalStateException(s"${other.getClass} is not a supported window aggregation")
    }
    case other =>
      throw new IllegalStateException(s"${other.getClass} is not a supported window function")
  }

  private[this] lazy val optimizedRunningWindow: Option[GpuRunningWindowFunction] = {
    if (normalizedFrameSpec.frameType == RowFrame &&
        GpuWindowExec.isRunningWindow(windowSpec) &&
        wrappedWindowFunc.isInstanceOf[GpuRunningWindowFunction]) {
      val runningWin = wrappedWindowFunc.asInstanceOf[GpuRunningWindowFunction]
      val isSupported = if (windowSpec.partitionSpec.isEmpty) {
        runningWin.isScanSupported
      } else {
        runningWin.isGroupByScanSupported
      }
      if (isSupported) {
        Some(runningWin)
      } else {
        None
      }
    } else {
      None
    }
  }

  lazy val isOptimizedRunningWindow: Boolean = optimizedRunningWindow.isDefined

  def initialProjections(isRunningBatched: Boolean): Seq[Expression] = {
    val running = optimizedRunningWindow
    if (running.isDefined) {
      val r = running.get
      if (windowSpec.partitionSpec.isEmpty) {
        r.scanInputProjection(isRunningBatched)
      } else {
        r.groupByScanInputProjection(isRunningBatched)
      }
    } else {
      wrappedWindowFunc.asInstanceOf[GpuAggregateWindowFunction].windowInputProjection
    }
  }
}

class GpuWindowSpecDefinitionMeta(
    windowSpec: WindowSpecDefinition,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends ExprMeta[WindowSpecDefinition](windowSpec, conf, parent, rule) {

  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    windowSpec.partitionSpec.map(wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    windowSpec.orderSpec.map(wrapExpr(_, conf, Some(this)))
  val windowFrame: BaseExprMeta[WindowFrame] =
    wrapExpr(windowSpec.frameSpecification, conf, Some(this))

  override val ignoreUnsetDataTypes: Boolean = true

  override def tagExprForGpu(): Unit = {
    if (!windowSpec.frameSpecification.isInstanceOf[SpecifiedWindowFrame]) {
      willNotWorkOnGpu(s"WindowFunctions without a SpecifiedWindowFrame are unsupported.")
    }
  }

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  override def convertToGpuImpl(): GpuExpression = {
    GpuWindowSpecDefinition(
      partitionSpec.map(_.convertToGpu()),
      orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
      windowFrame.convertToGpu().asInstanceOf[GpuWindowFrame])
  }
}

case class GpuWindowSpecDefinition(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    frameSpecification: GpuWindowFrame)
  extends GpuExpression with ShimExpression with GpuUnevaluable {

  override def children: Seq[Expression] = partitionSpec ++ orderSpec :+ frameSpecification

  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess &&
      frameSpecification.isInstanceOf[GpuSpecifiedWindowFrame]

  override def nullable: Boolean = true

  override def foldable: Boolean = false

  override def dataType: DataType = {
    // Note: WindowSpecDefinition has no dataType. Should throw UnsupportedOperationException.
    // Setting this to a concrete type to work around bug in SQL logging in certain
    // Spark versions, which mistakenly call `dataType()` on Unevaluable expressions.
    IntegerType
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    frameSpecification match {
      case GpuUnspecifiedFrame =>
        TypeCheckFailure(
          "Cannot use an UnspecifiedFrame. This should have been converted during analysis. " +
            "Please file a bug report.")
      case f: GpuSpecifiedWindowFrame if f.frameType == RangeFrame && !f.isUnbounded &&
        orderSpec.isEmpty =>
        TypeCheckFailure(
          "A range window frame cannot be used in an unordered window specification.")
      case f: GpuSpecifiedWindowFrame if f.frameType == RangeFrame && f.isValueBound &&
        orderSpec.size > 1 =>
        TypeCheckFailure(
          s"A range window frame with value boundaries cannot be used in a window specification " +
            s"with multiple order by expressions: ${orderSpec.mkString(",")}")
      case f: GpuSpecifiedWindowFrame if f.frameType == RangeFrame && f.isValueBound &&
        !isValidFrameType(f.valueBoundary.head.dataType) =>
        TypeCheckFailure(
          s"The data type '${orderSpec.head.dataType.catalogString}' used in the order " +
            "specification does not match the data type " +
            s"'${f.valueBoundary.head.dataType.catalogString}' which is used in the range frame.")
      case _ => TypeCheckSuccess
    }
  }

  override def sql: String = {
    def toSql(exprs: Seq[Expression], prefix: String): Seq[String] = {
      Seq(exprs).filter(_.nonEmpty).map(_.map(_.sql).mkString(prefix, ", ", ""))
    }

    val elements =
      toSql(partitionSpec, "PARTITION BY ") ++
        toSql(orderSpec, "ORDER BY ") ++
        Seq(frameSpecification.sql)
    elements.mkString("(", " ", ")")
  }

  private def isValidFrameType(ft: DataType): Boolean = {
    GpuWindowUtil.isValidRangeFrameType(orderSpec.head.dataType, ft)
  }
}

abstract class GpuSpecifiedWindowFrameMetaBase(
    windowFrame: SpecifiedWindowFrame,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends ExprMeta[SpecifiedWindowFrame](windowFrame, conf, parent, rule) {

  // SpecifiedWindowFrame has no associated dataType.
  override val ignoreUnsetDataTypes: Boolean = true

  /**
   * Tag RangeFrame for other types and get the value
   */
  def getAndTagOtherTypesForRangeFrame(bounds : Expression, isLower : Boolean): Long = {
    willNotWorkOnGpu(s"Bounds for Range-based window frames must be specified in numeric" +
      s" type (Boolean exclusive) or CalendarInterval. Found ${bounds.dataType}")
    if (isLower) -1 else 1 // not check again
  }

  override def tagExprForGpu(): Unit = {
    if (windowFrame.frameType.equals(RangeFrame)) {
      // Expect either SpecialFrame (UNBOUNDED PRECEDING/FOLLOWING, or CURRENT ROW),
      // or CalendarIntervalType in days.

      // Check that:
      //  1. if `bounds` is specified as a Literal, it is specified in DAYS.
      //  2. if `bounds` is a  lower-bound, it can't be ahead of the current row.
      //  3. if `bounds` is an upper-bound, it can't be behind the current row.
      def checkIfInvalid(bounds : Expression, isLower : Boolean) : Option[String] = {

        if (!bounds.isInstanceOf[Literal]) {
          // Bounds are likely SpecialFrameBoundaries (CURRENT_ROW, UNBOUNDED PRECEDING/FOLLOWING).
          return None
        }

        /**
         * Check bounds value relative to current row:
         *  1. lower-bound should not be ahead of the current row.
         *  2. upper-bound should not be behind the current row.
         */
        def checkBounds[T](boundsValue: T)
                          (implicit ev: Numeric[T]): Option[String] = {
          if (isLower && ev.compare(boundsValue, ev.zero) > 0) {
            Some(s"Lower-bounds ahead of current row is not supported. Found: $boundsValue")
          }
          else if (!isLower && ev.compare(boundsValue, ev.zero) < 0) {
            Some(s"Upper-bounds behind current row is not supported. Found: $boundsValue")
          }
          else {
            None
          }
        }

        bounds match {
          case Literal(value, ByteType) =>
            checkBounds(value.asInstanceOf[Byte].toLong)
          case Literal(value, ShortType) =>
            checkBounds(value.asInstanceOf[Short].toLong)
          case Literal(value, IntegerType) =>
            checkBounds(value.asInstanceOf[Int].toLong)
          case Literal(value, LongType) =>
            checkBounds(value.asInstanceOf[Long])
          case Literal(value, FloatType) =>
            checkBounds(value.asInstanceOf[Float])
          case Literal(value, DoubleType) =>
            checkBounds(value.asInstanceOf[Double])
          case Literal(value: Decimal, DecimalType()) =>
            checkBounds(BigInt(value.toJavaBigDecimal.unscaledValue()))
          case Literal(ci: CalendarInterval, CalendarIntervalType) =>
            if (ci.months != 0) {
              willNotWorkOnGpu("interval months isn't supported")
            }
            // return the total microseconds
            try {
              checkBounds(
                  Math.addExact(
                    Math.multiplyExact(ci.days.toLong, TimeUnit.DAYS.toMicros(1)),
                    ci.microseconds))
            } catch {
              case _: ArithmeticException =>
                willNotWorkOnGpu("windows over timestamps are converted to microseconds " +
                  s"and $ci is too large to fit")
                None
            }
          case _ =>
            getAndTagOtherTypesForRangeFrame(bounds, isLower)
            None
        }
      }

      val invalidUpper = checkIfInvalid(windowFrame.upper, isLower = false)
      if (invalidUpper.nonEmpty) {
        willNotWorkOnGpu(invalidUpper.get)
      }

      val invalidLower = checkIfInvalid(windowFrame.lower, isLower = true)
      if (invalidLower.nonEmpty) {
        willNotWorkOnGpu(invalidLower.get)
      }
    }

    if (windowFrame.frameType.equals(RowFrame)) {

      windowFrame.lower match {
        case literal : Literal =>
          if (!literal.value.isInstanceOf[Int]) {
            willNotWorkOnGpu(s"Literal Lower-bound of ROWS window-frame must be of INT type. " +
              s"Found ${literal.dataType}")
          }
          // We don't support a lower bound > 0 except for lead/lag where it is required
          // That check is done in GpuWindowExpressionMeta where it knows what type of operation
          // is being done
        case UnboundedPreceding =>
        case CurrentRow =>
        case _ =>
          willNotWorkOnGpu(s"Lower-bound of ROWS window-frame must be an INT literal," +
            s"UNBOUNDED PRECEDING, or CURRENT ROW. " +
            s"Found unexpected bound: ${windowFrame.lower.prettyName}")
      }

      windowFrame.upper match {
        case literal : Literal =>
          if (!literal.value.isInstanceOf[Int]) {
            willNotWorkOnGpu(s"Literal Upper-bound of ROWS window-frame must be of INT type. " +
              s"Found ${literal.dataType}")
          }
          // We don't support a upper bound < 0 except for lead/lag where it is required
          // That check is done in GpuWindowExpressionMeta where it knows what type of operation
          // is being done
        case UnboundedFollowing =>
        case CurrentRow =>
        case _ => willNotWorkOnGpu(s"Upper-bound of ROWS window-frame must be an INT literal," +
          s"UNBOUNDED FOLLOWING, or CURRENT ROW. " +
          s"Found unexpected bound: ${windowFrame.upper.prettyName}")
      }
    }
  }

  override def convertToGpuImpl(): GpuExpression = {
    val Seq(left, right) = childExprs.map(_.convertToGpu())
    GpuSpecifiedWindowFrame(windowFrame.frameType, left, right)
  }
}

trait GpuWindowFrame extends GpuExpression with GpuUnevaluable with ShimExpression {
  override def children: Seq[Expression] = Nil

  override def dataType: DataType = {
    // Note: WindowFrame has no dataType. Should throw UnsupportedOperationException.
    // Setting this to a concrete type to work around bug in SQL logging in certain
    // Spark versions, which mistakenly call `dataType()` on Unevaluable expressions.
    IntegerType
  }

  override def foldable: Boolean = false

  override def nullable: Boolean = false
}

case object GpuUnspecifiedFrame extends GpuWindowFrame // Placeholder, to handle UnspecifiedFrame

// This class closely follows what's done in SpecifiedWindowFrame.
case class GpuSpecifiedWindowFrame(
                                    frameType: FrameType,
                                    lower: Expression,
                                    upper: Expression)
  extends GpuWindowFrame {

  override def children: Seq[Expression] = lower :: upper :: Nil

  lazy val valueBoundary: Seq[Expression] =
    children.filterNot(_.isInstanceOf[GpuSpecialFrameBoundary])

  override def checkInputDataTypes(): TypeCheckResult = {
    // Check lower value.
    val lowerCheck = checkBoundary(lower, "lower")
    if (lowerCheck.isFailure) {
      return lowerCheck
    }

    // Check upper value.
    val upperCheck = checkBoundary(upper, "upper")
    if (upperCheck.isFailure) {
      return upperCheck
    }

    // Check combination (of expressions).
    (lower, upper) match {
      case (l: GpuExpression, u: GpuExpression) if !isValidFrameBoundary(l, u) =>
        TypeCheckFailure(s"Window frame upper bound '$upper' does not follow the lower bound " +
          s"'$lower'.")
      case (_: GpuSpecialFrameBoundary, _) => TypeCheckSuccess
      case (_, _: GpuSpecialFrameBoundary) => TypeCheckSuccess
      case (l: GpuExpression, u: GpuExpression) if l.dataType != u.dataType =>
        TypeCheckFailure(
          s"Window frame bounds '$lower' and '$upper' do no not have the same data type: " +
            s"'${l.dataType.catalogString}' <> '${u.dataType.catalogString}'")
      case (l: GpuExpression, u: GpuExpression) if isGreaterThan(l, u) =>
        TypeCheckFailure(
          "The lower bound of a window frame must be less than or equal to the upper bound")
      case _ => TypeCheckSuccess
    }
  }

  override def sql: String = {
    val lowerSql = boundarySql(lower)
    val upperSql = boundarySql(upper)
    s"${frameType.sql} BETWEEN $lowerSql AND $upperSql"
  }

  def isUnbounded: Boolean = {
    (lower, upper) match {
      case (l:GpuSpecialFrameBoundary, u:GpuSpecialFrameBoundary) =>
        l.boundary == UnboundedPreceding && u.boundary == UnboundedFollowing
      case _ => false
    }
  }

  def isValueBound: Boolean = valueBoundary.nonEmpty

  def isOffset: Boolean = (lower, upper) match {
    case (l: Expression, u: Expression) => frameType == RowFrame && l == u
    case _ => false
  }

  private def boundarySql(expr: Expression): String = expr match {
    case e: GpuSpecialFrameBoundary => e.sql
    case u: UnaryMinus => u.child.sql + " PRECEDING"
    case e: Expression => e.sql + " FOLLOWING"
  }

  // Check whether the left boundary value is greater than the right boundary value. It's required
  // that the both expressions have the same data type.
  // Since CalendarIntervalType is not comparable, we only compare expressions that are AtomicType.
  //
  // Note: This check is currently skipped for GpuSpecifiedWindowFrame,
  // because: AtomicType has protected access in Spark. It is not available here.
  private def isGreaterThan(l: Expression, r: Expression): Boolean = l.dataType match {
    // case _: org.apache.spark.sql.types.AtomicType =>
    //   GreaterThan(l, r).eval().asInstanceOf[Boolean]
    case _ => false
  }

  private def checkBoundary(b: Expression, location: String): TypeCheckResult = b match {
    case _: GpuSpecialFrameBoundary => TypeCheckSuccess
    case e: Expression if !e.foldable =>
      TypeCheckFailure(s"Window frame $location bound '$e' is not a literal.")
    // Skipping type checks, because AbstractDataType::acceptsType() has protected access.
    // This should have been checked already.
    //
    // case e: Expression if !frameType.inputType.acceptsType(e.dataType) =>
    //   TypeCheckFailure(
    //     s"The data type of the $location bound '${e.dataType.catalogString}' does not match " +
    //       s"the expected data type '${frameType.inputType.simpleString}'.")
    case _ => TypeCheckSuccess
  }

  private def isValidFrameBoundary(l: GpuExpression, u: GpuExpression): Boolean = {
    (l, u) match {
      case (low: GpuSpecialFrameBoundary, _) if low.boundary == UnboundedFollowing => false
      case (_, up: GpuSpecialFrameBoundary)  if  up.boundary == UnboundedPreceding => false
      case _ => true
    }
  }
}

case class GpuSpecialFrameBoundary(boundary : SpecialFrameBoundary)
  extends GpuExpression with ShimExpression with GpuUnevaluable {
  override def children : Seq[Expression] = Nil
  override def dataType: DataType = NullType
  override def foldable: Boolean = false
  override def nullable: Boolean = false

  /**
   * Maps boundary to an Int value that in some cases can be used to build up the window options
   * for a window aggregation. UnboundedPreceding and UnboundedFollowing produce Int.MinValue and
   * Int.MaxValue respectively. In row based operations this should be fine because we cannot have
   * a batch with that many rows in it anyways. For range based queries isUnbounded should be
   * called too, to properly interpret the data. CurrentRow produces 0 which works for both row and
   * range based queries.
   */
  def value : Int = {
    boundary match {
      case UnboundedPreceding => Int.MinValue
      case UnboundedFollowing => Int.MaxValue
      case CurrentRow => 0
      case anythingElse =>
        throw new UnsupportedOperationException(s"Unsupported window-bound $anythingElse!")
    }
  }

  def isUnbounded: Boolean = {
    boundary match {
      case UnboundedPreceding | UnboundedFollowing => true
      case _ => false
    }
  }
}

// This is here for now just to tag an expression as being a GpuWindowFunction and match
// Spark. This may expand in the future if other types of window functions show up.
trait GpuWindowFunction extends GpuUnevaluable with ShimExpression {
  /**
   * Get "min-periods" value, i.e. the minimum number of periods/rows
   * above which a non-null value is returned for the function.
   * Otherwise, null is returned.
   * @return Non-negative value for min-periods.
   */
  def getMinPeriods: Int = 1
}

/**
 * This is a special window function that simply replaces itself with one or more
 * window functions and other expressions that can be executed. This allows you to write
 * `GpuAverage` in terms of `GpuSum` and `GpuCount` which can both operate on all window
 * optimizations making `GpuAverage` be able to do the same.
 */
trait GpuReplaceWindowFunction extends GpuWindowFunction {
  /**
   * Return a new single expression that can replace the existing aggregation in window
   * calculations. Please note that this requires that there are no nested window operations.
   * For example you cannot do a SUM of AVERAGES with this currently. That support may be added
   * in the future.
   */
  def windowReplacement(spec: GpuWindowSpecDefinition): Expression

  /**
   * Return true if windowReplacement should be called to replace this GpuWindowFunction with
   * something else.
   */
  def shouldReplaceWindow(spec: GpuWindowSpecDefinition): Boolean = true
}

/**
 * GPU Counterpart of `AggregateWindowFunction`.
 * On the CPU this would extend `DeclarativeAggregate` and use the provided methods
 * to build up the expressions need to produce a result. For window operations we do it
 * in a single pass, where all of the data is available so instead we have out own set of
 * expressions.
 */
trait GpuAggregateWindowFunction extends GpuWindowFunction {
  /**
   * Using child references, define the shape of the vectors sent to the window operations
   */
  val windowInputProjection: Seq[Expression]

  /**
   * Create the aggregation operation to perform for Windowing. The input to this method
   * is a sequence of (index, ColumnVector) that corresponds one to one with what was
   * returned by [[windowInputProjection]].  The index is the index into the Table for the
   * corresponding ColumnVector. Some aggregations need extra values.
   */
  def windowAggregation(inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn

  /**
   * Do a final pass over the window aggregation output. This lets us cast the result to a desired
   * type or check for overflow. This is not used for GpuRunningWindowFunction. There you can use
   * `scanCombine`.
   */
  def windowOutput(result: ColumnVector): ColumnVector = result.incRefCount()
}

/**
 * A window function that is optimized for running windows using the cudf scan and group by
 * scan operations. In some cases, like row number and rank, Spark only supports them as running
 * window operations. This is why it directly extends GpuWindowFunction because it can be a stand
 * alone window function. In all other cases it should be combined with GpuAggregateWindowFunction
 * to provide a fully functional window operation. It should be noted that WindowExec tries to
 * deduplicate input projections and aggregations to reduce memory usage. Because of tracking
 * requirements it is required that there is a one to one relationship between an input projection
 * and a corresponding aggregation.
 */
trait GpuRunningWindowFunction extends GpuWindowFunction {
  /**
   * Get the input projections for a group by scan. This corresponds to a running window with
   * a partition by clause. The partition keys will be used as the grouping keys.
   * @param isRunningBatched is this for a batched running window that will use a fixer or not?
   * @return the input expressions that will be aggregated using the result from
   *         `groupByScanAggregation`
   */
  def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression]

  /**
   * Get the aggregations to perform on the results of `groupByScanInputProjection`. The
   * aggregations will be zipped with the values to produce the output.
   * @param isRunningBatched is this for a batched running window that will use a fixer or not?
   * @return the aggregations to perform as a group by scan.
   */
  def groupByScanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]]

  /**
   * Should a group by scan be run or not. This should never return false unless this is also an
   * instance of `GpuAggregateWindowFunction` so the window code can fall back to it for
   * computation.
   */
  def isGroupByScanSupported = true

  /**
   * Get the input projections for a scan. This corresponds to a running window without a
   * partition by clause.
   * @param isRunningBatched is this for a batched running window that will use a fixer or not?
   * @return the input expressions that will be aggregated using the result from
   *         `scanAggregation`
   */
  def scanInputProjection(isRunningBatched: Boolean): Seq[Expression]

  /**
   * Get the aggregations to perform on the results of `scanInputProjection`. The
   * aggregations will be zipped with the values to produce the output.
   * @param isRunningBatched is this for a batched running window that will use a fixer or not?
   * @return the aggregations to perform as a group by scan.
   */
  def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]]

  /**
   * Should a scan be run or not. This should never return false unless this is also an
   * instance of `GpuAggregateWindowFunction` so the window code can fall back to it for
   * computation.
   */
  def isScanSupported = true

  /**
   * Provides a way to combine the result of multiple aggregations into a final value. By
   * default it requires that there is a single aggregation and works as just a pass through.
   * @param isRunningBatched is this for a batched running window that will use a fixer or not?
   * @param cols the columns to be combined
   * @return the result of combining these together.
   */
  def scanCombine(isRunningBatched: Boolean, cols: Seq[ColumnVector]): ColumnVector = {
    require(cols.length == 1, "Only one column is supported fro the default scan combine")
    cols.head.incRefCount()
  }
}

/**
 * Provides a way to process running window operations without needing to buffer and split the
 * batches on partition by boundaries. When this happens part of a partition by key set may
 * have been processed in the last batch, and the rest of it will need to be updated. For example
 * if we are doing a running min operation. We may first get in something like
 * <code>
 * PARTS:  1, 1,  2, 2
 * VALUES: 2, 3, 10, 9
 * </code>
 *
 * The output of processing this would result in a new column that would look like
 * <code>
 *   MINS: 2, 2, 10, 9
 * </code>
 *
 * But we don't know if the group with 2 in PARTS is done or not. So the fixer saved
 * the last value in MINS, which is a 9. When the next batch shows up
 *
 * <code>
 *  PARTS:  2,  2,  3,  3
 * VALUES: 11,  5, 13, 14
 * </code>
 *
 * We generate the window result again and get
 *
 * <code>
 *    MINS: 11, 5, 13, 13
 * </code>
 *
 * But we cannot output this yet because there may have been overlap with the previous batch.
 * The framework will figure that out and pass data into `fixUp` to do the fixing. It will
 * pass in MINS, and also a column of boolean values `true, true, false, false` to indicate
 * which rows overlapped with the previous batch.  In our min example `fixUp` will do a min
 * between the last value in the previous batch and the values that could overlap with it.
 *
 * <code>
 * RESULT: 9, 5, 13, 13
 * </code>
 * which can be output.
 */
trait BatchedRunningWindowFixer extends AutoCloseable with Retryable {
  /**
   * Fix up `windowedColumnOutput` with any stored state from previous batches.
   * Like all window operations the input data will have been sorted by the partition
   * by columns and the order by columns.
   *
   * @param samePartitionMask a mask that uses `true` to indicate the row
   *                          is for the same partition by keys that was the last row in the
   *                          previous batch or `false` to indicate it is not. If this is known
   *                          to be all true or all false values a single boolean is used. If
   *                          it can change for different rows than a column vector is provided.
   *                          Only values that are for the same partition by keys should be
   *                          modified. Because the input data is sorted by the partition by
   *                          columns the boolean values will be grouped together.
   * @param sameOrderMask a mask just like `samePartitionMask` but for ordering. This happens
   *                      for some operations like `rank` and `dense_rank` that use the ordering
   *                      columns in a row based query. This is not needed for all fixers and is not
   *                      free to calculate, so you must set `needsOrderMask` to true if you are
   *                      going to use it.
   * @param windowedColumnOutput the output of the windowAggregation without anything
   *                             fixed/modified. This should not be closed by `fixUp` as it will be
   *                             handled by the framework.
   * @return a fixed ColumnVector that was with outputs updated for items that were in the same
   *         group by key as the last row in the previous batch.
   */
  def fixUp(
      samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector

  def needsOrderMask: Boolean = false

  protected def incRef(col: cudf.ColumnView): cudf.ColumnVector = col.copyToColumnVector()
}

/**
 * Provides a way to process window operations without needing to buffer and split the
 * batches on partition by boundaries. When this happens part of a partition by key set may
 * have been processed in the previous batches, and may need to be updated. For example
 * if we are doing a min operation with unbounded preceding and unbounded following.
 * We may first get in something like
 * <code>
 * PARTS:  1, 1,  2, 2
 * VALUES: 2, 3, 10, 9
 * </code>
 *
 * The output of processing this would result in a new column that would look like
 * <code>
 *   MINS: 2, 2, 9, 9
 * </code>
 *
 * But we don't know if the group with 2 in PARTS is done or not. So the fixer saved
 * the last value in MINS, which is a 9, and caches the batch. When the next batch shows up
 *
 * <code>
 *  PARTS:  2,  2,  3,  3
 * VALUES: 11,  5, 13, 14
 * </code>
 *
 * We generate the window result again and get
 *
 * <code>
 *    MINS: 5, 5, 13, 13
 * </code>
 *
 * And now we need to grab the first entry which is a 5 and update the cached data with another min.
 * The cached data for PARTS=2 is now 5. We then need to go back and fix up all of the previous
 * batches that had something to do with PARTS=2. The first batch will be pulled from the cache
 * and updated to look like
 *
 * <code>
 *  PARTS: 1, 1,  2, 2
 * VALUES: 2, 3, 10, 9
 *   MINS: 2, 2,  5, 5
 * </code>
 * which can be output because we were able to fix up all of the PARTS in that batch.
 */
trait BatchedUnboundedToUnboundedWindowFixer extends AutoCloseable {
  /**
   * Called to fix up a batch. There is no guarantee on the order the batches are fixed. The only
   * ordering guarantee is that the state will be updated for all batches before any are "fixed"
   * @param samePartitionMask indicates which rows are a part of the same partition.
   * @param column the column of data to be fixed.
   * @return a column of data that was fixed.
   */
  def fixUp(samePartitionMask: Either[ColumnVector, Boolean], column: ColumnVector): ColumnVector

  /**
   * Clear any state so that updateState can be called again for a new partition by group.
   */
  def reset(): Unit

  /**
   * Cache and update any state needed. Because this is specific to unbounded preceding to
   * unbounded following the result should be the same for any row within a batch. As such, this is
   * only guaranteed to be called once per batch with the value from a row within the batch.
   * @param scalar the value to use to update what is cached.
   */
  def updateState(scalar: Scalar): Unit
}

/**
 * For many operations a running window (unbounded preceding to current row) can
 * process the data without dividing the data up into batches that contain all of the data
 * for a given group by key set. Instead we store a small amount of state from a previous result
 * and use it to fix the final result. This is a memory optimization.
 */
trait GpuBatchedRunningWindowWithFixer {

  /**
   * Checks whether the running window can be fixed up. This should be called before
   * newFixer(), to check whether the fixer would work.
   */
  def canFixUp: Boolean = true

  /**
   * Get a new class that can be used to fix up batched running window operations.
   */
  def newFixer(): BatchedRunningWindowFixer
}

/**
 * For many window operations the results in earlier rows depends on the results from the last
 * or later rows. In many of these cases we chunk the data based off of the partition by groups
 * and process the data at once. But this can lead to out of memory errors, or hitting the
 * row limit on some columns. Doing two passes through the data where the first pass processes
 * the data and a second pass fixes up the data can let us keep the data in the original batches
 * and reduce total memory usage. But this requires that some of the batches be made spillable
 * while we wait for the end of the partition by group.
 *
 * Right now this is written to be specific to windows that are unbounded preceding to unbounded
 * following, but it could be adapted to also work for current row to unbounded following, and
 * possibly more situations.
 */
trait GpuUnboundToUnboundWindowWithFixer {
  def newUnboundedToUnboundedFixer: BatchedUnboundedToUnboundedWindowFixer
}

/**
 * This is used to tag a GpuAggregateFunction that it has been tested to work properly
 * with `GpuUnboundedToUnboundedAggWindowExec`.
 */
trait GpuUnboundedToUnboundedWindowAgg extends GpuAggregateFunction

/**
 * Fixes up a count operation for unbounded preceding to unbounded following
 * @param errorOnOverflow if we need to throw an exception when an overflow happens or not.
 */
class CountUnboundedToUnboundedFixer(errorOnOverflow: Boolean)
    extends BatchedUnboundedToUnboundedWindowFixer {
  private var previousValue: Option[Long] = None

  override def reset(): Unit = {
    previousValue = None
  }

  override def updateState(scalar: Scalar): Unit = {
    // It should be impossible for count to produce a null.
    // Even if the input was all nulls the count is 0
    assert(scalar.isValid)
    if (previousValue.isEmpty) {
      previousValue = Some(scalar.getLong)
    } else {
      val old = previousValue.get
      previousValue = Some(old + scalar.getLong)
      if (errorOnOverflow && previousValue.get < 0) {
        // This matches what would happen in an add operation, which is where the overflow
        // in the CPU count would happen
        throw RapidsErrorUtils.arithmeticOverflowError(
          "One or more rows overflow for Add operation.")
      }
    }
  }

  override def close(): Unit = reset()

  override def fixUp(samePartitionMask: Either[ColumnVector, Boolean],
      column: ColumnVector): ColumnVector = {
    assert(previousValue.nonEmpty)
    withResource(Scalar.fromLong(previousValue.get)) { scalar =>
      samePartitionMask match {
        case scala.Left(cv) =>
          cv.ifElse(scalar, column)
        case scala.Right(true) =>
          ColumnVector.fromScalar(scalar, column.getRowCount.toInt)
        case _ =>
          column.incRefCount()
      }
    }
  }
}

class BatchedUnboundedToUnboundedBinaryFixer(val binOp: BinaryOp, val dataType: DataType)
    extends BatchedUnboundedToUnboundedWindowFixer {
  private var previousResult: Option[Scalar] = None

  override def updateState(scalar: Scalar): Unit = previousResult match {
    case None =>
      previousResult = Some(scalar.incRefCount())
    case Some(prev) =>
      // This is ugly, but for now it is simple to make it work
      val result = withResource(ColumnVector.fromScalar(prev, 1)) { p1 =>
        withResource(p1.binaryOp(binOp, scalar, prev.getType)) { result1 =>
          result1.getScalarElement(0)
        }
      }
      closeOnExcept(result) { _ =>
        previousResult.foreach(_.close)
        previousResult = Some(result)
      }
  }

  override def fixUp(samePartitionMask: Either[ColumnVector, Boolean],
      column: ColumnVector): ColumnVector = {
    val scalar =  previousResult match {
      case Some(value) =>
        value.incRefCount()
      case None =>
        GpuScalar.from(null, dataType)
    }

    withResource(scalar) { scalar =>
      samePartitionMask match {
        case scala.Left(cv) =>
          cv.ifElse(scalar, column)
        case scala.Right(true) =>
          ColumnVector.fromScalar(scalar, column.getRowCount.toInt)
        case _ =>
          column.incRefCount()
      }
    }
  }

  override def close(): Unit = reset()

  override def reset(): Unit = {
    previousResult.foreach(_.close())
    previousResult = None
  }
}

/**
 * This class fixes up batched running windows by performing a binary op on the previous value and
 * those in the the same partition by key group. It does not deal with nulls, so it works for things
 * like row_number and count, that cannot produce nulls, or for NULL_MIN and NULL_MAX that do the
 * right thing when they see a null.
 */
class BatchedRunningWindowBinaryFixer(val binOp: BinaryOp, val name: String)
    extends BatchedRunningWindowFixer with Logging {
  private var previousResult: Option[Scalar] = None

  // checkpoint
  private var checkpointPreviousResult: Option[Scalar] = None

  override def checkpoint(): Unit = {
    checkpointPreviousResult = previousResult
  }

  override def restore(): Unit = {
    if (checkpointPreviousResult.isDefined) {
      // close previous result
      previousResult match {
        case Some(r) if r != checkpointPreviousResult.get =>
          r.close()
        case _ =>
      }
      previousResult = checkpointPreviousResult
      checkpointPreviousResult = None
    }
  }

  def getPreviousResult: Option[Scalar] = previousResult

  def updateState(finalOutputColumn: cudf.ColumnVector): Unit = {
    logDebug(s"$name: updateState from $previousResult to...")
    previousResult.foreach(_.close)
    previousResult =
      Some(finalOutputColumn.getScalarElement(finalOutputColumn.getRowCount.toInt - 1))
    logDebug(s"$name: ... $previousResult")
  }

  override def fixUp(samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector = {
    logDebug(s"$name: fix up $previousResult $samePartitionMask")
    val ret = (previousResult, samePartitionMask) match {
      case (None, _) => incRef(windowedColumnOutput)
      case (Some(prev), scala.util.Right(mask)) =>
        if (mask) {
          windowedColumnOutput.binaryOp(binOp, prev, prev.getType)
        } else {
          // The mask is all false so do nothing
          incRef(windowedColumnOutput)
        }
      case (Some(prev), scala.util.Left(mask)) =>
        withResource(windowedColumnOutput.binaryOp(binOp, prev, prev.getType)) { updated =>
          mask.ifElse(updated, windowedColumnOutput)
        }
    }
    updateState(ret)
    ret
  }

  override def close(): Unit = {
    previousResult.foreach(_.close())
    previousResult = None
  }
}

/**
 * Common base class for batched running window fixers for FIRST() and LAST() window functions.
 * This mostly handles the checkpoint logic. The fixup logic is left to the concrete subclass.
 *
 * @param name Name of the function (E.g. "FIRST").
 * @param ignoreNulls Whether the function needs to ignore NULL values in the calculation.
 */
abstract class FirstLastRunningWindowFixerBase(val name: String, val ignoreNulls: Boolean = false)
  extends BatchedRunningWindowFixer with Logging {

  /**
   * Saved "carry-over" result that might be applied to the next batch.
   */
  protected[this] var previousResult: Option[Scalar] = None

  /**
   * Checkpoint result, in case it needs to be rolled back.
   */
  protected[this] var chkptPreviousResult: Option[Scalar] = None

  /**
   * Saves the last row from the `finalOutputColumn`, to carry over to the next
   * column processed by this fixer.
   */
  protected[this] def resetPrevious(finalOutputColumn: cudf.ColumnVector): Unit = {
    val numRows = finalOutputColumn.getRowCount.toInt
    if (numRows > 0) {
      val lastIndex = numRows - 1
      logDebug(s"$name: updateState from $previousResult to...")
      previousResult.foreach(_.close)
      previousResult = Some(finalOutputColumn.getScalarElement(lastIndex))
      logDebug(s"$name: ... $previousResult")
    }
  }

  /**
   * Save the state, so it can be restored in the case of a retry.
   * (This is called inside a Spark task context on executors.)
   */
  override def checkpoint(): Unit = chkptPreviousResult = previousResult

  /**
   * Restore the state that was saved by calling to "checkpoint".
   * (This is called inside a Spark task context on executors.)
   */
  override def restore(): Unit = {
    // If there is a previous checkpoint result, restore it to previousResult.
    if (chkptPreviousResult.isDefined) {
      // Close erstwhile previousResult.
      previousResult match {
        case Some(r) if r != chkptPreviousResult.get => r.close()
        case _ => // Nothing to close if result is None, or matches the checkpoint.
      }
    }
    previousResult = chkptPreviousResult
    chkptPreviousResult = None
  }

  override def close(): Unit = {
    previousResult.foreach(_.close)
    previousResult = None
  }
}

/**
 * Batched running window fixer for `FIRST() ` window functions. Supports fixing for batched
 * execution for `ROWS` and `RANGE` based window specifications.
 * @param ignoreNulls Whether the function needs to ignore NULL values in the calculation.
 */
class FirstRunningWindowFixer(ignoreNulls: Boolean = false)
  extends FirstLastRunningWindowFixerBase(name="First", ignoreNulls=ignoreNulls) {
  /**
   * Fix up `windowedColumnOutput` with any stored state from previous batches.
   * Like all window operations the input data will have been sorted by the partition
   * by columns and the order by columns.
   *
   * @param samePartitionMask    a mask that uses `true` to indicate the row
   *                             is for the same partition by keys that was the last row in the
   *                             previous batch or `false` to indicate it is not. If this is known
   *                             to be all true or all false values a single boolean is used. If
   *                             it can change for different rows than a column vector is provided.
   *                             Only values that are for the same partition by keys should be
   *                             modified. Because the input data is sorted by the partition by
   *                             columns the boolean values will be grouped together.
   * @param sameOrderMask        Similar mask for ordering. Unused for `FIRST`.
   * @param unfixedWindowResults the output of the windowAggregation without anything
   *                             fixed/modified. This should not be closed by `fixUp` as it will be
   *                             handled by the framework.
   * @return a fixed ColumnVector that was with outputs updated for items that were in the same
   *         group by key as the last row in the previous batch.
   */
  override def fixUp(samePartitionMask: Either[ColumnVector, Boolean],
                     sameOrderMask: Option[Either[ColumnVector, Boolean]],
                     unfixedWindowResults: ColumnView): ColumnVector = {
    // `sameOrderMask` is irrelevant for this operation.
    logDebug(s"$name: fix up $previousResult $samePartitionMask")
    val ret = (previousResult, samePartitionMask) match {
      case (None, _) =>
        // No previous result. Current result needs no fixing.
        incRef(unfixedWindowResults)
      case (Some(prev), Right(allRowsInSamePartition)) => // Boolean flag.
        // All the current batch results may be replaced.
        if (allRowsInSamePartition) {
          if (!ignoreNulls || prev.isValid) {
            // If !ignoreNulls, `prev` is the result for all rows.
            // If ignoreNulls *AND* `prev` isn't null, `prev` is the result for all rows.
            ColumnVector.fromScalar(prev, unfixedWindowResults.getRowCount.toInt)
          } else {
            // If ignoreNulls, *AND* `prev` is null, keep the current result.
            incRef(unfixedWindowResults)
          }
        } else {
          // No rows in the same partition. Current result needs no fixing.
          incRef(unfixedWindowResults)
        }
      case (Some(prev), Left(someRowsInSamePartition)) => // Boolean vector.
        if (!ignoreNulls || prev.isValid) {
          someRowsInSamePartition.ifElse(prev, unfixedWindowResults)
        } else {
          incRef(unfixedWindowResults)
        }
    }
    // Reset previous result.
    closeOnExcept(ret) { ret =>
      resetPrevious(ret)
      ret
    }
  }
}

/**
 * Batched running window fixer for `LAST() ` window functions. Supports fixing for batched
 * execution for `ROWS` and `RANGE` based window specifications.
 * @param ignoreNulls Whether the function needs to ignore NULL values in the calculation.
 */
class LastRunningWindowFixer(ignoreNulls: Boolean = false)
  extends FirstLastRunningWindowFixerBase(name="Last", ignoreNulls=ignoreNulls) {
  /**
   * Fixes up `unfixedWindowResults` with stored state from previous batch(es).
   * In this case (i.e. `LAST`), the previous result only comes into it if:
   *   1. There was a previous result at all.
   *   2. Nulls have to be ignored (i.e. ignoreNulls == true).
   *   3. The previous result (row) from the last batch is not null.
   *   4. There exists at least one `unfixedWindowResults` row that is NULL, and
   *      belongs to the same partition/group as the previous result.
   * In all other cases, the `unfixedWindowResults` prevail.
   *
   * @param samePartitionMask    a mask that uses `true` to indicate the row
   *                             is for the same partition by keys that was the last row in the
   *                             previous batch or `false` to indicate it is not. If this is known
   *                             to be all true or all false values a single boolean is used. If
   *                             it can change for different rows than a column vector is provided.
   *                             Only values that are for the same partition by keys should be
   *                             modified. Because the input data is sorted by the partition by
   *                             columns the boolean values will be grouped together.
   * @param sameOrderMask        Similar mask for ordering. Unused for `LAST`.
   * @param unfixedWindowResults the output of the windowAggregation without anything
   *                             fixed/modified. This should not be closed by `fixUp` as it will be
   *                             handled by the framework.
   * @return a fixed ColumnVector that was with outputs updated for items that were in the same
   *         group by key as the last row in the previous batch.
   */
  override def fixUp(samePartitionMask: Either[ColumnVector, Boolean],
                     sameOrderMask: Option[Either[ColumnVector, Boolean]], // Irrelevant to LAST.
                     unfixedWindowResults: ColumnView): ColumnVector = {
    logDebug(s"$name: fix up $previousResult $samePartitionMask")
    val ret = (previousResult, samePartitionMask) match {
      case (None, _) =>
        // No previous result. Current result needs no fixing.
        incRef(unfixedWindowResults)
      case (Some(_), Right(false)) => // samePartitionMask == false.
        // No rows in this batch correspond to the previousResult's partition.
        // Current result needs no fixing.
        incRef(unfixedWindowResults)
      case (Some(prev), Right(true)) => // samePartitionMask == true.
        // All the rows in this batch correspond to the previousResult's partition.
        if (!ignoreNulls || !prev.isValid) {
          // If !ignoreNulls, current result needs no fixing. The latest answer is the right one.
          // If ignoreNulls, but prev is NULL, current result is again the right answer.
          incRef(unfixedWindowResults)
        } else {
          // ignoreNulls *and* prev.isValid. => Final result now depends on the unfixed results.
          // `prev` must replace all null rows from the same group in the unfixed results.
          // In this case, that includes the entire column.
          unfixedWindowResults.replaceNulls(prev)
        }
      case (Some(prev), Left(someRowsInSamePartition)) => // samePartitionMask is a Boolean vector.
        if (!ignoreNulls || !prev.isValid) {
          // If !ignoreNulls, current result needs no fixing. The latest answer is the right one.
          // If ignoreNulls, but prev is NULL, current result is again the right answer.
          incRef(unfixedWindowResults)
        } else {
          // ignoreNulls==true, *and* prev.isValid.
          // prev must replace nulls for all rows that belong in the same group.
          val mustReplace = withResource(unfixedWindowResults.isNull) { isNull =>
            isNull.and(someRowsInSamePartition)
          }
          withResource(mustReplace) { mustReplace =>
            mustReplace.ifElse(prev, unfixedWindowResults)
          }
        }
    }
    // Reset previous result.
    closeOnExcept(ret) { ret =>
      resetPrevious(ret)
      ret
    }
  }
}

/**
 * This class fixes up batched running windows for sum. Sum is a lot like other binary op
 * fixers, but it has to special case nulls and that is not super generic.  In the future we
 * might be able to make this more generic but we need to see what the use case really is.
 */
class SumBinaryFixer(toType: DataType, isAnsi: Boolean)
    extends BatchedRunningWindowFixer with Logging {
  private val name = "sum"
  private var previousResult: Option[Scalar] = None
  private var previousOverflow: Option[Scalar] = None

  // checkpoint
  private var checkpointResult: Option[Scalar] = None
  private var checkpointOverflow: Option[Scalar] = None

  private val needsBasicOverflowCheck = isAnsi &&
    GpuAnsi.needBasicOpOverflowCheck(toType)

  override def checkpoint(): Unit = {
    checkpointOverflow = previousOverflow
    checkpointResult = previousResult
  }

  override def restore(): Unit = {
    if (checkpointOverflow.isDefined) {
      // close previous result
      previousOverflow match {
        case Some(r) if r != checkpointOverflow.get =>
          r.close()
        case _ =>
      }
      previousOverflow = checkpointOverflow
      checkpointOverflow = None
    }
    if (checkpointResult.isDefined) {
      // close previous result
      previousResult match {
        case Some(r) if r != checkpointResult.get =>
          r.close()
        case _ =>
      }
      previousResult = checkpointResult
      checkpointResult = None
    }
  }

  def updateState(finalOutputColumn: cudf.ColumnVector,
      wasOverflow: Option[cudf.ColumnVector]): Unit = {
    val lastIndex = finalOutputColumn.getRowCount.toInt - 1
    logDebug(s"$name: updateState from $previousResult to...")
    previousResult.foreach(_.close)
    previousResult = Some(finalOutputColumn.getScalarElement(lastIndex))
    previousOverflow.foreach(_.close())
    previousOverflow = wasOverflow.map(_.getScalarElement(lastIndex))
    logDebug(s"$name: ... $previousResult")
  }

  private def makeZeroScalar(dt: DType): Scalar = dt match {
    case DType.INT8 => Scalar.fromByte(0.toByte)
    case DType.INT16 => Scalar.fromShort(0.toShort)
    case DType.INT32 => Scalar.fromInt(0)
    case DType.INT64=> Scalar.fromLong(0)
    case DType.FLOAT32 => Scalar.fromFloat(0.0f)
    case DType.FLOAT64 => Scalar.fromDouble(0.0)
    case dec if dec.isDecimalType =>
      if (dec.getTypeId == DType.DTypeEnum.DECIMAL32) {
        Scalar.fromDecimal(dec.getScale, 0)
      } else if (dec.getTypeId == DType.DTypeEnum.DECIMAL64) {
        Scalar.fromDecimal(dec.getScale, 0L)
      } else {
        Scalar.fromDecimal(dec.getScale, java.math.BigInteger.ZERO)
      }
    case other =>
      throw new IllegalArgumentException(s"Making a zero scalar for $other is not supported")
  }

  private[this] def fixUpNonDecimal(samePartitionMask: Either[cudf.ColumnVector, Boolean],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector = {
    logDebug(s"$name: fix up $previousResult $samePartitionMask $isAnsi")
    val ret = (previousResult, samePartitionMask) match {
      case (None, _) => incRef(windowedColumnOutput)
      case (Some(prev), scala.util.Right(mask)) =>
        if (mask) {
          // ADD is not null safe, so we have to replace NULL with 0 if and only if prev is also
          // not null
          if (prev.isValid) {
            val nullsReplaced = withResource(windowedColumnOutput.isNull) { nulls =>
              withResource(makeZeroScalar(windowedColumnOutput.getType)) { zero =>
                nulls.ifElse(zero, windowedColumnOutput)
              }
            }
            withResource(nullsReplaced) { nullsReplaced =>
              if (needsBasicOverflowCheck) {
                closeOnExcept(nullsReplaced.binaryOp(BinaryOp.ADD, prev, prev.getType)) { ret =>
                  AddOverflowChecks.basicOpOverflowCheck(nullsReplaced, prev, ret)
                  ret
                }
              } else {
                nullsReplaced.binaryOp(BinaryOp.ADD, prev, prev.getType)
              }
            }
          } else {
            // prev is NULL but NULL + something == NULL which we don't want
            incRef(windowedColumnOutput)
          }
        } else {
          // The mask is all false so do nothing
          incRef(windowedColumnOutput)
        }
      case (Some(prev), scala.util.Left(mask)) =>
        if (prev.isValid) {
          val nullsReplaced = withResource(windowedColumnOutput.isNull) { nulls =>
            withResource(nulls.and(mask)) { shouldReplace =>
              withResource(makeZeroScalar(windowedColumnOutput.getType)) { zero =>
                shouldReplace.ifElse(zero, windowedColumnOutput)
              }
            }
          }
          withResource(nullsReplaced) { nullsReplaced =>
            if (needsBasicOverflowCheck) {
              withResource(nullsReplaced.binaryOp(BinaryOp.ADD, prev, prev.getType)) { updated =>
                closeOnExcept(mask.ifElse(updated, windowedColumnOutput)) { ret =>
                  AddOverflowChecks.basicOpOverflowCheck(updated, prev, ret, mask = Some(mask))
                  ret
                }
              }
            } else {
              withResource(nullsReplaced.binaryOp(BinaryOp.ADD, prev, prev.getType)) { updated =>
                mask.ifElse(updated, windowedColumnOutput)
              }
            }
          }
        } else {
          // prev is NULL but NULL + something == NULL which we don't want
          incRef(windowedColumnOutput)
        }
    }
    closeOnExcept(ret) { ret =>
      updateState(ret, None)
      ret
    }
  }

  private[this] def fixUpDecimal(samePartitionMask: Either[cudf.ColumnVector, Boolean],
      windowedColumnOutput: cudf.ColumnView,
      dt: DecimalType): cudf.ColumnVector = {
    logDebug(s"$name: fix up $previousResult $samePartitionMask")
    val (ret, decimalOverflowOnAdd) = (previousResult, previousOverflow, samePartitionMask) match {
      case (None, None, _) =>
        // The mask is all false so do nothing
        withResource(Scalar.fromBool(false)) { falseVal =>
          closeOnExcept(ColumnVector.fromScalar(falseVal,
            windowedColumnOutput.getRowCount.toInt)) { over =>
            (incRef(windowedColumnOutput), over)
          }
        }
      case (Some(prev), Some(previousOver), scala.util.Right(mask)) =>
        if (mask) {
          if (!prev.isValid) {
            // So in the window operation we can have a null if all of the input values before it
            // were also null or if we overflowed the result and inserted in a null.
            //
            // If we overflowed, then all of the output for this group should be null, but the
            // overflow check code can handle inserting that, so just inc the ref count and return
            // the overflow column.
            //
            // If we didn't overflow, and the input is null then
            // prev is NULL but NULL + something == NULL which we don't want, so also
            // just increment the reference count and go on.
            closeOnExcept(ColumnVector.fromScalar(previousOver,
              windowedColumnOutput.getRowCount.toInt)) { over =>
              (incRef(windowedColumnOutput), over)
            }
          } else {
            // The previous didn't overflow, so now we need to do the add and check for overflow.
            val nullsReplaced = withResource(windowedColumnOutput.isNull) { nulls =>
              withResource(makeZeroScalar(windowedColumnOutput.getType)) { zero =>
                nulls.ifElse(zero, windowedColumnOutput)
              }
            }
            withResource(nullsReplaced) { nullsReplaced =>
              closeOnExcept(nullsReplaced.binaryOp(BinaryOp.ADD, prev, prev.getType)) { added =>
                (added, AddOverflowChecks.didDecimalOverflow(nullsReplaced, prev, added))
              }
            }
          }
        } else {
          // The mask is all false so do nothing
          withResource(Scalar.fromBool(false)) { falseVal =>
            closeOnExcept(ColumnVector.fromScalar(falseVal,
              windowedColumnOutput.getRowCount.toInt)) { over =>
              (incRef(windowedColumnOutput), over)
            }
          }
        }
      case (Some(prev), Some(previousOver), scala.util.Left(mask)) =>
        if (prev.isValid) {
          // The previous didn't overflow, so now we need to do the add and check for overflow.
          val nullsReplaced = withResource(windowedColumnOutput.isNull) { nulls =>
            withResource(nulls.and(mask)) { shouldReplace =>
              withResource(makeZeroScalar(windowedColumnOutput.getType)) { zero =>
                shouldReplace.ifElse(zero, windowedColumnOutput)
              }
            }
          }
          withResource(nullsReplaced) { nullsReplaced =>
            withResource(nullsReplaced.binaryOp(BinaryOp.ADD, prev, prev.getType)) { added =>
              closeOnExcept(mask.ifElse(added, windowedColumnOutput)) { updated =>
                withResource(Scalar.fromBool(false)) { falseVal =>
                  withResource(AddOverflowChecks
                      .didDecimalOverflow(nullsReplaced, prev, added)) { over =>
                    (updated, mask.ifElse(over, falseVal))
                  }
                }
              }
            }
          }
        } else {
          // So in the window operation we can have a null if all of the input values before it
          // were also null or if we overflowed the result and inserted in a null.
          //
          // If we overflowed, then all of the output for this group should be null, but the
          // overflow check code can handle inserting that, so just inc the ref count and return
          // the overflow column.
          //
          // If we didn't overflow, and the input is null then
          // prev is NULL but NULL + something == NULL which we don't want, so also
          // just increment the reference count and go on.
          closeOnExcept(ColumnVector.fromScalar(previousOver,
            windowedColumnOutput.getRowCount.toInt)) { over =>
            (incRef(windowedColumnOutput), over)
          }
        }
      case _ =>
        throw new IllegalStateException("INTERNAL ERROR: Should never have a situation where " +
            "prev and previousOver do not match.")
    }
    withResource(ret) { _ =>
      val outOfBounds = withResource(decimalOverflowOnAdd) { _ =>
        withResource(DecimalUtil.outOfBounds(ret, dt)) {
          _.or(decimalOverflowOnAdd)
        }
      }
      withResource(outOfBounds) { _ =>
        closeOnExcept(GpuCast.fixDecimalBounds(ret, outOfBounds, dt, isAnsi)) { replaced =>
          updateState(replaced, Some(outOfBounds))
          replaced
        }
      }
    }
  }

  override def fixUp(samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector = {
    toType match {
      case dt: DecimalType =>
        fixUpDecimal(samePartitionMask, windowedColumnOutput, dt)
      case _ =>
        fixUpNonDecimal(samePartitionMask, windowedColumnOutput)
    }
  }

  override def close(): Unit = {
    previousResult.foreach(_.close())
    previousResult = None
    previousOverflow.foreach(_.close())
    previousOverflow = None
  }
}

/**
 * Rank is more complicated than DenseRank to fix. This is because there are gaps in the
 * rank values. The rank value of each group is row number of the first row in the group.
 * So values in the same partition group but not the same ordering are fixed by adding
 * the row number from the previous batch to them. If they are a part of the same ordering and
 * part of the same partition, then we need to just put in the previous rank value.
 *
 * Because we need both a rank and a row number to fix things up the input to this is a struct
 * containing a rank column as the first entry and a row number column as the second entry. This
 * happens in the `scanCombine` method for GpuRank.  It is a little ugly but it works to maintain
 * the requirement that the input to the fixer is a single column.
 */
class RankFixer extends BatchedRunningWindowFixer with Logging {
  import RankFixer._

  // We have to look at row number as well as rank.  This fixer is the same one that `GpuRowNumber`
  // uses.
  private[this] val rowNumFixer = new BatchedRunningWindowBinaryFixer(BinaryOp.ADD, "row_number")
  // convenience method to get access to the previous row number.
  private[this] def previousRow: Option[Scalar] = rowNumFixer.getPreviousResult
  // The previous rank value
  private[this] var previousRank: Option[Scalar] = None

  // checkpoint
  private[this] var checkpointRank: Option[Scalar] = None

  override def checkpoint(): Unit = {
    rowNumFixer.checkpoint()
    checkpointRank = previousRank
  }

  override def restore(): Unit = {
    rowNumFixer.restore()
    if (checkpointRank.isDefined) {
      // close previous result
      previousRank match {
        case Some(r) if r != checkpointRank.get =>
          r.close()
        case _ =>
      }
      previousRank = checkpointRank
      checkpointRank = None
    }
  }

  override def needsOrderMask: Boolean = true

  override def fixUp(
      samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector = {
    assert(windowedColumnOutput.getType == DType.STRUCT)
    assert(windowedColumnOutput.getNumChildren == 2)
    val initialRank = windowedColumnOutput.getChildColumnView(0)
    val initialRowNum = windowedColumnOutput.getChildColumnView(1)
    val ret = (previousRank, samePartitionMask) match {
      case (None, _) => incRef(initialRank)
      case (Some(prevRank), scala.util.Right(partMask)) =>
        if (partMask) {
          // We are in the same partition as the last part of the batch so we have to look at the
          // ordering to know what to do.
          sameOrderMask.get match {
            case scala.util.Left(orderMask) =>
              fixRankSamePartition(initialRank, orderMask, prevRank, previousRow)
            case scala.util.Right(orderMask) =>
              // Technically I think this code is unreachable because the only time a constant
              // true or false is returned is if the order by column is empty or if the parts mask
              // is false. Spark requires there to be order by columns and we already know that
              // the partition mask is true. But it is small so just to be on the safe side.
              if (orderMask) {
                // it is all for the same partition and order so it is the same value as the
                // previous rank
                cudf.ColumnVector.fromScalar(prevRank, initialRank.getRowCount.toInt)
              } else {
                fixRankSamePartDifferentOrdering(initialRank, previousRow)
              }
          }
        } else {
          incRef(initialRank)
        }
      case (Some(prevRank), scala.util.Left(partMask)) =>
        sameOrderMask.get match {
          case scala.util.Left(orderMask) =>
            // Fix up the data for the same partition and keep the rest unchanged.
            val samePart = fixRankSamePartition(initialRank, orderMask, prevRank, previousRow)
            withResource(samePart) { samePart =>
              partMask.ifElse(samePart, initialRank)
            }
          case scala.util.Right(_) =>
            // The framework guarantees that the order by mask is a subset of the group by mask
            // So if the group by mask in not a constant, then the order by mask also cannot be
            // a constant
            throw new IllegalStateException(
              "Internal Error the order mask is not a subset of the part mask")
        }
    }

    // We just want to update the state for row num
    rowNumFixer.fixUp(samePartitionMask, sameOrderMask, initialRowNum).close()
    logDebug(s"rank: updateState from $previousRank to...")
    previousRank.foreach(_.close)
    previousRank = Some(ret.getScalarElement(ret.getRowCount.toInt - 1))
    logDebug(s"rank/row: ... $previousRank $previousRow")
    ret
  }

  override def close(): Unit = {
    previousRank.foreach(_.safeClose())
    previousRank = None
    rowNumFixer.close()
  }
}

object RankFixer {
  private def fixRankSamePartDifferentOrdering(rank: cudf.ColumnView,
      previousRow: Option[Scalar]): cudf.ColumnVector =
    rank.add(previousRow.get, rank.getType)

  private def fixRankSamePartition(rank: cudf.ColumnView,
      orderMask: cudf.ColumnView,
      prevRank: Scalar,
      previousRow: Option[Scalar]): cudf.ColumnVector = {
    withResource(fixRankSamePartDifferentOrdering(rank, previousRow)) { partlyFixed =>
      orderMask.ifElse(prevRank, partlyFixed)
    }
  }
}

/**
 * Fix up dense rank batches. A dense rank has no gaps in the rank values.
 * The rank corresponds to the ordering columns(s) equality. So when a batch
 * finishes and another starts that split can either be at the beginning of a
 * new order by section or part way through one. If it is at the beginning, then
 * like row number we want to just add in the previous value and go on. If
 * it was part way through, then we want to add in the previous value minus 1.
 * The minus one is to pick up where we left off.
 * If anything is outside of a continues partition by group then we just keep
 * those values unchanged.
 */
class DenseRankFixer extends BatchedRunningWindowFixer with Logging {
  import DenseRankFixer._

  private var previousRank: Option[Scalar] = None

  // checkpoint
  private var checkpointRank: Option[Scalar] = None

  override def checkpoint(): Unit = {
    checkpointRank = previousRank
  }

  override def restore(): Unit = {
    if (checkpointRank.isDefined) {
      // close previous result
      previousRank match {
        case Some(r) if r != checkpointRank.get =>
          r.close()
        case _ =>
      }
      previousRank = checkpointRank
      checkpointRank = None
    }
  }

  override def needsOrderMask: Boolean = true

  override def fixUp(
      samePartitionMask: Either[cudf.ColumnVector, Boolean],
      sameOrderMask: Option[Either[cudf.ColumnVector, Boolean]],
      windowedColumnOutput: cudf.ColumnView): cudf.ColumnVector = {
    val ret = (previousRank, samePartitionMask) match {
      case (None, _) => incRef(windowedColumnOutput)
      case (Some(prevRank), scala.util.Right(partMask)) =>
        if (partMask) {
          // We are in the same partition as the last part of the batch so we have to look at the
          // ordering to know what to do.
          sameOrderMask.get match {
            case scala.util.Left(orderMask) =>
              // It is all in the same partition so just fix it for that.
              fixRankInSamePartition(windowedColumnOutput, orderMask, prevRank)
            case scala.util.Right(orderMask) =>
              // Technically I think this code is unreachable because the only time a constant
              // true or false is returned is if the order by column is empty or if the parts mask
              // is false. Spark requires there to be order by columns and we already know that
              // the partition mask is true. But it is small so just to be on the safe side.
              if (orderMask) {
                // Everything in this batch is part of the same ordering group too.
                // We don't add previous rank - 1, because the current value for everything is 1
                // so rank - 1 + 1 == rank.
                cudf.ColumnVector.fromScalar(prevRank, windowedColumnOutput.getRowCount.toInt)
              } else {
                // Same partition but hit an order by boundary.
                addPrevRank(windowedColumnOutput, prevRank)
              }
          }
        } else {
          // Different partition by group so this is a NOOP
          incRef(windowedColumnOutput)
        }
      case (Some(prevRank), scala.util.Left(partMask)) =>
        sameOrderMask.get match {
          case scala.util.Left(orderMask) =>
            // Fix up the data for the same partition and keep the rest unchanged.
            val samePart = fixRankInSamePartition(windowedColumnOutput, orderMask, prevRank)
            withResource(samePart) { samePart =>
              partMask.ifElse(samePart, windowedColumnOutput)
            }
          case scala.util.Right(_) =>
            // The framework guarantees that the order by mask is a subset of the group by mask
            // So if the group by mask in not a constant, then the order by mask also cannot be
            // a constant
            throw new IllegalStateException(
              "Internal Error the order mask is not a subset of the part mask")
        }
    }

    logDebug(s"dense rank: updateState from $previousRank to...")
    previousRank.foreach(_.close)
    previousRank = Some(ret.getScalarElement(ret.getRowCount.toInt - 1))
    logDebug(s"dense rank: ... $previousRank")

    ret
  }

  override def close(): Unit = {
    previousRank.foreach(_.close())
    previousRank = None
  }
}

object DenseRankFixer {
  private[this] def isFirstTrue(cv: cudf.ColumnView): Boolean = {
    withResource(cv.getScalarElement(0)) { scalar =>
      scalar.getBoolean
    }
  }

  private def addPrevRank(cv: cudf.ColumnView, prevRank: Scalar): cudf.ColumnVector =
    cv.add(prevRank)

  private[this] def addPrevRankMinusOne(cv: cudf.ColumnView,
      prevRank: Scalar): cudf.ColumnVector = {
    withResource(addPrevRank(cv, prevRank)) { prev =>
      withResource(Scalar.fromInt(1)) { one =>
        prev.sub(one)
      }
    }
  }

  private def fixRankInSamePartition(
      rank: cudf.ColumnView,
      orderMask: cudf.ColumnView,
      prevRank: Scalar): cudf.ColumnVector = {
    // This is a little ugly, but the only way to tell if we are part of the previous order by
    // group or not is to look at the orderMask. In this case we check if the first value in the
    // mask is true.
    val added = if (isFirstTrue(orderMask)) {
      addPrevRankMinusOne(rank, prevRank)
    } else {
      addPrevRank(rank, prevRank)
    }
    withResource(added) { added =>
      orderMask.ifElse(prevRank, added)
    }
  }
}

/**
 * Rank is a special window operation where it is only supported as a running window. In cudf
 * it is only supported as a scan and a group by scan. But there are special requirements beyond
 * that when doing the computation as a running batch. To fix up each batch it needs both the rank
 * and the row number. To make this work and be efficient there is different behavior for batched
 * running window vs non-batched. If it is for a running batch we include the row number values,
 * in both the initial projections and in the corresponding aggregations. Then we combine them
 * into a struct column in `scanCombine` before it is passed on to the `RankFixer`. If it is not
 * a running batch, then we drop the row number part because it is just not needed.
 * @param children the order by columns.
 * @note this is a running window only operator.
 */
case class GpuRank(children: Seq[Expression]) extends GpuRunningWindowFunction
    with GpuBatchedRunningWindowWithFixer with ShimExpression {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] = {
    // The requirement is that each input projection has a corresponding aggregation
    // associated with it. This also fits with how rank works in cudf, where the input is also
    // a single column. If there are multiple order by columns we wrap them in a struct.
    // This is not ideal from a memory standpoint, and in the future we might be able to fix this
    // with a ColumnView, but for now with how the Java cudf APIs work it would be hard to work
    // around.
    val orderedBy = if (children.length == 1) {
      children.head
    } else {
      val childrenWithNames = children.zipWithIndex.flatMap {
        case (expr, idx) => Seq(GpuLiteral(idx.toString, StringType), expr)
      }
      GpuCreateNamedStruct(childrenWithNames)
    }

    // If the computation is for a batched running window then we need a row number too
    if (isRunningBatched) {
      Seq(orderedBy, GpuLiteral(1, IntegerType))
    } else {
      Seq(orderedBy)
    }
  }

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] = {
    if (isRunningBatched) {
      // We are computing both rank and row number so we can fix it up at the end
      Seq(AggAndReplace(GroupByScanAggregation.rank(), None),
        AggAndReplace(GroupByScanAggregation.sum(), None))
    } else {
      // Not batched just do the rank
      Seq(AggAndReplace(GroupByScanAggregation.rank(), None))
    }
  }

  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    groupByScanInputProjection(isRunningBatched)
  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] = {
    if (isRunningBatched) {
      // We are computing both rank and row number so we can fix it up at the end
      Seq(AggAndReplace(ScanAggregation.rank(), None), AggAndReplace(ScanAggregation.sum(), None))
    } else {
      // Not batched just do the rank
      Seq(AggAndReplace(ScanAggregation.rank(), None))
    }
  }

  override def scanCombine(isRunningBatched: Boolean, cols: Seq[ColumnVector]): ColumnVector = {
    if (isRunningBatched) {
      // When the data is batched we are using the fixer, and it needs rank and row number
      // to calculate the final value
      assert(cols.length == 2)
      ColumnVector.makeStruct(cols: _*)
    } else {
      assert(cols.length == 1)
      cols.head.incRefCount()
    }
  }

  override def newFixer(): BatchedRunningWindowFixer = new RankFixer()
}

/**
 * Dense Rank is a special window operation where it is only supported as a running window. In cudf
 * it is only supported as a scan and a group by scan.
 * @param children the order by columns.
 * @note this is a running window only operator
 */
case class GpuDenseRank(children: Seq[Expression]) extends GpuRunningWindowFunction
    with GpuBatchedRunningWindowWithFixer {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] = {
    // The requirement is that each input projection has a corresponding aggregation
    // associated with it. This also fits with how rank works in cudf, where the input is also
    // a single column. If there are multiple order by columns we wrap them in a struct.
    // This is not ideal from a memory standpoint, and in the future we might be able to fix this
    // with a ColumnView, but for now with how the Java cudf APIs work it would be hard to work
    // around.
    if (children.length == 1) {
      Seq(children.head)
    } else {
      val childrenWithNames = children.zipWithIndex.flatMap {
        case (expr, idx) => Seq(GpuLiteral(idx.toString, StringType), expr)
      }
      Seq(GpuCreateNamedStruct(childrenWithNames))
    }
  }

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] =
    Seq(AggAndReplace(GroupByScanAggregation.denseRank(), None))

  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    groupByScanInputProjection(isRunningBatched)

  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] =
    Seq(AggAndReplace(ScanAggregation.denseRank(), None))

  override def newFixer(): BatchedRunningWindowFixer = new DenseRankFixer()
}

/**
 * The row number in the window.
 * @note this is a running window only operator
 */
case object GpuRowNumber extends GpuRunningWindowFunction
    with GpuBatchedRunningWindowWithFixer {
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = Nil

  override def newFixer(): BatchedRunningWindowFixer =
    new BatchedRunningWindowBinaryFixer(BinaryOp.ADD, "row_number")

  // For group by scans cudf does not support ROW_NUMBER so we will do a SUM
  // on a column of 1s. We could do a COUNT_ALL too, but it would not be as consistent
  // with the non group by scan
  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    Seq(GpuLiteral(1, IntegerType))

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] =
    Seq(AggAndReplace(GroupByScanAggregation.sum(), None))

  // For regular scans cudf does not support ROW_NUMBER, nor does it support COUNT_ALL
  // so we will do a SUM on a column of 1s
  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    groupByScanInputProjection(isRunningBatched)
  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] =
    Seq(AggAndReplace(ScanAggregation.sum(), None))

  override def scanCombine(isRunningBatched: Boolean, cols: Seq[ColumnVector]): ColumnVector = {
    cols.head.castTo(DType.INT32)
  }
}

trait GpuOffsetWindowFunction extends GpuAggregateWindowFunction {
  protected val input: Expression
  protected val offset: Expression
  protected val default: Expression

  protected val parsedOffset: Int = offset match {
    case GpuLiteral(o: Int, IntegerType) => o
    case other =>
      throw new IllegalStateException(s"$other is not a supported offset type")
  }
  override def nullable: Boolean = default == null || default.nullable || input.nullable
  override def dataType: DataType = input.dataType

  override def children: Seq[Expression] = Seq(input, offset, default)

  override val windowInputProjection: Seq[Expression] = default match {
    case GpuLiteral(v, _) if v == null => Seq(input)
    case _ => Seq(input, default)
  }
}

case class GpuLead(input: Expression, offset: Expression, default: Expression)
    extends GpuOffsetWindowFunction {

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn = {
    val in = inputs.toArray
    if (in.length > 1) {
      // Has a default
      RollingAggregation.lead(parsedOffset, in(1)._1).onColumn(in.head._2)
    } else {
      RollingAggregation.lead(parsedOffset).onColumn(in.head._2)
    }
  }
}

case class GpuLag(input: Expression, offset: Expression, default: Expression)
    extends GpuOffsetWindowFunction {

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn = {
    val in = inputs.toArray
    if (in.length > 1) {
      // Has a default
      RollingAggregation.lag(parsedOffset, in(1)._1).onColumn(in.head._2)
    } else {
      RollingAggregation.lag(parsedOffset).onColumn(in.head._2)
    }
  }
}

/**
 * percent_rank() is a running window function in that it only operates on a window of unbounded
 * preceding to current row. But the percent part actually makes it need a full count of the number
 * of rows in the window. This is why we rewrite the operator to allow us to compute the result
 * in a way that will not overflow memory.
 */
case class GpuPercentRank(children: Seq[Expression]) extends GpuReplaceWindowFunction {
  override def nullable: Boolean = false
  override def dataType: DataType = DoubleType

  override def windowReplacement(spec: GpuWindowSpecDefinition): Expression = {
    // Spark writes this as
    // If(n > one, (rank - one).cast(DoubleType) / (n - one).cast(DoubleType), 0.0d)
    // where n is the count of all values in the window and rank is the rank.
    //
    // The databricks docs describe it as
    // nvl(
    //     (rank() over (PARTITION BY p ORDER BY o) - 1) /
    //     (nullif(count(1) over(PARTITION BY p) - 1, 0)),
    //     0.0)
    //
    // We do it slightly differently to try and optimize things for the GPU.
    // We ignore ANSI mode because the count agg will take care of overflows already
    // and n - 1 cannot overflow. It also cannot be negative because it is COUNT(1) and 1
    // cannot be null.
    // A divide by 0 in non-ANSI mode produces a null, which we can use to avoid extra data copies.
    // The If/Else from the original Spark expression on the GPU needs to split the input data to
    // avoid the ANSI divide throwing an error on the divide by 0 that it is trying to avoid. We
    // skip that and just take the null as output, which we can replace with 0.0 afterwards.
    // That is the only case when we would get a null as output.
    // From this we essentially do
    // coalesce(CAST(rank - 1 AS DOUBLE) / CAST(n - 1 AS DOUBLE), 0.0)
    val isAnsi = false
    val fullUnboundedFrame = GpuSpecifiedWindowFrame(RowFrame,
      GpuSpecialFrameBoundary(UnboundedPreceding),
      GpuSpecialFrameBoundary(UnboundedFollowing))
    val fullUnboundedSpec = GpuWindowSpecDefinition(spec.partitionSpec, spec.orderSpec,
      fullUnboundedFrame)
    val count = GpuWindowExpression(GpuCount(Seq(GpuLiteral(1))), fullUnboundedSpec)
    val rank = GpuWindowExpression(GpuRank(children), spec)
    val rankMinusOne = GpuCast(GpuSubtract(rank, GpuLiteral(1), isAnsi)(), DoubleType, isAnsi)()
    val countMinusOne = GpuCast(GpuSubtract(count, GpuLiteral(1L), isAnsi)(), DoubleType, isAnsi)()
    val divided = GpuDivide(rankMinusOne, countMinusOne, isAnsi)()
    GpuCoalesce(Seq(divided, GpuLiteral(0.0)))
  }
}