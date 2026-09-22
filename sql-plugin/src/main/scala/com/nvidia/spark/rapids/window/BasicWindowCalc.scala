/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf.{AggregationOverWindow, DType, GroupByOptions, GroupByScanAggregation, NullPolicy, ReplacePolicy, ReplacePolicyWithColumn, Scalar, ScanAggregation, ScanType, Table, WindowOptions}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.shims.GpuWindowUtil

import org.apache.spark.sql.catalyst.expressions.{CurrentRow, Expression, FrameType, NullsFirst,
  RangeFrame, RowFrame, SortOrder}
import org.apache.spark.sql.types.{ByteType, CalendarIntervalType, DataType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.CalendarInterval


/**
 * For Scan and GroupBy Scan aggregations nulls are not always treated the same way as they are
 * in window operations. Often we have to run a post processing step and replace them. This
 * groups those two together so we can have a complete picture of how to perform these types of
 * aggregations.
 */
case class AggAndReplace[T](agg: T, nullReplacePolicy: Option[ReplacePolicy])

/**
 * The class represents a window function and the locations of its deduped inputs after an initial
 * projection.
 */
case class BoundGpuWindowFunction(
    windowFunc: GpuWindowFunction,
    boundInputLocations: Array[Int]) {

  /**
   * Get the operations to perform a scan aggregation.
   * @param isRunningBatched is this for a batched running window operation?
   * @return the sequence of aggregation operators to do. There will be one `AggAndReplace`
   *         for each value in `boundInputLocations` so that they can be zipped together.
   */
  def scan(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] = {
    val aggFunc = windowFunc.asInstanceOf[GpuRunningWindowFunction]
    aggFunc.scanAggregation(isRunningBatched)
  }

  /**
   * Get the operations to perform a group by scan aggregation.
   * @param isRunningBatched is this for a batched running window operation?
   * @return the sequence of aggregation operators to do. There will be one `AggAndReplace`
   *         for each value in `boundInputLocations` so that they can be zipped together.
   */
  def groupByScan(isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] = {
    val aggFunc = windowFunc.asInstanceOf[GpuRunningWindowFunction]
    aggFunc.groupByScanAggregation(isRunningBatched)
  }

  /**
   * After a scan or group by scan if there are multiple columns they need to be combined together
   * into a single final output column. This does that job.
   * @param isRunningBatched is this for a batched running window operation?
   * @param cols the columns to be combined. This should not close them.
   * @return a single result column.
   */
  def scanCombine(isRunningBatched: Boolean,
      cols: Seq[cudf.ColumnVector]): cudf.ColumnVector = {
    val aggFunc = windowFunc.asInstanceOf[GpuRunningWindowFunction]
    aggFunc.scanCombine(isRunningBatched, cols)
  }

  def aggOverWindow(cb: ColumnarBatch,
      windowOpts: WindowOptions): AggregationOverWindow = {
    val aggFunc = windowFunc.asInstanceOf[GpuAggregateWindowFunction]
    val inputs = boundInputLocations.map { pos =>
      (cb.column(pos).asInstanceOf[GpuColumnVector].getBase, pos)
    }
    aggFunc.windowAggregation(inputs).overWindow(windowOpts)
  }

  def windowOutput(cv: cudf.ColumnVector): cudf.ColumnVector = {
    val aggFunc = windowFunc.asInstanceOf[GpuAggregateWindowFunction]
    aggFunc.windowOutput(cv)
  }

  val dataType: DataType = windowFunc.dataType
}

/**
 * Abstraction for possible range-boundary specifications.
 *
 * This provides type disjunction for Long, BigInt and Double,
 * the three types that might represent a range boundary.
 */
abstract class RangeBoundaryValue {
  def long: Long = RangeBoundaryValue.long(this)
  def bigInt: BigInt = RangeBoundaryValue.bigInt(this)
  def double: Double = RangeBoundaryValue.double(this)
}

case class LongRangeBoundaryValue(value: Long) extends RangeBoundaryValue
case class BigIntRangeBoundaryValue(value: BigInt) extends RangeBoundaryValue
case class DoubleRangeBoundaryValue(value: Double) extends RangeBoundaryValue

object RangeBoundaryValue {

  def long(boundary: RangeBoundaryValue): Long = boundary match {
    case LongRangeBoundaryValue(l) => l
    case other => throw new NoSuchElementException(s"Cannot get `long` from $other")
  }

  def bigInt(boundary: RangeBoundaryValue): BigInt = boundary match {
    case BigIntRangeBoundaryValue(b) => b
    case other => throw new NoSuchElementException(s"Cannot get `bigInt` from $other")
  }

  def double(boundary: RangeBoundaryValue): Double = boundary match {
    case DoubleRangeBoundaryValue(d) => d
    case other => throw new NoSuchElementException(s"Cannot get `double` from $other")
  }

  def long(value: Long): LongRangeBoundaryValue = LongRangeBoundaryValue(value)

  def bigInt(value: BigInt): BigIntRangeBoundaryValue = BigIntRangeBoundaryValue(value)

  def double(value: Double): DoubleRangeBoundaryValue = DoubleRangeBoundaryValue(value)
}

case class ParsedBoundary(isUnbounded: Boolean, value: RangeBoundaryValue)

object GroupedAggregations {
  /**
   * Get the window options for an aggregation
   * @param orderSpec the order by spec
   * @param orderPositions the positions of the order by columns
   * @param frame the frame to translate
   * @return the options to use when doing the aggregation.
   */
  private def getWindowOptions(
      orderSpec: Seq[SortOrder],
      orderPositions: Seq[Int],
      frame: GpuSpecifiedWindowFrame,
      minPeriods: Int): WindowOptions = {
    frame.frameType match {
      case RowFrame =>
        withResource(getRowBasedLower(frame)) { lower =>
          withResource(getRowBasedUpper(frame)) { upper =>
            val builder = WindowOptions.builder().minPeriods(minPeriods)
            if (isUnbounded(frame.lower)) builder.unboundedPreceding() else builder.preceding(lower)
            if (isUnbounded(frame.upper)) builder.unboundedFollowing() else builder.following(upper)
            builder.build
          }
        }
      case RangeFrame =>
        // This gets to be a little more complicated

        require(orderSpec.nonEmpty)
        require(orderPositions.length == orderSpec.length)

        if (orderSpec.length == 1) {
          val orderExpr = orderSpec.head

          // We only support basic types for now too
          val orderType = GpuColumnVector.getNonNestedRapidsType(orderExpr.dataType)

          val orderByIndex = orderPositions.head
          val lower = getRangeBoundaryValue(frame.lower, orderType)
          val upper = getRangeBoundaryValue(frame.upper, orderType)

          withResource(asScalarRangeBoundary(orderType, lower)) { preceding =>
            withResource(asScalarRangeBoundary(orderType, upper)) { following =>
              val windowOptionBuilder = WindowOptions.builder()
                  .minPeriods(1) // Does not currently support custom minPeriods.
                  .orderByColumnIndex(orderByIndex)

              if (preceding.isEmpty) {
                windowOptionBuilder.unboundedPreceding()
              } else {
                // Bounded STRING bounds can only mean "CURRENT ROW".
                if (orderType == DType.STRING) {
                  windowOptionBuilder.currentRowPreceding()
                } else {
                  windowOptionBuilder.preceding(preceding.get)
                }
              }

              if (following.isEmpty) {
                windowOptionBuilder.unboundedFollowing()
              } else {
                // Bounded STRING bounds can only mean "CURRENT ROW".
                if (orderType == DType.STRING) {
                  windowOptionBuilder.currentRowFollowing()
                } else {
                  windowOptionBuilder.following(following.get)
                }
              }

              if (orderExpr.isAscending) {
                windowOptionBuilder.orderByAscending()
              } else {
                windowOptionBuilder.orderByDescending()
              }

              windowOptionBuilder.build()
            }
          }
        } else {
          val ascending = orderSpec.map(_.isAscending).toArray
          val nullsFirst = orderSpec.map(_.nullOrdering == NullsFirst).toArray
          val windowOptionBuilder = WindowOptions.builder()
              .minPeriods(1) // Does not currently support custom minPeriods.
              .orderByColumns(
                orderPositions.toArray,
                ascending,
                nullsFirst)

          if (isUnbounded(frame.lower)) {
            windowOptionBuilder.unboundedPreceding()
          } else if (isCurrentRow(frame.lower)) {
            windowOptionBuilder.currentRowPreceding()
          } else {
            throw new UnsupportedOperationException(
              s"Unsupported multi-column RANGE lower-bound ${frame.lower}")
          }

          if (isUnbounded(frame.upper)) {
            windowOptionBuilder.unboundedFollowing()
          } else if (isCurrentRow(frame.upper)) {
            windowOptionBuilder.currentRowFollowing()
          } else {
            throw new UnsupportedOperationException(
              s"Unsupported multi-column RANGE upper-bound ${frame.upper}")
          }

          windowOptionBuilder.build()
        }
    }
  }

  private def isCurrentRow(boundary: Expression): Boolean = boundary match {
    case special: GpuSpecialFrameBoundary => special.boundary == CurrentRow
    case _ => false
  }

  private def isUnbounded(boundary: Expression): Boolean = boundary match {
    case special: GpuSpecialFrameBoundary => special.isUnbounded
    case _ => false
  }

  private def getRowBasedLower(windowFrameSpec : GpuSpecifiedWindowFrame): Scalar = {
    val lower = getRowBoundaryValue(windowFrameSpec.lower)

    // Translate the lower bound value to CUDF semantics:
    // In spark 0 is the current row and lower bound is negative relative to that
    // In CUDF the preceding window starts at the current row with 1 and up from there the
    // further from the current row.
    val ret = if (lower >= Int.MaxValue) {
      Int.MinValue
    } else if (lower <= Int.MinValue) {
      Int.MaxValue
    } else {
      -(lower-1)
    }
    Scalar.fromInt(ret)
  }

  private def getRowBasedUpper(windowFrameSpec : GpuSpecifiedWindowFrame): Scalar =
    Scalar.fromInt(getRowBoundaryValue(windowFrameSpec.upper))

  private def getRowBoundaryValue(boundary : Expression) : Int = boundary match {
    case literal: GpuLiteral if literal.dataType.equals(IntegerType) =>
      literal.value.asInstanceOf[Int]
    case special: GpuSpecialFrameBoundary =>
      special.value
    case anythingElse =>
      throw new UnsupportedOperationException(s"Unsupported window frame expression $anythingElse")
  }

  /**
   * Create a Scalar from boundary value according to order by column type.
   *
   * Timestamp types will be converted into interval types.
   *
   * @param orderByType the type of order by column
   * @param bound boundary value
   * @return a Scalar holding boundary value or None if the boundary is unbounded.
   */
  private def asScalarRangeBoundary(orderByType: DType, bound: ParsedBoundary): Option[Scalar] = {
    if (bound.isUnbounded) {
      None
    } else {
      val s = orderByType match {
        case DType.INT8 => Scalar.fromByte(bound.value.long.toByte)
        case DType.INT16 => Scalar.fromShort(bound.value.long.toShort)
        case DType.INT32 => Scalar.fromInt(bound.value.long.toInt)
        case DType.INT64 => Scalar.fromLong(bound.value.long)
        case DType.FLOAT32 => Scalar.fromFloat(bound.value.double.toFloat)
        case DType.FLOAT64 => Scalar.fromDouble(bound.value.double)
        // Interval is not working for DateType
        case DType.TIMESTAMP_DAYS => Scalar.durationFromLong(DType.DURATION_DAYS, bound.value.long)
        case DType.TIMESTAMP_MICROSECONDS =>
          Scalar.durationFromLong(DType.DURATION_MICROSECONDS, bound.value.long)
        case x if x.getTypeId == DType.DTypeEnum.DECIMAL32 =>
          Scalar.fromDecimal(x.getScale, bound.value.long.toInt)
        case x if x.getTypeId == DType.DTypeEnum.DECIMAL64 =>
          Scalar.fromDecimal(x.getScale, bound.value.long)
        case x if x.getTypeId == DType.DTypeEnum.DECIMAL128 =>
          Scalar.fromDecimal(x.getScale, bound.value.bigInt.underlying())
        case x if x.getTypeId == DType.DTypeEnum.STRING =>
          // Not UNBOUNDED. The only other supported boundary for String is CURRENT ROW, i.e. 0.
          Scalar.fromString("")
        case _ => throw new RuntimeException(s"Not supported order by type, Found $orderByType")
      }
      Some(s)
    }
  }

  private def getRangeBoundaryValue(boundary: Expression, orderByType: DType): ParsedBoundary =
    boundary match {
      case special: GpuSpecialFrameBoundary =>
        ParsedBoundary(
          isUnbounded = special.isUnbounded,
          value = orderByType.getTypeId match {
            case DType.DTypeEnum.DECIMAL128 => RangeBoundaryValue.bigInt(special.value)
            case DType.DTypeEnum.FLOAT32 | DType.DTypeEnum.FLOAT64 =>
              RangeBoundaryValue.double(special.value)
            case _ => RangeBoundaryValue.long(special.value)
          }
        )
      case GpuLiteral(ci: CalendarInterval, CalendarIntervalType) =>
        // Get the total microseconds for TIMESTAMP_MICROSECONDS
        var x = TimeUnit.DAYS.toMicros(ci.days) + ci.microseconds
        if (x == Long.MinValue) x = Long.MaxValue
        ParsedBoundary(isUnbounded = false, RangeBoundaryValue.long(Math.abs(x)))
      case GpuLiteral(value, ByteType) =>
        var x = value.asInstanceOf[Byte]
        if (x == Byte.MinValue) x = Byte.MaxValue
        ParsedBoundary(isUnbounded = false, RangeBoundaryValue.long(Math.abs(x)))
      case GpuLiteral(value, ShortType) =>
        var x = value.asInstanceOf[Short]
        if (x == Short.MinValue) x = Short.MaxValue
        ParsedBoundary(isUnbounded = false, RangeBoundaryValue.long(Math.abs(x)))
      case GpuLiteral(value, IntegerType) =>
        var x = value.asInstanceOf[Int]
        if (x == Int.MinValue) x = Int.MaxValue
        ParsedBoundary(isUnbounded = false, RangeBoundaryValue.long(Math.abs(x)))
      case GpuLiteral(value, LongType) =>
        var x = value.asInstanceOf[Long]
        if (x == Long.MinValue) x = Long.MaxValue
        ParsedBoundary(isUnbounded = false, RangeBoundaryValue.long(Math.abs(x)))
      case GpuLiteral(value, FloatType) =>
        var x = value.asInstanceOf[Float]
        if (x == Float.MinValue) x = Float.MaxValue
        ParsedBoundary(isUnbounded = false, RangeBoundaryValue.double(Math.abs(x)))
      case GpuLiteral(value, DoubleType) =>
        var x = value.asInstanceOf[Double]
        if (x == Double.MinValue) x = Double.MaxValue
        ParsedBoundary(isUnbounded = false, RangeBoundaryValue.double(Math.abs(x)))
      case GpuLiteral(value: Decimal, DecimalType()) =>
        orderByType.getTypeId match {
          case DType.DTypeEnum.DECIMAL32 | DType.DTypeEnum.DECIMAL64 =>
            ParsedBoundary(isUnbounded = false,
              RangeBoundaryValue.long(Math.abs(value.toUnscaledLong)))
          case DType.DTypeEnum.DECIMAL128 =>
            ParsedBoundary(isUnbounded = false,
              RangeBoundaryValue.bigInt(value.toJavaBigDecimal.unscaledValue().abs))
          case anythingElse =>
            throw new UnsupportedOperationException(s"Unexpected Decimal type: $anythingElse")
        }
      case anything => GpuWindowUtil.getRangeBoundaryValue(anything)
    }
}

/**
 * Window aggregations that are grouped together. It holds the aggregation and the offsets of
 * its input columns, along with the output columns it should write the result to.
 */
class GroupedAggregations {
  import GroupedAggregations._

  // The window frame to a map of the window function to the output locations for the result
  private val data = mutable.HashMap[GpuSpecifiedWindowFrame,
      mutable.HashMap[BoundGpuWindowFunction, ArrayBuffer[Int]]]()

  // This is similar to data but specific to running windows. We don't divide it up by the
  // window frame because the frame is the same for all of them unbounded rows preceding to
  // the current row.
  private val runningWindowOptimizedData =
    mutable.HashMap[BoundGpuWindowFunction, ArrayBuffer[Int]]()

  /**
   * Add an aggregation.
   * @param win the window this aggregation is over.
   * @param inputLocs the locations of the input columns for this aggregation.
   * @param outputIndex the output index this will write to in the final output.
   */
  def addAggregation(win: GpuWindowExpression, inputLocs: Array[Int], outputIndex: Int): Unit = {
    val forSpec = if (win.isOptimizedRunningWindow) {
      runningWindowOptimizedData
    } else {
      data.getOrElseUpdate(win.normalizedFrameSpec, mutable.HashMap.empty)
    }

    forSpec.getOrElseUpdate(BoundGpuWindowFunction(win.wrappedWindowFunc, inputLocs),
      ArrayBuffer.empty) += outputIndex
  }

  private def doAggInternal(
      frameType: FrameType,
      boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector],
      aggIt: (Table.GroupByOperation, Seq[AggregationOverWindow]) => Table): Unit = {
    data.foreach {
      case (frameSpec, functions) =>
        if (frameSpec.frameType == frameType) {
          // For now I am going to assume that we don't need to combine calls across frame specs
          // because it would just not help that much
          val result = {
            val allWindowOpts = functions.map { f =>
              getWindowOptions(boundOrderSpec, orderByPositions, frameSpec,
                f._1.windowFunc.getMinPeriods)
            }
            withResource(allWindowOpts.toSeq) { allWindowOpts =>
              val allAggs = allWindowOpts.zip(functions).map { case (windowOpt, f) =>
                f._1.aggOverWindow(inputCb, windowOpt)
              }
              withResource(GpuColumnVector.from(inputCb)) { initProjTab =>
                aggIt(initProjTab.groupBy(partByPositions: _*), allAggs)
              }
            }
          }
          withResource(result) { result =>
            functions.zipWithIndex.foreach {
              case ((func, outputIndexes), resultIndex) =>
                val aggColumn = result.getColumn(resultIndex)

                outputIndexes.foreach { outIndex =>
                  require(outputColumns(outIndex) == null,
                    "Attempted to overwrite a window output column!!")
                  outputColumns(outIndex) = func.windowOutput(aggColumn)
                }
            }
          }
        }
    }
  }

  private def doRowAggs(boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    doAggInternal(
      RowFrame, boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns,
      (groupBy, aggs) => groupBy.aggregateWindows(aggs: _*))
  }

  private def doRangeAggs(boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    doAggInternal(
      RangeFrame, boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns,
      (groupBy, aggs) => groupBy.aggregateWindowsOverRanges(aggs: _*))
  }

  private final def doRunningWindowScan(
      isRunningBatched: Boolean,
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    runningWindowOptimizedData.foreach {
      case (func, outputIndexes) =>
        val aggAndReplaces = func.scan(isRunningBatched)
        // For now we need at least one column. For row number in the future we might be able
        // to change that, but I think this is fine.
        require(func.boundInputLocations.length == aggAndReplaces.length,
          s"Input locations for ${func.windowFunc} do not match aggregations " +
              s"${func.boundInputLocations.toSeq} vs $aggAndReplaces")
        val combined = withResource(
          new ArrayBuffer[cudf.ColumnVector](aggAndReplaces.length)) { replacedCols =>
          func.boundInputLocations.indices.foreach { aggIndex =>
            val inputColIndex = func.boundInputLocations(aggIndex)
            val inputCol = inputCb.column(inputColIndex).asInstanceOf[GpuColumnVector].getBase
            val anr = aggAndReplaces(aggIndex)
            val agg = anr.agg
            val replacePolicy = anr.nullReplacePolicy
            replacedCols +=
                withResource(inputCol.scan(agg, ScanType.INCLUSIVE, NullPolicy.EXCLUDE)) {
                  scanned =>
                    // For scans when nulls are excluded then each input row that has a null in it
                    // the output row also has a null in it. Typically this is not what we want,
                    // because for windows that only happens if the first values are nulls. So we
                    // will then call replace nulls as needed to fix that up. Typically the
                    // replacement policy is preceding.
                    replacePolicy.map(scanned.replaceNulls).getOrElse(scanned.incRefCount())
                }
          }
          func.scanCombine(isRunningBatched, replacedCols.toSeq)
        }

        withResource(combined) { combined =>
          outputIndexes.foreach { outIndex =>
            require(outputColumns(outIndex) == null,
              "Attempted to overwrite a window output column!!")
            outputColumns(outIndex) = combined.incRefCount()
          }
        }
    }
  }

  // Part by is always ascending with nulls first, which is the default for group by options too
  private[this] val sortedGroupingOpts = GroupByOptions.builder()
      .withKeysSorted(true)
      .build()

  /**
   * Do just the grouped scan portion of a grouped scan aggregation.
   * @param isRunningBatched is this optimized for a running batch?
   * @param partByPositions what are the positions of the part by columns.
   * @param inputCb the input data to process
   * @return a Table that is the result of the aggregations. The partition
   *         by columns will be first, followed by one column for each aggregation in the order of
   *         `runningWindowOptimizedData`.
   */
  private final def justGroupedScan(
      isRunningBatched: Boolean,
      partByPositions: Array[Int],
      inputCb: ColumnarBatch): Table = {
    val allAggsWithInputs = runningWindowOptimizedData.map { case (func, _) =>
      func.groupByScan(isRunningBatched).zip(func.boundInputLocations)
    }.toArray

    val allAggs = allAggsWithInputs.flatMap { aggsWithInputs =>
      aggsWithInputs.map { case (aggAndReplace, index) =>
        aggAndReplace.agg.onColumn(index)
      }
    }

    val unoptimizedResult = withResource(GpuColumnVector.from(inputCb)) { initProjTab =>
      initProjTab.groupBy(sortedGroupingOpts, partByPositions: _*).scan(allAggs: _*)
    }
    // Our scan is sorted, but to comply with the API requirements of a non-sorted scan
    // the group/partition by columns are copied out. This is more memory then we want,
    // so we will replace them in the result with the same columns from the input batch
    withResource(unoptimizedResult) { unoptimizedResult =>
      withResource(new Array[cudf.ColumnVector](unoptimizedResult.getNumberOfColumns)) { cols =>
        // First copy over the part by columns
        partByPositions.zipWithIndex.foreach { case (inPos, outPos) =>
          cols(outPos) = inputCb.column(inPos).asInstanceOf[GpuColumnVector].getBase.incRefCount()
        }

        // Now copy over the scan results
        (partByPositions.length until unoptimizedResult.getNumberOfColumns).foreach { pos =>
          cols(pos) = unoptimizedResult.getColumn(pos).incRefCount()
        }
        new Table(cols: _*)
      }
    }
  }

  private final def groupedReplace(
      isRunningBatched: Boolean,
      partByPositions: Array[Int],
      tabFromScan: Table): Table = {
    // This gets a little complicated, because scan does not typically treat nulls the
    // way window treats nulls. So in some cases we need to do another group by and replace
    // the nulls to make them match what we want. But this is not all of the time, so we
    // keep track of which aggregations need to have a replace called on them, and where
    // we need to copy the results back out to. This is a little hard, but to try and help keep
    // track of it all the output of scan has the group by columns first followed by the scan
    // result columns in the order of `runningWindowOptimizedData`, and the output of
    // replace has the group by columns first followed by the replaced columns. So scans that
    // don't need a replace don't show up in the output of the replace call.
    val allReplace = ArrayBuffer[ReplacePolicyWithColumn]()
    val copyFromScan = ArrayBuffer[Int]()
    // We will not drop the partition by columns
    copyFromScan.appendAll(partByPositions.indices)

    // Columns to copy from the output of replace in the format of (fromReplaceIndex, toOutputIndex)
    val copyFromReplace = ArrayBuffer[(Int, Int)]()
    // Index of a column after it went through replace
    var afterReplaceIndex = partByPositions.length
    // Index of a column before it went through replace (this should be the same as the scan input
    // and the final output)
    var beforeReplaceIndex = partByPositions.length
    runningWindowOptimizedData.foreach { case (func, _) =>
      func.groupByScan(isRunningBatched).foreach { aggAndReplace =>
        val replace = aggAndReplace.nullReplacePolicy
        if (replace.isDefined) {
          allReplace.append(replace.get.onColumn(beforeReplaceIndex))
          copyFromReplace.append((afterReplaceIndex, beforeReplaceIndex))
          afterReplaceIndex += 1
        } else {
          copyFromScan.append(beforeReplaceIndex)
        }
        beforeReplaceIndex += 1
      }
    }

    withResource(new Array[cudf.ColumnVector](tabFromScan.getNumberOfColumns)) { columns =>
      copyFromScan.foreach { index =>
        columns(index) = tabFromScan.getColumn(index).incRefCount()
      }
      if (allReplace.nonEmpty) {
        // Don't bother to do the replace if none of them want anything replaced
        withResource(tabFromScan
            .groupBy(sortedGroupingOpts, partByPositions.indices: _*)
            .replaceNulls(allReplace.toSeq: _*)) { replaced =>
          copyFromReplace.foreach { case (from, to) =>
            columns(to) = replaced.getColumn(from).incRefCount()
          }
        }
      }
      new Table(columns: _*)
    }
  }

  /**
   * Take the aggregation results and run `scanCombine` on them if needed before copying them to
   * the output location.
   */
  private final def combineAndOutput(isRunningBatched: Boolean,
      partByPositions: Array[Int],
      scannedAndReplaced: Table,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    var readIndex = partByPositions.length
    runningWindowOptimizedData.foreach { case (func, outputLocations) =>
      val numScans = func.boundInputLocations.length
      val columns =
        (readIndex until (readIndex + numScans)).map(scannedAndReplaced.getColumn).toArray
      withResource(func.scanCombine(isRunningBatched, columns)) { col =>
        outputLocations.foreach { outIndex =>
          require(outputColumns(outIndex) == null,
            "Attempted to overwrite a window output column!!")
          outputColumns(outIndex) = col.incRefCount()
        }
      }
      readIndex += numScans
    }
  }

  /**
   * Do any running window grouped scan aggregations.
   */
  private final def doRunningWindowGroupedScan(
      isRunningBatched: Boolean,
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    val replaced =
      withResource(justGroupedScan(isRunningBatched, partByPositions, inputCb)) { scanned =>
        groupedReplace(isRunningBatched, partByPositions, scanned)
      }
    withResource(replaced) { replaced =>
      combineAndOutput(isRunningBatched, partByPositions, replaced, outputColumns)
    }
  }

  /**
   * Do any running window optimized aggregations.
   */
  private def doRunningWindowOptimizedAggs(
      isRunningBatched: Boolean,
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    if (runningWindowOptimizedData.nonEmpty) {
      if (partByPositions.isEmpty) {
        // This is implemented in terms of a scan on a column
        doRunningWindowScan(isRunningBatched, inputCb, outputColumns)
      } else {
        doRunningWindowGroupedScan(isRunningBatched, partByPositions, inputCb, outputColumns)
      }
    }
  }

  /**
   * Do all of the aggregations and put them in the output columns. There may be extra processing
   * after this before you get to a final result.
   */
  def doAggs(isRunningBatched: Boolean,
      boundOrderSpec: Seq[SortOrder],
      orderByPositions: Array[Int],
      partByPositions: Array[Int],
      inputCb: ColumnarBatch,
      outputColumns: Array[cudf.ColumnVector]): Unit = {
    doRunningWindowOptimizedAggs(isRunningBatched, partByPositions, inputCb, outputColumns)
    doRowAggs(boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns)
    doRangeAggs(boundOrderSpec, orderByPositions, partByPositions, inputCb, outputColumns)
  }

  /**
   * Turn the final result of the aggregations into a ColumnarBatch.
   */
  def convertToColumnarBatch(dataTypes: Array[DataType],
      aggOutputColumns: Array[cudf.ColumnVector]): ColumnarBatch = {
    assert(dataTypes.length == aggOutputColumns.length)
    val numRows = aggOutputColumns.head.getRowCount.toInt
    closeOnExcept(new Array[ColumnVector](aggOutputColumns.length)) { finalOutputColumns =>
      dataTypes.indices.foreach { index =>
        val dt = dataTypes(index)
        val col = aggOutputColumns(index)
        finalOutputColumns(index) = GpuColumnVector.from(col, dt).incRefCount()
      }
      new ColumnarBatch(finalOutputColumns, numRows)
    }
  }
}

/**
 * Calculates the results of window operations. It assumes that any batching of the data
 * or fixups after the fact to get the right answer is done outside of this.
 */
trait BasicWindowCalc {
  val boundWindowOps: Seq[GpuExpression]
  val boundPartitionSpec: Seq[GpuExpression]
  val boundOrderSpec: Seq[SortOrder]

  /**
   * Is this going to do a batched running window optimization or not.
   */
  def isRunningBatched: Boolean

  // In order to dedupe aggregations we take a slightly different approach from
  // group by aggregations. Instead of using named expressions to line up different
  // parts of the aggregation (pre-processing, aggregation, post-processing) we
  // keep track of the offsets directly. This is quite a bit more complex, but lets us
  // see that 5 aggregations want a column of just 1 and we dedupe it so it is only
  // materialized once.
  // `initialProjections` are a list of projections that provide the inputs to the `aggregations`
  // The order of these matter and `aggregations` is keeping track of them
  // `passThrough` are columns that go directly from the input to the output. The first value
  // is the index in the original input batch. The second value is the index in the final output
  // batch
  // `orderByPositions`  and `partByPositions` are the positions in `initialProjections` for
  // the order by columns and the part by columns respectively.
  private val (initialProjections,
  passThrough,
  aggregations,
  orderByPositions,
  partByPositions) = {
    val initialProjections = ArrayBuffer[Expression]()
    val dedupedInitialProjections = mutable.HashMap[Expression, Int]()

    def getOrAddInitialProjectionIndex(expr: Expression): Int =
      dedupedInitialProjections.getOrElseUpdate(expr, {
        val at = initialProjections.length
        initialProjections += expr
        at
      })

    val passThrough = ArrayBuffer[(Int, Int)]()
    val aggregations = new GroupedAggregations()

    boundWindowOps.zipWithIndex.foreach {
      case (GpuAlias(GpuBoundReference(inputIndex, _, _), _), outputIndex) =>
        passThrough.append((inputIndex, outputIndex))
      case (GpuBoundReference(inputIndex, _, _), outputIndex) =>
        passThrough.append((inputIndex, outputIndex))
      case (GpuAlias(win: GpuWindowExpression, _), outputIndex) =>
        val inputLocations = win.initialProjections(isRunningBatched)
            .map(getOrAddInitialProjectionIndex).toArray
        aggregations.addAggregation(win, inputLocations, outputIndex)
      case _ =>
        throw new IllegalArgumentException("Unexpected operation found in window expression")
    }

    val partByPositions =  boundPartitionSpec.map(getOrAddInitialProjectionIndex).toArray
    val orderByPositions = boundOrderSpec.map { so =>
      getOrAddInitialProjectionIndex(so.child)
    }.toArray

    (initialProjections, passThrough, aggregations, orderByPositions, partByPositions)
  }

  /**
   * Compute the basic aggregations. In some cases the resulting columns may not be the expected
   * types.  This could be caused by cudf type differences and can be fixed by calling
   * `castResultsIfNeeded` or it could be different because the window operations know about a
   * post processing step that needs to happen prior to `castResultsIfNeeded`.
   * @param cb the batch to do window aggregations on.
   * @return the cudf columns that are the results of doing the aggregations.
   */
  def computeBasicWindow(cb: ColumnarBatch): Array[cudf.ColumnVector] = {
    closeOnExcept(new Array[cudf.ColumnVector](boundWindowOps.length)) { outputColumns =>

      withResource(GpuProjectExec.project(cb, initialProjections.toSeq)) { proj =>
        aggregations.doAggs(
          isRunningBatched,
          boundOrderSpec,
          orderByPositions,
          partByPositions,
          proj,
          outputColumns)
      }

      // if the window aggregates were successful, lets splice the passThrough
      // columns
      passThrough.foreach {
        case (inputIndex, outputIndex) =>
          outputColumns(outputIndex) =
            cb.column(inputIndex).asInstanceOf[GpuColumnVector].getBase.incRefCount()
      }

      outputColumns
    }
  }

  def convertToBatch(dataTypes: Array[DataType],
      cols: Array[cudf.ColumnVector]): ColumnarBatch =
    aggregations.convertToColumnarBatch(dataTypes, cols)
}
