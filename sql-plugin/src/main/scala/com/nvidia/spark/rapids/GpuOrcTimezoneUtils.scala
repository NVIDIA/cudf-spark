/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.time.{DateTimeException, ZoneId}
import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.jni.{DateTimeRebase, GpuTimeZoneDB}

object GpuOrcTimezoneUtils {

  /** Resolve an ORC stripe footer timezone once at the metadata boundary. */
  private[rapids] def resolveWriterTimezone(writerTimezone: String): ZoneId = {
    if (writerTimezone.isEmpty) {
      ZoneId.systemDefault()
    } else {
      try {
        ZoneId.of(writerTimezone, ZoneId.SHORT_IDS)
      } catch {
        case e: DateTimeException =>
          throw new IllegalArgumentException(
            s"Unrecognized writer timezone in ORC stripe footer: '$writerTimezone'", e)
      }
    }
  }

  /** Return whether every timezone has the same rules. */
  private[rapids] def writerTimezonesShareRules(writerTimezones: Iterable[ZoneId]): Boolean = {
    writerTimezones.headOption.forall { head =>
      writerTimezones.forall(_.getRules == head.getRules)
    }
  }

  /**
   * Convert an integer-derived local timestamp using the same timezone semantics as Spark's
   * ORC schema-evolution reader.
   *
   * Apache ORC uses java.util.TimeZone for this conversion, while Spark materializes the
   * resulting java.sql.Timestamp using java.time rules. Before the reader timezone's first
   * recorded transition those rule sets can differ, so use java.time for historical values and
   * retain ORC's conversion for all later values, including DST gaps and overlaps.
   */
  private[rapids] def convertOrcIntegerTimestamp(
      timestamp: ColumnVector,
      readerZone: ZoneId): ColumnVector = {
    val readerTz = readerZone.getId
    withResource(GpuTimeZoneDB.buildOrcTimezoneContext(readerTz, readerTz)) { tzCtx =>
      withResource(GpuTimeZoneDB.convertOrcFromUtc(timestamp, tzCtx)) { orcTimestamp =>
        val firstTransitionUs = tzCtx.getReaderFirstTransitionUs
        if (firstTransitionUs == Long.MinValue) {
          orcTimestamp.incRefCount()
        } else {
          withResource(GpuTimeZoneDB.fromTimestampToUtcTimestamp(
              timestamp, readerZone.normalized())) { javaTimeTimestamp =>
            withResource(Scalar.timestampFromLong(
                DType.TIMESTAMP_MICROSECONDS, firstTransitionUs)) { firstTransition =>
              withResource(timestamp.lessThan(firstTransition)) { isHistorical =>
                isHistorical.ifElse(javaTimeTimestamp, orcTimestamp)
              }
            }
          }
        }
      }
    }
  }

  /**
   * Rebase ORC legacy dates and timestamps considering writer and reader timezones.
   *
   * Uses the JNI kernel `GpuTimeZoneDB.convertOrcTimezones` for both same- and cross-timezone
   * reads. Even when the timezone rules match, the kernel must reconstruct the writer-specific
   * ORC 2015 base before deciding whether to apply the negative nanos borrow.
   *
   * @param input the input table (timestamps read as UTC via ignoreTimezoneInStripeFooter)
   * @param writerTimezone the resolved writer timezone from the ORC stripe footer
   * @param writerUsedProlepticGregorian whether the writer used the proleptic Gregorian calendar
   * @return table with rebased date/time columns; input is closed
   */
  def rebaseOrcDateTime(
      input: Table,
      writerTimezone: ZoneId,
      writerUsedProlepticGregorian: Boolean): Table = {
    rebaseWithWriterTimezone(input, writerTimezone.getId, ZoneId.systemDefault().getId,
      writerUsedProlepticGregorian)
  }

  /**
   * Rebase date/time values using the writer calendar and writer/reader timezones.
   *
   * Legacy dates are rebased from the hybrid Julian/Gregorian calendar to the proleptic
   * Gregorian calendar. Proleptic dates are retained unchanged.
   *
   * cuDF reads ORC timestamps with `ignoreTimezoneInStripeFooter`, so the base_timestamp
   * is computed in UTC. ORC Java computes base_timestamp in the *writer* timezone, so the
   * millis passed to `convertBetweenTimezones` already encode the writer TZ base offset.
   *
   * To match ORC Java, the JNI `convertOrcTimezones` kernel first applies the writer TZ base
   * offset and recomputes the negative nanos borrow, then applies any writer-to-reader TZ delta.
   */
  private def rebaseWithWriterTimezone(
      input: Table,
      writerTz: String,
      readerTz: String,
      writerUsedProlepticGregorian: Boolean): Table = {
    val readerZone = ZoneId.of(readerTz, ZoneId.SHORT_IDS)
    withResource(input) { _ =>
      if (containsOrcTimestamp(input)) {
        withResource(GpuTimeZoneDB.buildOrcTimezoneContext(writerTz, readerTz)) { tzCtx =>
          rebaseColumns(input, Some(tzCtx), readerZone, writerUsedProlepticGregorian)
        }
      } else {
        rebaseColumns(input, None, readerZone, writerUsedProlepticGregorian)
      }
    }
  }

  private def containsOrcTimestamp(input: Table): Boolean = {
    (0 until input.getNumberOfColumns).exists { colIdx =>
      containsOrcTimestamp(input.getColumn(colIdx))
    }
  }

  private def containsOrcTimestamp(col: ColumnView): Boolean = {
    val dType = col.getType
    if (dType.hasTimeResolution) {
      true
    } else if (dType == DType.LIST || dType == DType.STRUCT) {
      (0 until col.getNumChildren).exists { childIdx =>
        withResource(col.getChildColumnView(childIdx)) { child =>
          containsOrcTimestamp(child)
        }
      }
    } else {
      false
    }
  }

  private def rebaseColumns(
      input: Table,
      tzCtx: Option[GpuTimeZoneDB.OrcTimezoneContext],
      readerZone: ZoneId,
      writerUsedProlepticGregorian: Boolean): Table = {
    val newColumns = (0 until input.getNumberOfColumns).safeMap { colIdx =>
      val col = input.getColumn(colIdx)
      val dType = col.getType
      if (dType == DType.TIMESTAMP_DAYS && !writerUsedProlepticGregorian) {
        DateTimeRebase.rebaseJulianToGregorian(col)
      } else if (dType.hasTimeResolution) {
        convertOrcTimestamp(col, tzCtx.get, readerZone)
      } else if (dType == DType.LIST || dType == DType.STRUCT) {
        withResource(new ArrayBuffer[ColumnView]) { toClose =>
          val rebased = rebaseNestedWithWriterTimezone(
            col, tzCtx, readerZone, writerUsedProlepticGregorian, toClose)
          if (rebased eq col) {
            col.incRefCount()
          } else {
            toClose += rebased
            rebased.copyToColumnVector()
          }
        }
      } else {
        col.incRefCount()
      }
    }
    withResource(newColumns) { _ =>
      new Table(newColumns: _*)
    }
  }

  /**
   * Match the full Spark ORC timestamp path. Apache ORC uses java.util.TimeZone while decoding,
   * but Spark materializes the resulting java.sql.Timestamp using java.time rules. Those rule
   * sets can differ for historical and projected timestamps.
   */
  private def convertOrcTimestamp(
      col: ColumnView,
      tzCtx: GpuTimeZoneDB.OrcTimezoneContext,
      readerZone: ZoneId): ai.rapids.cudf.ColumnVector = {
    withResource(GpuTimeZoneDB.convertOrcTimezones(col, tzCtx)) { orcTimestamp =>
      val firstTransitionUs = tzCtx.getReaderFirstTransitionUs
      if (firstTransitionUs == Long.MinValue) {
        orcTimestamp.incRefCount()
      } else {
        val utilMicros = withResource(
            GpuTimeZoneDB.convertOrcFromUtc(orcTimestamp, tzCtx)) { utilUtc =>
          utilUtc.castTo(DType.INT64)
        }
        val ruleCorrection = withResource(utilMicros) { utilMicros =>
          withResource(GpuTimeZoneDB.fromTimestampToUtcTimestamp(
              orcTimestamp, readerZone.normalized())) { zoneUtc =>
            withResource(zoneUtc.castTo(DType.INT64)) { zoneMicros =>
              zoneMicros.sub(utilMicros)
            }
          }
        }
        val correctedTimestamp = withResource(ruleCorrection) { ruleCorrection =>
          withResource(orcTimestamp.castTo(DType.INT64)) { orcMicros =>
            withResource(orcMicros.add(ruleCorrection)) { corrected =>
              corrected.castTo(DType.TIMESTAMP_MICROSECONDS)
            }
          }
        }
        withResource(correctedTimestamp) { _ =>
          withResource(Scalar.timestampFromLong(
              DType.TIMESTAMP_MICROSECONDS, firstTransitionUs)) { firstTransition =>
            withResource(orcTimestamp.lessThan(firstTransition)) { needsCorrection =>
              needsCorrection.ifElse(correctedTimestamp, orcTimestamp)
            }
          }
        }
      }
    }
  }

  private def rebaseNestedWithWriterTimezone(
      col: ColumnView,
      tzCtx: Option[GpuTimeZoneDB.OrcTimezoneContext],
      readerZone: ZoneId,
      writerUsedProlepticGregorian: Boolean,
      toClose: ArrayBuffer[ColumnView]): ColumnView = {
    val addToClose = (v: ColumnView) => { toClose += v; v }
    val dType = col.getType

    if (dType == DType.TIMESTAMP_DAYS && !writerUsedProlepticGregorian) {
      DateTimeRebase.rebaseJulianToGregorian(col)
    } else if (dType.hasTimeResolution) {
      convertOrcTimestamp(col, tzCtx.get, readerZone)
    } else if (dType == DType.LIST) {
      val child = addToClose(col.getChildColumnView(0))
      val newChild = rebaseNestedWithWriterTimezone(
        child, tzCtx, readerZone, writerUsedProlepticGregorian, toClose)
      if (newChild ne child) {
        col.replaceListChild(addToClose(newChild))
      } else {
        col
      }
    } else if (dType == DType.STRUCT) {
      var childChanged = false
      val newViews = (0 until col.getNumChildren).map { i =>
        val child = addToClose(col.getChildColumnView(i))
        val newChild = rebaseNestedWithWriterTimezone(
          child, tzCtx, readerZone, writerUsedProlepticGregorian, toClose)
        if (newChild ne child) {
          childChanged = true
          addToClose(newChild)
        }
        newChild
      }
      if (childChanged) {
        val opNullCount = Optional.of(col.getNullCount.asInstanceOf[java.lang.Long])
        new ColumnView(col.getType, col.getRowCount, opNullCount, col.getValid,
          col.getOffsets, newViews.toArray)
      } else {
        col
      }
    } else {
      col
    }
  }
}
