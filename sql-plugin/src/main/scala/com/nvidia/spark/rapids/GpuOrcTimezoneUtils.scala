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

import ai.rapids.cudf.{ColumnView, DType, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.jni.GpuTimeZoneDB

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
   * Rebase ORC timestamps considering writer and reader timezones.
   *
   * Uses the JNI kernel `GpuTimeZoneDB.convertOrcTimezones` for both same- and cross-timezone
   * reads. Even when the timezone rules match, the kernel must reconstruct the writer-specific
   * ORC 2015 base before deciding whether to apply the negative nanos borrow.
   *
   * @param input the input table (timestamps read as UTC via ignoreTimezoneInStripeFooter)
   * @param writerTimezone the resolved writer timezone from the ORC stripe footer
   * @return table with rebased timestamp columns; input is closed
   */
  def rebaseOrcTimestamps(input: Table, writerTimezone: ZoneId): Table = {
    rebaseWithWriterTimezone(input, writerTimezone.getId, ZoneId.systemDefault().getId)
  }

  /**
   * Rebase timestamps using the writer and reader timezones.
   *
   * cuDF reads ORC timestamps with `ignoreTimezoneInStripeFooter`, so the base_timestamp
   * is computed in UTC. ORC Java computes base_timestamp in the *writer* timezone, so the
   * millis passed to `convertBetweenTimezones` already encode the writer TZ base offset.
   *
   * To match ORC Java, the JNI `convertOrcTimezones` kernel first applies the writer TZ base
   * offset and recomputes the negative nanos borrow, then applies any writer-to-reader TZ delta.
   */
  private def rebaseWithWriterTimezone(
      input: Table, writerTz: String, readerTz: String): Table = {
    withResource(input) { _ =>
      withResource(GpuTimeZoneDB.buildOrcTimezoneContext(writerTz, readerTz)) { tzCtx =>
        val newColumns = (0 until input.getNumberOfColumns).safeMap { colIdx =>
          val col = input.getColumn(colIdx)
          val dType = col.getType
          if (dType.hasTimeResolution) {
            GpuTimeZoneDB.convertOrcTimezones(col, tzCtx)
          } else if (dType == DType.LIST || dType == DType.STRUCT) {
            withResource(new ArrayBuffer[ColumnView]) { toClose =>
              val rebased = rebaseNestedWithWriterTimezone(col, tzCtx, toClose)
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
    }
  }

  private def rebaseNestedWithWriterTimezone(
      col: ColumnView,
      tzCtx: GpuTimeZoneDB.OrcTimezoneContext,
      toClose: ArrayBuffer[ColumnView]): ColumnView = {
    val addToClose = (v: ColumnView) => { toClose += v; v }
    val dType = col.getType

    if (dType.hasTimeResolution) {
      GpuTimeZoneDB.convertOrcTimezones(col, tzCtx)
    } else if (dType == DType.LIST) {
      val child = addToClose(col.getChildColumnView(0))
      val newChild = rebaseNestedWithWriterTimezone(child, tzCtx, toClose)
      if (newChild ne child) {
        col.replaceListChild(addToClose(newChild))
      } else {
        col
      }
    } else if (dType == DType.STRUCT) {
      val newViews = (0 until col.getNumChildren).map { i =>
        val child = addToClose(col.getChildColumnView(i))
        val newChild = rebaseNestedWithWriterTimezone(child, tzCtx, toClose)
        if (newChild ne child) addToClose(newChild)
        newChild
      }
      val opNullCount = Optional.of(col.getNullCount.asInstanceOf[java.lang.Long])
      new ColumnView(col.getType, col.getRowCount, opNullCount, col.getValid,
        col.getOffsets, newViews.toArray)
    } else {
      col
    }
  }
}
