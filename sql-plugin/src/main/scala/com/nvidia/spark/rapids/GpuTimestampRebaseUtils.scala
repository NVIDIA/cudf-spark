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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType}
import ai.rapids.cudf.{HostColumnVector, OrderByArg, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.sql.rapids.RebaseDateTimeBridge

private[rapids] object GpuTimestampRebaseUtils {

  private def retainOrCopy(input: ColumnView): ColumnVector = input match {
    case columnVector: ColumnVector => columnVector.incRefCount()
    case _ => input.copyToColumnVector()
  }

  private def isModernOrAllNull(input: ColumnView): Boolean = {
    withResource(input.min()) { minValue =>
      !minValue.isValid || minValue.getLong >= RebaseDateTimeBridge.lastSwitchJulianTs
    }
  }

  final class JulianToGregorianMicrosContext(
      timeZoneId: String,
      switches: Option[Table],
      paddedDiffs: Option[Table]) extends AutoCloseable {

    private def rebaseOnHost(input: ColumnView): ColumnVector = {
      val rowCount = input.getRowCount.toInt
      withResource(input.copyToHost()) { hostInput =>
        withResource(HostColumnVector.builder(DType.TIMESTAMP_MICROSECONDS, rowCount)) { builder =>
          var row = 0
          while (row < rowCount) {
            if (hostInput.isNull(row)) {
              builder.appendNull()
            } else {
              builder.append(RebaseDateTimeBridge.rebaseJulianToGregorianMicros(
                timeZoneId, hostInput.getLong(row)))
            }
            row += 1
          }
          withResource(builder.build()) { hostOutput =>
            hostOutput.copyToDevice()
          }
        }
      }
    }

    private def rebaseWithSearchColumn(
        input: ColumnView,
        searchColumn: ColumnVector): ColumnVector = {
      withResource(new Table(searchColumn)) { searchTable =>
        withResource(switches.get.upperBound(searchTable, OrderByArg.asc(0, false))) { indices =>
          val hasBeforeFirstSwitch = withResource(indices.min()) { minIndex =>
            minIndex.isValid && minIndex.getInt == 0
          }
          if (hasBeforeFirstSwitch) {
            // Spark's precomputed maps intentionally stop at the Common Era boundary. Preserve
            // exact Spark semantics for rarer BCE data by using its Calendar-based slow path.
            rebaseOnHost(input)
          } else {
            withResource(paddedDiffs.get.gather(indices)) { gatheredDiffs =>
              input.binaryOp(
                BinaryOp.ADD, gatheredDiffs.getColumn(0), DType.TIMESTAMP_MICROSECONDS)
            }
          }
        }
      }
    }

    def rebase(input: ColumnView): ColumnVector = {
      require(input.getType == DType.TIMESTAMP_MICROSECONDS,
        s"expected TIMESTAMP_MICROSECONDS but found ${input.getType}")
      if (input.getRowCount == 0 || isModernOrAllNull(input)) {
        retainOrCopy(input)
      } else {
        rebaseLegacy(input)
      }
    }

    private[rapids] def rebaseLegacy(input: ColumnView): ColumnVector = {
      if (switches.isEmpty) {
        // Spark's bundled map can lag valid IDs added by newer JDK timezone databases.
        rebaseOnHost(input)
      } else {
        input match {
          case columnVector: ColumnVector =>
            rebaseWithSearchColumn(input, columnVector)
          case _ =>
            withResource(input.copyToColumnVector()) { searchColumn =>
              rebaseWithSearchColumn(input, searchColumn)
            }
        }
      }
    }

    override def close(): Unit = {
      try {
        switches.foreach(_.close())
      } finally {
        paddedDiffs.foreach(_.close())
      }
    }
  }

  final class LazyJulianToGregorianMicrosContext(timeZoneId: String) extends AutoCloseable {
    private var delegate: JulianToGregorianMicrosContext = _

    def rebase(input: ColumnView): ColumnVector = {
      require(input.getType == DType.TIMESTAMP_MICROSECONDS,
        s"expected TIMESTAMP_MICROSECONDS but found ${input.getType}")
      if (input.getRowCount == 0 || isModernOrAllNull(input)) {
        retainOrCopy(input)
      } else {
        if (delegate == null) {
          delegate = createJulianToGregorianMicrosContext(timeZoneId)
        }
        delegate.rebaseLegacy(input)
      }
    }

    override def close(): Unit = if (delegate != null) {
      delegate.close()
      delegate = null
    }
  }

  def createJulianToGregorianMicrosContext(
      timeZoneId: String): JulianToGregorianMicrosContext = {
    RebaseDateTimeBridge.getJulianToGregorianMicros(timeZoneId).map { info =>
      require(info.switches.nonEmpty, s"empty Spark timestamp rebase map for '$timeZoneId'")
      require(info.switches.length == info.diffs.length,
        s"invalid Spark timestamp rebase map for '$timeZoneId'")

      val switchTable = withResource(
        ColumnVector.timestampMicroSecondsFromLongs(info.switches: _*)) { switchColumn =>
        new Table(switchColumn)
      }
      closeOnExcept(switchTable) { _ =>
        // upperBound returns 0 before the first switch and k + 1 at switch k. The leading
        // sentinel aligns every valid upper-bound index directly with its Spark rebase diff.
        val padded = new Array[Long](info.diffs.length + 1)
        System.arraycopy(info.diffs, 0, padded, 1, info.diffs.length)
        val diffTable = withResource(
          ColumnVector.durationMicroSecondsFromLongs(padded: _*)) { diffColumn =>
          new Table(diffColumn)
        }
        new JulianToGregorianMicrosContext(
          timeZoneId, Some(switchTable), Some(diffTable))
      }
    }.getOrElse {
      new JulianToGregorianMicrosContext(timeZoneId, None, None)
    }
  }
}
