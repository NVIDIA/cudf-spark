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

package com.nvidia.spark.rapids.delta.common

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.delta.RapidsDeletionVectorRowCountUtils
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.BlockMetaData

import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.deletionvectors.{
  RapidsDeletionVectorStoredBitmap,
  RoaringBitmapArray,
  StoredBitmap
}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.rapids.DeltaMdcShims.mdc
import org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore
import org.apache.spark.sql.sources._

object RapidsDeletionVectors extends Logging {
  private def dvDescAndFilterType(
      dvDescriptorOpt: Option[String],
      filterTypeOpt: Option[RowIndexFilterType])
  : Option[(DeletionVectorDescriptor, RowIndexFilterType)] = {
    (dvDescriptorOpt, filterTypeOpt) match {
      case (Some(dvDescriptor), Some(filterType)) =>
        Some((DeletionVectorDescriptor.deserializeFromBase64(dvDescriptor), filterType))
      case (None, None) =>
        None
      case (Some(_), None) | (None, Some(_)) =>
        throw new IllegalStateException(
          "Both dvDescriptorOpt and filterTypeOpt must be defined together or both absent.")
    }
  }

  /**
   * Translates the filter to use physical column names instead of logical column names.
   * This is needed when the column mapping mode is set to `NameMapping` or `IdMapping`
   * to match the requested schema that's passed to the [[ParquetFileFormat]].
   */
  def translateFilterForColumnMapping(
      filter: Filter,
      physicalNameMap: Map[String, String]): Option[Filter] = {
    object PhysicalAttribute {
      def unapply(attribute: String): Option[String] = {
        physicalNameMap.get(attribute)
      }
    }

    filter match {
      case EqualTo(PhysicalAttribute(physicalAttribute), value) =>
        Some(EqualTo(physicalAttribute, value))
      case EqualNullSafe(PhysicalAttribute(physicalAttribute), value) =>
        Some(EqualNullSafe(physicalAttribute, value))
      case GreaterThan(PhysicalAttribute(physicalAttribute), value) =>
        Some(GreaterThan(physicalAttribute, value))
      case GreaterThanOrEqual(PhysicalAttribute(physicalAttribute), value) =>
        Some(GreaterThanOrEqual(physicalAttribute, value))
      case LessThan(PhysicalAttribute(physicalAttribute), value) =>
        Some(LessThan(physicalAttribute, value))
      case LessThanOrEqual(PhysicalAttribute(physicalAttribute), value) =>
        Some(LessThanOrEqual(physicalAttribute, value))
      case In(PhysicalAttribute(physicalAttribute), values) =>
        Some(In(physicalAttribute, values))
      case IsNull(PhysicalAttribute(physicalAttribute)) =>
        Some(IsNull(physicalAttribute))
      case IsNotNull(PhysicalAttribute(physicalAttribute)) =>
        Some(IsNotNull(physicalAttribute))
      case And(left, right) =>
        val newLeft = translateFilterForColumnMapping(left, physicalNameMap)
        val newRight = translateFilterForColumnMapping(right, physicalNameMap)
        (newLeft, newRight) match {
          case (Some(l), Some(r)) => Some(And(l, r))
          case (Some(l), None) => Some(l)
          case (_, _) => newRight
        }
      case Or(left, right) =>
        val newLeft = translateFilterForColumnMapping(left, physicalNameMap)
        val newRight = translateFilterForColumnMapping(right, physicalNameMap)
        (newLeft, newRight) match {
          case (Some(l), Some(r)) => Some(Or(l, r))
          case (_, _) => None
        }
      case Not(child) =>
        translateFilterForColumnMapping(child, physicalNameMap).map(Not)
      case StringStartsWith(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringStartsWith(physicalAttribute, value))
      case StringEndsWith(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringEndsWith(physicalAttribute, value))
      case StringContains(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringContains(physicalAttribute, value))
      case AlwaysTrue() => Some(AlwaysTrue())
      case AlwaysFalse() => Some(AlwaysFalse())
      case _ =>
        logError(s"Failed to translate filter ${mdc(DeltaLogKeys.FILTER, filter)}")
        None
    }
  }

  /**
   * Reads the deletion vector bitmap for a given deletion vector descriptor and returns it
   * as a serialized standard bitmap in a HostMemoryBuffer. If the deletion vector descriptor
   * does not exist, an empty bitmap will be returned.
   */
  def loadDeletionVector(fileIO: RapidsFileIO,
      dvDescriptorOpt: Option[String],
      filterTypeOpt: Option[RowIndexFilterType],
      tablePath: String): HostMemoryBuffer = {
    dvDescAndFilterType(dvDescriptorOpt, filterTypeOpt) match {
      case Some((dvDesc, filterType)) =>
        // The bitmap represents marked row indexes. The filter type determines whether those
        // rows are removed or retained.
        // See [[RowIndexFilterType]] for more details.
        filterType match {
          case RowIndexFilterType.IF_CONTAINED | RowIndexFilterType.IF_NOT_CONTAINED =>
            val storedBitmap = RapidsDeletionVectorStoredBitmap(dvDesc, new Path(tablePath))
            storedBitmap.load(fileIO)
          case unexpectedFilterType => throw new IllegalStateException(
            s"Unexpected row index filter type for Deletion Vectors. " +
              s"Expected: ${RowIndexFilterType.IF_CONTAINED} or " +
              s"${RowIndexFilterType.IF_NOT_CONTAINED}; Actual: ${unexpectedFilterType}")
        }
      case None =>
        RapidsDeletionVectorStoredBitmap.serializedEmptyBitmap()
    }
  }

  /**
   * Convenience overload for callers that have already verified the filter type is
   * [[RowIndexFilterType.IF_CONTAINED]] and only carry the descriptor string.
   */
  def loadDeletionVector(
      fileIO: RapidsFileIO,
      dvDescriptorOpt: Option[String],
      tablePath: String): HostMemoryBuffer =
    loadDeletionVector(fileIO, dvDescriptorOpt,
      dvDescriptorOpt.map(_ => RowIndexFilterType.IF_CONTAINED),
      tablePath)

  def loadScalaBitmap(
      conf: Configuration,
      dvDescriptorOpt: Option[String],
      filterTypeOpt: Option[RowIndexFilterType],
      tablePath: String): RoaringBitmapArray = {
    dvDescAndFilterType(dvDescriptorOpt, filterTypeOpt) match {
      case Some((dvDesc, filterType)) =>
        // The bitmap represents marked row indexes. The filter type determines whether those
        // rows are removed or retained.
        // See [[RowIndexFilterType]] for more details.
        filterType match {
          case RowIndexFilterType.IF_CONTAINED | RowIndexFilterType.IF_NOT_CONTAINED =>
            val dvStore = new HadoopFileSystemDVStore(conf)
            StoredBitmap.create(dvDesc, new Path(tablePath)).load(dvStore)
          case unexpectedFilterType => throw new IllegalStateException(
            s"Unexpected row index filter type for Deletion Vectors. " +
              s"Expected: ${RowIndexFilterType.IF_CONTAINED} or " +
              s"${RowIndexFilterType.IF_NOT_CONTAINED}; Actual: ${unexpectedFilterType}")
        }
      case None =>
        new RoaringBitmapArray()
    }
  }

  def getRowGroupMetadata(blocks: collection.Seq[BlockMetaData]): (Array[Long], Array[Int]) =
    RapidsDeletionVectorRowCountUtils.getRowGroupMetadata(blocks)

  /**
   * Computes the number of marked rows within the given row ranges in the bitmap.
   */
  private def countMarkedRows(
      scalaBitmap: RoaringBitmapArray,
      rowGroupOffsets: Array[Long],
      rowGroupNumRows: Array[Int]): Long = {
    RapidsDeletionVectorRowCountUtils.countMarkedRows(
      scalaBitmap.cardinality, rowGroupOffsets, rowGroupNumRows) { countMarkedRow =>
        scalaBitmap.forEach { markedIndex: Long =>
          countMarkedRow(markedIndex)
        }
    }
  }

  /**
   * Computes the number of rows remaining after applying the deletion vector within the given
   * row ranges.
   */
  def computeNumRowsAlive(
      totalNumRows: Long,
      scalaBitmap: RoaringBitmapArray,
      filterTypeOpt: Option[RowIndexFilterType],
      rowGroupOffsets: Array[Long],
      rowGroupNumRows: Array[Int]): Long = {
    val numRowsMarked = countMarkedRows(scalaBitmap, rowGroupOffsets, rowGroupNumRows)
    require(numRowsMarked <= totalNumRows,
      s"Row-index filter cardinality ($numRowsMarked) exceeds file row count ($totalNumRows)")

    filterTypeOpt match {
      case Some(RowIndexFilterType.IF_CONTAINED) => totalNumRows - numRowsMarked
      case Some(RowIndexFilterType.IF_NOT_CONTAINED) => numRowsMarked
      case None => totalNumRows
      case Some(unexpectedFilterType) => throw new IllegalStateException(
        s"Unexpected row index filter type: $unexpectedFilterType")
    }
  }

  /**
   * Drops the first column from a table. Used when reading with deletion vectors,
   * as the cuDF API prepends a UINT64 index column that are not used.
   */
  def dropFirstColumn(table: Table): Table = {
    if (table.getNumberOfColumns == 0) {
      throw new IllegalStateException("Table has no columns to drop")
    } else {
      val columnIndices = (1 until table.getNumberOfColumns).toArray
      withResource(table) { _ =>
        new Table(columnIndices.map(table.getColumn): _*)
      }
    }
  }

  def isIfNotContainedRowIndexFilter(filterTypeOpt: Option[RowIndexFilterType]): Boolean = {
    filterTypeOpt.contains(RowIndexFilterType.IF_NOT_CONTAINED)
  }
}
