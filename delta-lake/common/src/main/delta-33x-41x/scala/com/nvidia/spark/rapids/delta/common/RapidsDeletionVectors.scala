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
import org.apache.spark.sql.delta.deletionvectors.{RapidsDeletionVectorStore, RapidsDeletionVectorStoredBitmap, RoaringBitmapArray, StoredBitmap}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.rapids.DeltaMdcShims.mdc
import org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore
import org.apache.spark.sql.sources._

object RapidsDeletionVectors extends Logging {
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
    if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
      val dvDesc = DeletionVectorDescriptor.deserializeFromBase64(dvDescriptorOpt.get)

      // The filter type should always be IF_CONTAINED for deletion vectors
      // as the bitmap represents the rows to be deleted.
      // See [[RowIndexFilterType]] for more details.
      filterTypeOpt.get match {
        case RowIndexFilterType.IF_CONTAINED =>
          val dvStore = RapidsDeletionVectorStore.createInstance(fileIO)
          val storedBitmap = RapidsDeletionVectorStoredBitmap(dvDesc, new Path(tablePath))
          storedBitmap.load(dvStore)
        case unexpectedFilterType => throw new IllegalStateException(
          s"Unexpected row index filter type for Deletion Vectors. " +
            s"Expected: ${RowIndexFilterType.IF_CONTAINED}; Actual: ${unexpectedFilterType}")
      }
    } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
      throw new IllegalStateException(
        "Both dvDescriptorOpt and filterTypeOpt must be defined together or both absent.")
    } else {
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
    if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
      val dvDesc = DeletionVectorDescriptor.deserializeFromBase64(dvDescriptorOpt.get)

      // The filter type should always be IF_CONTAINED for deletion vectors
      // as the bitmap represents the rows to be deleted.
      // See [[RowIndexFilterType]] for more details.
      filterTypeOpt.get match {
        case RowIndexFilterType.IF_CONTAINED =>
          val dvStore = new HadoopFileSystemDVStore(conf)
          StoredBitmap.create(dvDesc, new Path(tablePath)).load(dvStore)
        case unexpectedFilterType => throw new IllegalStateException(
          s"Unexpected row index filter type for Deletion Vectors. " +
            s"Expected: ${RowIndexFilterType.IF_CONTAINED}; Actual: ${unexpectedFilterType}")
      }
    } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
      throw new IllegalStateException(
        "Both dvDescriptorOpt and filterTypeOpt must be defined together or both absent.")
    } else {
      new RoaringBitmapArray()
    }
  }

  def getRowGroupMetadata(blocks: collection.Seq[BlockMetaData]): (Array[Long], Array[Int]) =
    RapidsDeletionVectorRowCountUtils.getRowGroupMetadata(blocks)

  /**
   * Computes the number of deleted rows within the given row ranges in the bitmap.
   */
  def countDeletedRows(
      scalaBitmap: RoaringBitmapArray,
      rowGroupOffsets: Array[Long],
      rowGroupNumRows: Array[Int]): Long = {
    RapidsDeletionVectorRowCountUtils.countDeletedRows(
      scalaBitmap.cardinality, rowGroupOffsets, rowGroupNumRows) { countDeletedRow =>
        scalaBitmap.forEach { deletedIndex: Long =>
          countDeletedRow(deletedIndex)
        }
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
}
