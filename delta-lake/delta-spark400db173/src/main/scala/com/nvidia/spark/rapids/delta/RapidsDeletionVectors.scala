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

package com.nvidia.spark.rapids.delta

import ai.rapids.cudf.{HostMemoryBuffer, Table}
import com.databricks.sql.io.{RowIndexFilterProvider, RowIndexFilterType}
import com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat._
import com.databricks.sql.transaction.tahoe.actions.DeletionVectorDescriptor
import com.databricks.sql.transaction.tahoe.deletionvectors.{RoaringBitmapArray, StoredBitmap}
import com.databricks.sql.transaction.tahoe.files.TahoeFileIndex
import com.databricks.sql.transaction.tahoe.storage.dv.DeletionVectorStoreEdge
import com.databricks.sql.transaction.tahoe.util.DeltaFileOperations.absolutePath
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import java.io.IOException
import java.nio.{ByteBuffer, ByteOrder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.BlockMetaData

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionedFile}
import org.apache.spark.sql.sources._

case class RapidsDeletionVectorDescriptorWithFilterType(
    descriptor: DeletionVectorDescriptor,
    filterType: RowIndexFilterType)

case class RapidsDeletionVectorReadInfo(
    tablePath: String,
    filePathToDVMap: Map[String, RapidsDeletionVectorDescriptorWithFilterType],
    filePathToFilterProvider: Map[String, RowIndexFilterProvider])

object RapidsDeletionVectors extends Logging {
  private val DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE = 4
  private val MISSING_ROW_INDEX_FILTER_MESSAGE = "Row index filter not found. file="

  case class DeletionVectorLookupResult(
      dvDescriptor: Option[String],
      filterType: Option[RowIndexFilterType],
      rowIndexFilterProvider: Option[RowIndexFilterProvider])

  private[delta] def isMissingRowIndexFilterAssertion(e: AssertionError): Boolean =
    Option(e.getMessage).exists(_.contains(MISSING_ROW_INDEX_FILTER_MESSAGE))

  /**
   * Translates filters to use physical column names instead of logical column names. This is
   * needed when Delta column mapping is enabled because Parquet files and pushed schemas use
   * physical names.
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
        logError(s"Failed to translate filter $filter")
        None
    }
  }

  def buildDeletionVectorReadInfo(
      relation: HadoopFsRelation,
      tablePath: Option[String]): Option[RapidsDeletionVectorReadInfo] = {
    relation.location match {
      case tahoeFileIndex: TahoeFileIndex =>
        val tahoeTablePath = tablePath.getOrElse(tahoeFileIndex.path.toString)
        val filterTypes = tahoeFileIndex.rowIndexFilters.getOrElse(Map.empty)
          .map(kv => kv._1 -> kv._2.getRowIndexFilterType)
        val matchingFiles = tahoeFileIndex
          .matchingFiles(partitionFilters = Seq(TrueLiteral), dataFilters = Seq(TrueLiteral))

        def fileKeys(relativePath: String): Seq[String] = {
          val absolute = absolutePath(tahoeFileIndex.path.toString, relativePath)
          Seq(relativePath, absolute.toString, absolute.toUri.toString).distinct
        }

        val filePathToFilterProvider = {
          val fromRowIndexFilters = tahoeFileIndex.rowIndexFilters.getOrElse(Map.empty)
            .flatMap { case (path, provider) =>
              fileKeys(path).map(_ -> provider)
            }
          val fromMatchingFiles = matchingFiles.flatMap { addFile =>
            val provider = try {
              tahoeFileIndex.getRowIndexFilterForFile(addFile.path)
            } catch {
              // DB-17.3 can assert here when the AddFile path and candidate path are equivalent
              // but rendered differently. The direct rowIndexFilters map and DV descriptor map
              // still cover the file lookup, so skip this optional provider path.
              case e: AssertionError if isMissingRowIndexFilterAssertion(e) => None
            }
            provider.toSeq.flatMap { p => fileKeys(addFile.path).map(_ -> p) }
          }
          (fromRowIndexFilters ++ fromMatchingFiles).toMap
        }

        val filesWithDVs = matchingFiles.filter(_.deletionVector != null)
        val filePathToDVMap = filesWithDVs.flatMap { addFile =>
          val filterType = filterTypes.getOrElse(addFile.path, RowIndexFilterType.IF_CONTAINED)
          fileKeys(addFile.path).map { key =>
            key -> RapidsDeletionVectorDescriptorWithFilterType(addFile.deletionVector, filterType)
          }
        }.toMap

        Some(RapidsDeletionVectorReadInfo(
          tahoeTablePath, filePathToDVMap, filePathToFilterProvider))
      case _ => None
    }
  }

  def lookupDeletionVector(
      partitionedFile: PartitionedFile,
      deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo])
  : DeletionVectorLookupResult = {
    val (dvDescriptorOpt, filterTypeOpt) = deletionVectorDescriptorAndFilter(partitionedFile)
    val rowIndexFilterProviderOpt = partitionedFile.getRowIndexFilter.toSeq.headOption

    if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
      return DeletionVectorLookupResult(
        dvDescriptorOpt, filterTypeOpt, rowIndexFilterProviderOpt)
    } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
      throw new IllegalStateException(
        s"Both $FILE_ROW_INDEX_FILTER_ID_ENCODED and $FILE_ROW_INDEX_FILTER_TYPE " +
          "should either both have values or no values at all.")
    }

    rowIndexFilterProviderOpt
      .map(provider => DeletionVectorLookupResult(None, None, Some(provider)))
      .orElse {
        val lookupKeys = fileLookupKeys(partitionedFile)
        lookupKeys.flatMap(key =>
          deletionVectorReadInfo.flatMap(_.filePathToDVMap.get(key))).headOption
          .map { descriptorWithFilterType =>
            DeletionVectorLookupResult(
              Some(descriptorWithFilterType.descriptor.serializeToBase64()),
              Some(descriptorWithFilterType.filterType),
              None)
          }
      }
      .orElse {
        val lookupKeys = fileLookupKeys(partitionedFile)
        lookupKeys.flatMap(key =>
          deletionVectorReadInfo.flatMap(_.filePathToFilterProvider.get(key))).headOption
          .map(provider => DeletionVectorLookupResult(None, None, Some(provider)))
      }
      .getOrElse(DeletionVectorLookupResult(None, None, None))
  }

  /**
   * Reads the deletion vector bitmap for a PartitionedFile and returns it as a serialized
   * standard bitmap in host memory. Files with no DV get an empty serialized bitmap.
   */
  def loadDeletionVector(
      conf: Configuration,
      partitionedFile: PartitionedFile,
      tablePath: String): HostMemoryBuffer = {
    loadDeletionVector(conf, partitionedFile, tablePath, None)
  }

  def loadDeletionVector(
      conf: Configuration,
      partitionedFile: PartitionedFile,
      tablePath: String,
      deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo]): HostMemoryBuffer = {
    val dv = lookupDeletionVector(partitionedFile, deletionVectorReadInfo)
    loadDeletionVector(
      conf,
      dv.dvDescriptor,
      dv.filterType,
      dv.rowIndexFilterProvider,
      tablePath)
  }

  def loadDeletionVector(
      conf: Configuration,
      dvDescriptorOpt: Option[String],
      tablePath: String): HostMemoryBuffer =
    loadDeletionVector(
      conf,
      dvDescriptorOpt,
      dvDescriptorOpt.map(_ => RowIndexFilterType.IF_CONTAINED),
      None,
      tablePath)

  def loadDeletionVector(
      conf: Configuration,
      dvDescriptorOpt: Option[String],
      rowIndexFilterProviderOpt: Option[RowIndexFilterProvider],
      tablePath: String): HostMemoryBuffer =
    loadDeletionVector(
      conf,
      dvDescriptorOpt,
      dvDescriptorOpt.map(_ => RowIndexFilterType.IF_CONTAINED),
      rowIndexFilterProviderOpt,
      tablePath)

  private def loadDeletionVector(
      conf: Configuration,
      dvDescriptorOpt: Option[String],
      filterTypeOpt: Option[RowIndexFilterType],
      rowIndexFilterProviderOpt: Option[RowIndexFilterProvider],
      tablePath: String): HostMemoryBuffer = {
    if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
      val dvDesc = DeletionVectorDescriptor.deserializeFromBase64(dvDescriptorOpt.get)
      filterTypeOpt.get match {
        case RowIndexFilterType.IF_CONTAINED =>
          if (dvDesc.cardinality == 0) {
            serializedEmptyBitmap()
          } else {
            val dvStore = DeletionVectorStoreEdge.createInstance(conf, None)
            val storedBitmap = StoredBitmap.create(dvDesc, new Path(tablePath))
            loadStandardSerializedBitmap(storedBitmap.loadSerialized(dvStore).buffer)
          }
        case unexpectedFilterType => throw new IllegalStateException(
          s"Unexpected row index filter type for Deletion Vectors. " +
            s"Expected: ${RowIndexFilterType.IF_CONTAINED}; Actual: ${unexpectedFilterType}")
      }
    } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
      throw new IllegalStateException(
        "Both dvDescriptorOpt and filterTypeOpt must be defined together or both absent.")
    } else {
      rowIndexFilterProviderOpt.map(loadDeletionVector(conf, _))
        .getOrElse(serializedEmptyBitmap())
    }
  }

  private def loadDeletionVector(
      conf: Configuration,
      rowIndexFilterProvider: RowIndexFilterProvider): HostMemoryBuffer = {
    require(rowIndexFilterProvider.getRowIndexFilterType == RowIndexFilterType.IF_CONTAINED,
      s"Unexpected row index filter type for Deletion Vectors. " +
        s"Expected: ${RowIndexFilterType.IF_CONTAINED}; " +
        s"Actual: ${rowIndexFilterProvider.getRowIndexFilterType}")
    if (rowIndexFilterProvider.getCardinality == 0) {
      serializedEmptyBitmap()
    } else {
      loadStandardSerializedBitmap(rowIndexFilterProvider.retrieveSerialized(conf).buffer)
    }
  }

  def loadScalaBitmap(
      conf: Configuration,
      partitionedFile: PartitionedFile,
      tablePath: String): RoaringBitmapArray = {
    loadScalaBitmap(conf, partitionedFile, tablePath, None)
  }

  def loadScalaBitmap(
      conf: Configuration,
      partitionedFile: PartitionedFile,
      tablePath: String,
      deletionVectorReadInfo: Option[RapidsDeletionVectorReadInfo]): RoaringBitmapArray = {
    val dv = lookupDeletionVector(partitionedFile, deletionVectorReadInfo)
    loadScalaBitmap(
      conf,
      dv.dvDescriptor,
      dv.filterType,
      dv.rowIndexFilterProvider,
      tablePath)
  }

  def loadScalaBitmap(
      conf: Configuration,
      dvDescriptorOpt: Option[String],
      filterTypeOpt: Option[RowIndexFilterType],
      rowIndexFilterProviderOpt: Option[RowIndexFilterProvider],
      tablePath: String): RoaringBitmapArray = {
    if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
      val dvDesc = DeletionVectorDescriptor.deserializeFromBase64(dvDescriptorOpt.get)
      filterTypeOpt.get match {
        case RowIndexFilterType.IF_CONTAINED =>
          val dvStore = new com.databricks.sql.transaction.tahoe.storage.dv.HadoopFileSystemDVStore(
            conf)
          StoredBitmap.create(dvDesc, new Path(tablePath)).load(dvStore)
        case unexpectedFilterType => throw new IllegalStateException(
          s"Unexpected row index filter type for Deletion Vectors. " +
            s"Expected: ${RowIndexFilterType.IF_CONTAINED}; Actual: ${unexpectedFilterType}")
      }
    } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
      throw new IllegalStateException(
        "Both dvDescriptorOpt and filterTypeOpt must be defined together or both absent.")
    } else {
      rowIndexFilterProviderOpt.map { provider =>
        require(provider.getRowIndexFilterType == RowIndexFilterType.IF_CONTAINED,
          s"Unexpected row index filter type for Deletion Vectors. " +
            s"Expected: ${RowIndexFilterType.IF_CONTAINED}; " +
            s"Actual: ${provider.getRowIndexFilterType}")
        RoaringBitmapArray.readFrom(provider.retrieveSerialized(conf).buffer)
      }.getOrElse(new RoaringBitmapArray())
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
   * Drops the first column from a table. Used when reading with deletion vectors, as the cuDF API
   * prepends a UINT64 index column that is not used.
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

  private def deletionVectorDescriptorAndFilter(
      partitionedFile: PartitionedFile): (Option[String], Option[RowIndexFilterType]) = {
    val dvDescriptorOpt = partitionedFile.otherConstantMetadataColumnValues
      .get(FILE_ROW_INDEX_FILTER_ID_ENCODED).asInstanceOf[Option[String]]
    val filterTypeOpt = partitionedFile.otherConstantMetadataColumnValues
      .get(FILE_ROW_INDEX_FILTER_TYPE).asInstanceOf[Option[RowIndexFilterType]]
    (dvDescriptorOpt, filterTypeOpt)
  }

  private def fileLookupKeys(partitionedFile: PartitionedFile): Seq[String] = {
    Seq(
      partitionedFile.pathUri.toString,
      partitionedFile.filePath.toString,
      partitionedFile.urlEncodedPath).distinct
  }

  private def loadStandardSerializedBitmap(bytes: Array[Byte]): HostMemoryBuffer = {
    if (bytes.isEmpty) {
      serializedEmptyBitmap()
    } else {
      val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
      byteBuffer.getInt match {
        case magic if magic == DeltaBitmapUtils.portableMagicNumber() =>
          copySerializedBytes(
            bytes,
            DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE,
            bytes.length - DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE)
        case magic if magic == DeltaBitmapUtils.nativeMagicNumber() =>
          val bitmaps = DeltaBitmapUtils.deserializeNative(byteBuffer)
          val reserialized = DeltaBitmapUtils.serializeAsDeltaPortable(bitmaps)
          copySerializedBytes(
            reserialized,
            DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE,
            reserialized.length - DELTA_BITMAP_MAGIC_NUMBER_BYTE_SIZE)
        case other =>
          throw new IOException(s"Unexpected RoaringBitmapArray magic number $other")
      }
    }
  }

  private def serializedEmptyBitmap(): HostMemoryBuffer = {
    closeOnExcept(HostMemoryBuffer.allocate(8)) { buffer =>
      buffer.setLong(0, 0L)
      buffer
    }
  }

  private def copySerializedBytes(
      bytes: Array[Byte],
      offset: Int,
      length: Int): HostMemoryBuffer = {
    closeOnExcept(HostMemoryBuffer.allocate(length)) { buffer =>
      buffer.setBytes(0, bytes, offset, length)
      buffer
    }
  }
}
