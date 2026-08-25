/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.parquet.converter

import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.iceberg.shaded.org.apache.parquet.column.{Encoding => ShadedEncoding, EncodingStats => ShadedEncodingStats}
import org.apache.iceberg.shaded.org.apache.parquet.column.statistics.{Statistics => ShadedStatistics}
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.metadata.{BlockMetaData => ShadedBlockMetaData, ColumnChunkMetaData => ShadedColumnChunkMetaData, ColumnChunkProperties => ShadedColumnChunkProperties, ColumnPath => ShadedColumnPath, CompressionCodecName => ShadedCompressionCodecName}
import org.apache.iceberg.shaded.org.apache.parquet.internal.hadoop.metadata.{IndexReference => ShadedIndexReference}
import org.apache.iceberg.shaded.org.apache.parquet.schema.{ColumnOrder => ShadedColumnOrder, GroupType => ShadedGroupType, LogicalTypeAnnotation => ShadedLogicalTypeAnnotation, MessageType => ShadedMessageType, PrimitiveType => ShadedPrimitiveType, Type => ShadedType}
import org.apache.iceberg.shaded.org.apache.parquet.schema.LogicalTypeAnnotation.{LogicalTypeAnnotationVisitor => ShadedLogicalTypeAnnotationVisitor, TimeUnit => ShadedTimeUnit}
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType.{PrimitiveTypeName => ShadedPrimitiveTypeName}
import org.apache.parquet.column.{Encoding, EncodingStats}
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.internal.hadoop.metadata.IndexReference
import org.apache.parquet.schema._
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName


object FromIcebergShaded {
  def unshade(columnOrder: ShadedColumnOrder): ColumnOrder = {
    if (columnOrder == null) {
      return null
    }
    columnOrder.getColumnOrderName match {
      case ShadedColumnOrder.ColumnOrderName.UNDEFINED => ColumnOrder.undefined()
      case ShadedColumnOrder.ColumnOrderName.TYPE_DEFINED_ORDER => ColumnOrder.typeDefined()
      case _ => throw new IllegalArgumentException(s"Unknown column order: $columnOrder")
    }
  }

  def unshade(typeName: ShadedPrimitiveTypeName): PrimitiveTypeName = {
    if (typeName == null) {
      return null
    }
    PrimitiveTypeName.valueOf(typeName.name())
  }

  def unshade(repetition: ShadedType.Repetition): Type.Repetition = {
    if (repetition == null) {
      return null
    }
    Type.Repetition.valueOf(repetition.name())
  }

  def unshade(inner: ShadedTimeUnit): TimeUnit = {
    if (inner == null) {
      return null
    }
    TimeUnit.valueOf(inner.name())
  }

  def unshade(inner: ShadedLogicalTypeAnnotation): LogicalTypeAnnotation = {
    if (inner == null) {
      return null
    }
    inner.accept(new ShadedLogicalTypeAnnotationVisitor[LogicalTypeAnnotation] {
      override def visit(stringLogicalType: ShadedLogicalTypeAnnotation
      .StringLogicalTypeAnnotation): Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.stringType())
      }

      override def visit(mapLogicalType: ShadedLogicalTypeAnnotation.MapLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.mapType())
      }

      override def visit(listLogicalType: ShadedLogicalTypeAnnotation.ListLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.listType())
      }

      override def visit(enumLogicalType: ShadedLogicalTypeAnnotation.EnumLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.enumType())
      }

      override def visit(shaded: ShadedLogicalTypeAnnotation
      .DecimalLogicalTypeAnnotation): Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.decimalType(
          shaded.getScale,
          shaded.getPrecision))
      }

      override def visit(dateLogicalType: ShadedLogicalTypeAnnotation.DateLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.dateType())
      }

      override def visit(inner: ShadedLogicalTypeAnnotation.TimeLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.timeType(
          inner.isAdjustedToUTC,
          unshade(inner.getUnit)))
      }

      override def visit(inner: ShadedLogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.timestampType(
          inner.isAdjustedToUTC,
          unshade(inner.getUnit)))
      }

      override def visit(inner: ShadedLogicalTypeAnnotation.IntLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.intType(
          inner.getBitWidth,
          inner.isSigned))
      }

      override def visit(inner: ShadedLogicalTypeAnnotation.JsonLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.jsonType())
      }

      override def visit(inner: ShadedLogicalTypeAnnotation.BsonLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.bsonType())
      }

      override def visit(inner: ShadedLogicalTypeAnnotation.UUIDLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.uuidType())
      }

      override def visit(inner: ShadedLogicalTypeAnnotation.IntervalLogicalTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance())
      }

      override def visit(inner: ShadedLogicalTypeAnnotation.MapKeyValueTypeAnnotation)
      : Optional[LogicalTypeAnnotation] = {
        Optional.of(LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance())
      }
    }).orElseGet(() => {
      throw new IllegalArgumentException(s"Unknown logical type annotation: $inner")
    })
  }


  def unshade(inner: ShadedType): Type = {
    if (inner == null) {
      return null
    }
    inner match {
      case t: ShadedPrimitiveType => unshade(t)
      case t: ShadedMessageType => unshade(t)
      case t: ShadedGroupType => unshade(t)
      case _ => throw new IllegalArgumentException(s"Unknown type: $inner")
    }
  }

  def unshade(inner: ShadedMessageType): MessageType = {
    if (inner == null) {
      return null
    }
    val builder = Types.buildMessage()
    inner.getFields.forEach { field =>
      builder.addField(unshade(field))
    }

    Option(inner.getId).foreach { id =>
      builder.id(id.intValue())
    }

    builder.named(inner.getName)
  }

  def unshade(inner: ShadedGroupType): GroupType = {
    if (inner == null) {
      return null
    }
    var builder = Types.buildGroup(unshade(inner.getRepetition))

    inner.getFields.forEach { field =>
      builder.addField(unshade(field))
    }

    Option(inner.getLogicalTypeAnnotation).foreach { ann =>
      builder = builder.as(unshade(ann))
    }

    Option(inner.getId).foreach { id =>
      builder = builder.id(id.intValue())
    }

    builder.named(inner.getName)
  }


  def unshade(inner: ShadedPrimitiveType): PrimitiveType = {
    if (inner == null) {
      return null
    }
    var builder = Types.primitive(unshade(inner.getPrimitiveTypeName),
        unshade(inner.getRepetition))
      .columnOrder(unshade(inner.columnOrder()))
      .length(inner.getTypeLength)

    Option(inner.getLogicalTypeAnnotation).foreach { ann =>
      builder = builder.as(unshade(ann))
    }

    Option(inner.getId).foreach { id =>
      builder = builder.id(id.intValue())
    }

    builder.named(inner.getName)
  }

  def unshade(inner: ShadedEncoding): Encoding = {
    if (inner == null) {
      return null
    }
    Encoding.valueOf(inner.name())
  }

  def unshade(inner: ShadedColumnPath): ColumnPath = {
    if (inner == null) {
      return null
    }
    ColumnPath.get(inner.toArray: _*)
  }

  def unshade(inner: ShadedCompressionCodecName): CompressionCodecName = {
    if (inner == null) {
      return null
    }
    CompressionCodecName.valueOf(inner.name())
  }

  def unshade(inner: ShadedColumnChunkProperties): ColumnChunkProperties = {
    if (inner == null) {
      return null
    }
    ColumnChunkProperties.get(
      unshade(inner.getPath),
      unshade(inner.getPrimitiveType),
      unshade(inner.getCodec),
      inner.getEncodings.asScala.map(unshade).asJava,
    )
  }

  def unshade(inner: ShadedEncodingStats): EncodingStats = {
    if (inner == null) {
      return null
    }

    val builder = new EncodingStats.Builder()

    if (inner.usesV2Pages()) {
      builder.withV2Pages()
    }

    for (encoding <- inner.getDictionaryEncodings.asScala) {
      builder.addDictEncoding(unshade(encoding), inner.getNumDictionaryPagesEncodedAs
      (encoding))
    }
    for (encoding <- inner.getDataEncodings.asScala) {
      builder.addDataEncoding(unshade(encoding), inner.getNumDataPagesEncodedAs(encoding))
    }

    builder.build()
  }

  def unshade(inner: ShadedIndexReference): IndexReference = {
    if (inner == null) {
      return null
    }
    new IndexReference(inner.getOffset, inner.getLength)
  }

  def unshade(inner: ShadedStatistics[_]): Statistics[_] = {
    if (inner == null) {
      return null
    }
    Statistics.getBuilderForReading(unshade(inner.`type`()))
      .withMin(inner.getMinBytes)
      .withMax(inner.getMaxBytes)
      .withNumNulls(inner.getNumNulls)
      .build()
  }

  def unshade(inner: ShadedColumnChunkMetaData): ColumnChunkMetaData = {
    if (inner == null) {
      return null
    }
    if (inner.isEncrypted) {
      throw new UnsupportedOperationException("Encrypted column chunks are not supported")
    }

    ColumnChunkMetaData.get(
      unshade(inner.getPath),
      unshade(inner.getPrimitiveType),
      unshade(inner.getCodec),
      unshade(inner.getEncodingStats),
      inner.getEncodings.asScala.map(unshade).asJava,
      unshade(inner.getStatistics),
      inner.getFirstDataPageOffset,
      inner.getDictionaryPageOffset,
      inner.getValueCount,
      inner.getTotalSize,
      inner.getTotalUncompressedSize)
  }

    def unshade(inner: ShadedBlockMetaData): BlockMetaData = {
      if (inner == null) {
        return null
      }
      val ret = new BlockMetaData
      ret.setRowCount(inner.getRowCount)
      ret.setTotalByteSize(inner.getTotalByteSize)
      ret.setPath(inner.getPath)
      ret.setOrdinal(inner.getOrdinal)
      ret.setRowIndexOffset(inner.getRowIndexOffset)

      inner.getColumns.asScala.foreach { column =>
        ret.addColumn(unshade(column))
      }

      ret
    }
}