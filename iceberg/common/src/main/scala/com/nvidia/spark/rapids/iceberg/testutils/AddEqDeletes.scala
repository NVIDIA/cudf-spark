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

package com.nvidia.spark.rapids.iceberg.testutils

import java.io.File
import java.util.UUID

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.iceberg.parquet.IcebergPartitionedFile
import com.nvidia.spark.rapids.iceberg.testutils.AddEqDeletes.getParquetFileInfo
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg._
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.{GenericRecord, GpuFileHelpers, Record}
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.mapping.{MappedField, MappedFields, NameMapping}
import org.apache.iceberg.parquet.{Parquet, ParquetValueReader}
import org.apache.iceberg.shaded.org.apache.parquet.schema.{MessageType => ShadedMessageType}
import org.apache.iceberg.types.Types.{NestedField, StructType}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.api.java.UDF3

/** A spark sql udf to add eq-deletes to iceberg table from a parquet file.
 *
 * This udf is used in integration tests to help associate equality deletes to iceberg table. We
 * need this because in iceberg spark extension, when we do dml operations to merge on read
 * table, only position deletes are generated, and there is no builtin way to generate equation
 * deletes.
 */
class AddEqDeletes extends UDF3[String, String, String, Unit] with Logging {

  /**
   * This method is used to add eq-deletes to iceberg table from a parquet file.
   *
   * @param warehouse Hadoop warehouse path.
   * @param tableName Table identifier.
   * @param dataFile  Parquet file path with contains equation deletes and partition values.
   */
  override def call(warehouse: String, tableName: String, dataFile: String): Unit = {
    logDebug(s"Adding eq-deletes to $tableName from $dataFile")
    val catalog = new HadoopCatalog()
    catalog.setConf(new Configuration())
    catalog.initialize("spark_catalog", Map(CatalogProperties.WAREHOUSE_LOCATION -> warehouse)
      .asJava)

    val table = catalog.loadTable(TableIdentifier.parse(tableName))
    val parquetFileInfo = getParquetFileInfo(table, dataFile)
    logDebug("Parquet file info:\n" + parquetFileInfo)

    val parquetReader = {
      Parquet.read(table.io().newInputFile(dataFile))
        .project(parquetFileInfo.readSchema)
        .createReaderFunc(parquetFileInfo.createRecordReader(_))
        .withNameMapping(parquetFileInfo.nameMapping)
        .build()
    }

    withResource(parquetReader) { reader =>
      reader.asInstanceOf[java.lang.Iterable[GenericRecord]].asScala
        .map(r => {
          val deleteRecord = GenericRecord.create(parquetFileInfo.deleteSchema)
          parquetFileInfo.delColumnFieldsIndices.foreach(i => deleteRecord.set(i, r.get(i)))

          val partitionRecord = parquetFileInfo.partitionFieldIdx.
            map(r.get(_).asInstanceOf[StructLike])

          partitionRecord -> deleteRecord
        }).toSeq.groupBy(_._1)
        .mapValues(_.map(_._2))
        .map { case (partitionValue, deleteRecords) =>
          val outputFile = Files.localOutput(new File(
            s"${table.location()}/data",
            s"eq-deletes-${UUID.randomUUID()}.parquet"))

          GpuFileHelpers.writeDeleteFile(table,
            outputFile,
            partitionValue.orNull,
            deleteRecords.map(_.asInstanceOf[Record]).asJava,
            parquetFileInfo.deleteSchema)
        }.foldLeft(table.newRowDelta())(_.addDeletes(_))
        .commit()

      null
    }
  }
}

object AddEqDeletes extends Logging {
  private val PartitionColId = MetadataColumns.PARTITION_COLUMN_ID - 20000

  case class ParquetFileInfo(
      fileSchema: ShadedMessageType,
      deleteSchema: Schema,
      partitionSchema: Option[StructType]) {
    val readSchema: Schema = partitionSchema match {
      case Some(p) =>
        val fields = deleteSchema.columns().asScala :+ NestedField.required(
          PartitionColId, MetadataColumns.PARTITION_COLUMN_NAME, p)

        new Schema(fields.toArray[NestedField]: _*)
      case None => deleteSchema
    }

    val delColumnFieldsIndices: Seq[Int] = 0 until deleteSchema.columns().size()
    val partitionFieldIdx: Option[Int] = partitionSchema.map(_ => deleteSchema.columns().size())

    def createRecordReader(fileSchema: ShadedMessageType): ParquetValueReader[Record] =
      GenericParquetReaders.buildReader(readSchema, fileSchema)

    def nameMapping: NameMapping = {
      val deleteFieldsMapping = deleteSchema
        .columns()
        .asScala
        .map(f => MappedField.of(f.fieldId(), f.name()))

      val partitionFieldsMapping = partitionSchema
        .map(_.fields().asScala.map(f => MappedField.of(f.fieldId(), f.name())))
        .map(fields => MappedField.of(PartitionColId, MetadataColumns.PARTITION_COLUMN_NAME,
          MappedFields.of(fields.toSeq: _*)))

      val allMappedFields = deleteFieldsMapping ++ partitionFieldsMapping
      NameMapping.of(allMappedFields.toSeq: _*)
    }
  }

  def getParquetFileInfo(table: Table, parquetFile: String): ParquetFileInfo = {
    val icebergPartitionedFile = IcebergPartitionedFile(new IcebergInputFile(table.io.newInputFile
    (parquetFile)))

    val tableSchema = table.schema
    withResource(icebergPartitionedFile.newReader()) { reader =>
      logDebug(s"Reading eq delete parquet file $parquetFile, " +
        s"schema:\n${reader.getFileMetaData.getSchema}")
      val colNames = reader
        .getFileMetaData
        .getSchema
        .getColumns
        .asScala
        .map(_.getPath.head)
        .filterNot(MetadataColumns.isMetadataColumn)
        .toSeq

      val deleteSchema = tableSchema.select(colNames: _*)
      val partitionSchema = Option(table.spec()).map(_.partitionType())

      ParquetFileInfo(reader.getFileMetaData.getSchema,
        deleteSchema,
        partitionSchema)
    }
  }
}