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

package com.nvidia.spark.rapids.iceberg.data

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Table => CudfTable}
import com.nvidia.spark.rapids.{GpuColumnVector, LazySpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.iceberg.{IcebergFileIO, IcebergInputFile}
import com.nvidia.spark.rapids.iceberg.ShimUtils.locationOf
import com.nvidia.spark.rapids.iceberg.parquet._
import org.apache.iceberg.{DeleteFile, MetadataColumns, Schema}

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch


trait GpuDeleteLoader {
  def loadDeletes(deletes: Seq[DeleteFile],
      schema: Schema,
      sparkTypes: Array[DataType]): LazySpillableColumnarBatch
}

class DefaultDeleteLoader(
    private val rapidsFileIO: IcebergFileIO,
    private val inputFiles: Map[String, IcebergInputFile],
    private val parquetConf: GpuIcebergParquetReaderConf) extends GpuDeleteLoader {

  def loadDeletes(deletes: Seq[DeleteFile],
      schema: Schema,
      sparkTypes: Array[DataType]): LazySpillableColumnarBatch = {
    val files = deletes.map(f => IcebergPartitionedFile(inputFiles(locationOf(f))))
    withResource(createReader(schema, files)) { reader =>
      withResource(new ArrayBuffer[ColumnarBatch]()) { batches =>
        while (reader.hasNext) {
          batches += reader.next()
        }

        withResource(new ArrayBuffer[CudfTable](batches.size)) { tables =>
          batches.foreach { batch =>
            tables += GpuColumnVector.from(batch)
          }

          if (tables.size > 1) {
            withResource(CudfTable.concatenate(tables.toArray: _*)) { combined =>
              withResource(GpuColumnVector.from(combined, sparkTypes)) { combinedBatch =>
                LazySpillableColumnarBatch(combinedBatch, "Eq deletes")
              }
            }
          } else {
            withResource(GpuColumnVector.from(tables.head, sparkTypes)) { singleBatch =>
              LazySpillableColumnarBatch(singleBatch, "Eq deletes")
            }
          }
        }
      }
    }
  }

  private def createReader(schema: Schema,
      files: Seq[IcebergPartitionedFile]): GpuIcebergParquetReader = {
    val newConf = parquetConf.copy(
      expectedSchema = schema,
      threadConf = updateThreadConf(schema))
    newConf.threadConf match {
      case SingleFile =>
        new GpuSingleThreadIcebergParquetReader(
          rapidsFileIO,
          files,
          _ => Map.empty[Integer, Any].asJava,
          _ => None,
          newConf)
      case _: MultiThread =>
        new GpuMultiThreadIcebergParquetReader(
          rapidsFileIO,
          files,
          _ => Map.empty[Integer, Any].asJava,
          _ => None,
          newConf)
      case _: MultiFile =>
        new GpuCoalescingIcebergParquetReader(rapidsFileIO, files,
          _ => Map.empty[Integer, Any].asJava,
          newConf)
    }
  }

  private def updateThreadConf(schema: Schema): ThreadConf = {
    val hasFilePathMetadata = schema.findField(MetadataColumns.FILE_PATH.fieldId()) != null
    val hasRowPositionMetadata = schema.findField(MetadataColumns.ROW_POSITION.fieldId()) != null
    parquetConf.threadConf match {
      case threadConf: MultiThread =>
        threadConf.copy(
          disableCombining =
            hasFilePathMetadata || hasRowPositionMetadata,
          hasFilePathMetadata = hasFilePathMetadata,
          hasRowPositionMetadata = hasRowPositionMetadata)
      case threadConf: MultiFile =>
        threadConf.copy(
          hasFilePathMetadata = hasFilePathMetadata,
          hasRowPositionMetadata = hasRowPositionMetadata)
      case SingleFile => SingleFile
    }
  }
}