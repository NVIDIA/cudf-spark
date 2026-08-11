/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Scalar, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuMetric}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import org.roaringbitmap.longlong.{PeekableLongIterator, Roaring64Bitmap}

import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}


object GpuDeltaParquetFileFormatUtils {
  /**
   * Row number of the row in the file. When used with [[FILE_PATH_COL]] together, it can be used
   * as unique id of a row in file. Currently to correctly calculate this, the caller needs to
   * set both [[isSplitable]] to false, and [[RapidsConf.PARQUET_READER_TYPE]] to "PERFILE".
   */
  val METADATA_ROW_IDX_COL: String = "__metadata_row_index"
  val METADATA_ROW_IDX_FIELD: StructField = StructField(METADATA_ROW_IDX_COL, LongType,
    nullable = false)

  val METADATA_ROW_DEL_COL: String = "__metadata_row_del"
  val METADATA_ROW_DEL_FIELD: StructField = StructField(METADATA_ROW_DEL_COL, BooleanType,
    nullable = false)


  /**
   * File path of the file that the row came from.
   */
  val FILE_PATH_COL: String = "_metadata_file_path"
  val FILE_PATH_FIELD: StructField = StructField(FILE_PATH_COL, StringType, nullable = false)

  /**
   * Add a metadata column to the iterator. Currently only support [[METADATA_ROW_IDX_COL]].
   */
  def addMetadataColumnToIterator(
      schema: StructType,
      delVector: Option[Roaring64Bitmap],
      input: Iterator[ColumnarBatch],
      maxBatchSize: Int,
      delVectorScatterTimeMetric: GpuMetric
  ): Iterator[ColumnarBatch] = {
    val metadataRowIndexCol = schema.fieldNames.indexOf(METADATA_ROW_IDX_COL)
    val delRowIdx = schema.fieldNames.indexOf(METADATA_ROW_DEL_COL)
    if (metadataRowIndexCol == -1 && delRowIdx == -1) {
      return input
    }
    var rowIndex = 0L
    input.map { batch =>
      withResource(batch) { _ =>
        val rowIdxCol = if (metadataRowIndexCol == -1) {
          None
        } else {
          Some(metadataRowIndexCol)
        }

        val delRowIdx2 = if (delRowIdx == -1) {
          None
        } else {
          Some(delRowIdx)
        }
        val newBatch = addMetadataColumns(rowIdxCol, delRowIdx2, delVector,maxBatchSize,
          rowIndex, batch, delVectorScatterTimeMetric)
        rowIndex += batch.numRows()
        newBatch
      }
    }
  }

  private def createFalseTable(numRows: Int): Table = {
    withResource(Scalar.fromBool(false)) { s =>
      withResource(CudfColumnVector.fromScalar(s, numRows)) { c =>
        new Table(c)
      }
    }
  }


  private def addMetadataColumns(
      rowIdxPos: Option[Int],
      delRowIdx: Option[Int],
      delVec: Option[Roaring64Bitmap],
      maxBatchSize: Int,
      rowIdxStart: Long,
      batch: ColumnarBatch,
      delVectorScatterTimeMetric: GpuMetric,
  ): ColumnarBatch = {
    val rowIdxCol = rowIdxPos.map { _ =>
      withResource(Scalar.fromLong(rowIdxStart)) { start =>
        GpuColumnVector.from(CudfColumnVector.sequence(start, batch.numRows()),
          METADATA_ROW_IDX_FIELD.dataType)
      }
    }

    closeOnExcept(rowIdxCol) { rowIdxCol =>

      val delVecCol = delVec.map { delVec =>
        delVectorScatterTimeMetric.ns {
          val table = new RoaringBitmapIterator(
            delVec.getLongIteratorFrom(rowIdxStart),
            rowIdxStart,
            rowIdxStart + batch.numRows())
            .grouped(Math.min(maxBatchSize, batch.numRows()))
            .foldLeft(createFalseTable(batch.numRows())){ (table, posChunk) =>
              withResource(table) { _ =>
                withResource(CudfColumnVector.fromLongs(posChunk: _*)) { poses =>
                  withResource(Scalar.fromBool(true)) { s =>
                    Table.scatter(Array(s), poses, table)
                  }
                }
              }
            }

              withResource(table) { _ =>
                GpuColumnVector.from(table.getColumn(0).incRefCount(),
                  METADATA_ROW_DEL_FIELD.dataType)
              }
        }
      }

      closeOnExcept(delVecCol) { delVecCol =>
        // Replace row_idx column
        val columns = new Array[ColumnVector](batch.numCols())
        for (i <- 0 until batch.numCols()) {
          if (rowIdxPos.contains(i)) {
            columns(i) = rowIdxCol.get
          } else if (delRowIdx.contains(i)) {
            columns(i) = delVecCol.get
          } else {
            columns(i) = batch.column(i) match {
              case gpuCol: GpuColumnVector => gpuCol.incRefCount()
              case col => col
            }
          }
        }

        new ColumnarBatch(columns, batch.numRows())
      }
    }
  }
}

class RoaringBitmapIterator(val inner: PeekableLongIterator, val start: Long, val end: Long)
  extends Iterator[Long] {

  override def hasNext: Boolean = {
    inner.hasNext && inner.peekNext() < end
  }

  override def next(): Long = {
    inner.next() - start
  }
}
