/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark

import java.io.IOException

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.parquet.{ByteArrayInputFile, ParquetCachedBatch}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.{ParquetReadOptions, VersionParser}
import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.hadoop.ParquetFileReader

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetToSparkSchemaConverter, ParquetWriteSupport}
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class CurrentBatchIterator(
    val parquetCachedBatch: ParquetCachedBatch,
    conf: SQLConf,
    selectedAttributes: Seq[Attribute],
    options: ParquetReadOptions,
    hadoopConf: Configuration) extends Iterator[ColumnarBatch] with AutoCloseable {

  val capacity = conf.parquetVectorizedReaderBatchSize
  val vectors: Array[OffHeapColumnVector] =
    OffHeapColumnVector.allocateColumns(capacity, selectedAttributes.toStructType)
  val columnarBatch = new ColumnarBatch(vectors.asInstanceOf[Array[vectorized.ColumnVector]])
  var rowsReturned: Long = 0L
  var totalCountLoadedSoFar: Long = 0
  val parquetFileReader = {
    ParquetFileReader.open(new ByteArrayInputFile(parquetCachedBatch.buffer), options)
  }
  val writerVersion: ParsedVersion = try {
    VersionParser.parse(parquetFileReader.getFileMetaData.getCreatedBy)
  } catch {
    case _: Exception =>
      // If any problems occur trying to parse the writer version, fallback to sequential reads
      // if the column is a delta byte array encoding (due to PARQUET-246).
      null
  }
  val parquetToSparkSchemaConverter = new ParquetToSparkSchemaConverter(hadoopConf)
  // we are getting parquet schema and then converting it to catalyst schema
  // because catalyst schema that we get from Spark doesn't have the exact schema expected
  // by the columnar parquet reader
  val inMemCacheParquetSchema = parquetFileReader.getFooter.getFileMetaData.getSchema
  val inMemCacheSparkSchema = parquetToSparkSchemaConverter.convert(inMemCacheParquetSchema)

  val totalRowCount = parquetFileReader.getRowGroups.asScala.map(_.getRowCount).sum
  val inMemReqSparkSchema = StructType(selectedAttributes.toStructType.map { field =>
    inMemCacheSparkSchema.fields(inMemCacheSparkSchema.fieldIndex(field.name))
  })
  val reqSparkSchemaInCacheOrder = StructType(inMemCacheSparkSchema.filter(f =>
    inMemReqSparkSchema.fields.exists(f0 => f0.name.equals(f.name))))

  // There could be a case especially in a distributed environment where the requestedSchema
  // and cacheSchema are not in the same order. We need to create a map so we can guarantee
  // that we writing to the correct columnVector
  val cacheSchemaToReqSchemaMap: Map[Int, Int] =
  reqSparkSchemaInCacheOrder.indices.map { index =>
    index -> inMemReqSparkSchema.fields.indexOf(reqSparkSchemaInCacheOrder.fields(index))
  }.toMap

  val reqParquetSchemaInCacheOrder = new org.apache.parquet.schema.MessageType(
    inMemCacheParquetSchema.getName(), reqSparkSchemaInCacheOrder.fields.map { f =>
      inMemCacheParquetSchema.getFields().get(inMemCacheParquetSchema.getFieldIndex(f.name))
    }:_*)

  // reset spark schema calculated from parquet schema
  hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, inMemReqSparkSchema.json)
  hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, inMemReqSparkSchema.json)

  override def hasNext: Boolean = rowsReturned < totalRowCount

  override def next(): ColumnarBatch = {
    if (nextBatch) {
      // FYI, A very IMPORTANT thing to note is that we are returning the columnar batch
      // as-is i.e. this batch has NullTypes saved as IntegerTypes with null values. The
      // way Spark optimizes the read of NullTypes makes this work without having to rip out
      // the IntegerType column to be replaced by a NullType column. This could change in
      // future and will affect this code.
      columnarBatch
    } else {
      throw new NoSuchElementException("no elements found")
    }
  }

  onTaskCompletion(close())

  override def close(): Unit = {
    parquetFileReader.close()
  }

  @throws[IOException]
  def checkEndOfRowGroup(): Unit

  /**
   * Read the next RowGroup and read each column and return the columnarBatch
   */
  def nextBatch: Boolean
}
