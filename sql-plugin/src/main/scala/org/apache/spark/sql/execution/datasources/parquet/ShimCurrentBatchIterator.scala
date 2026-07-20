/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.IOException
import java.util

import scala.collection.JavaConverters._

import com.nvidia.spark.CurrentBatchIterator
import com.nvidia.spark.rapids.parquet.ParquetCachedBatch
import com.nvidia.spark.rapids.shims.LegacyBehaviorPolicyShim
import com.nvidia.spark.rapids.shims.parquet.ParquetTimestampNTZShims
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.schema.{GroupType, Type}

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object ParquetVectorizedReader {
  /**
   * We are getting this method using reflection because its a package-private
   */
  lazy val readBatchMethod = {
    val method = classOf[VectorizedColumnReader].getDeclaredMethod("readBatch", Integer.TYPE,
      classOf[WritableColumnVector],
      classOf[WritableColumnVector],
      classOf[WritableColumnVector])
    method.setAccessible(true)
    method
  }
}

/**
 * This class takes a lot of the logic from
 * org.apache/spark/sql/execution/datasources/parquet/VectorizedParquetRecordReader.java
 */
class ShimCurrentBatchIterator(
    parquetCachedBatch: ParquetCachedBatch,
    conf: SQLConf,
    selectedAttributes: Seq[Attribute],
    options: ParquetReadOptions,
    hadoopConf: Configuration)
    extends CurrentBatchIterator(
      parquetCachedBatch,
      conf,
      selectedAttributes,
      options,
      hadoopConf) {

  val missingColumns: util.Set[ParquetColumn] = new util.HashSet[ParquetColumn]()
  val config = new Configuration
  config.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, false)
  config.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, false)
  config.setBoolean(SQLConf.CASE_SENSITIVE.key, false)
  ParquetTimestampNTZShims.setupTimestampNTZConfig(config, conf)
  val parquetColumn = new ParquetToSparkSchemaConverter(config)
      .convertParquetColumn(reqParquetSchemaInCacheOrder, Option.empty)

  // initialize missingColumns to cover the case where requested column isn't present in the
  // cache, which should never happen but just in case it does
  for (column <- parquetColumn.children) {
    checkColumn(column)
  }

  val sparkSchema = parquetColumn.sparkType.asInstanceOf[StructType]
  val parquetColumnVectors = (for (i <- 0 until sparkSchema.fields.length) yield {
    ParquetCVShims.newParquetCV(sparkSchema, i, parquetColumn.children.apply(i),
      vectors(i), capacity, MemoryMode.OFF_HEAP, missingColumns, true)
  }).toArray

  private def containsPath(parquetType: Type, path: Array[String]): Boolean =
    containsPath(parquetType, path, 0)

  private def containsPath(parquetType: Type, path: Array[String], depth: Int): Boolean = {
    if (path.length == depth) return true
    if (parquetType.isInstanceOf[GroupType]) {
      val fieldName = path(depth)
      val parquetGroupType = parquetType.asInstanceOf[GroupType]
      if (parquetGroupType.containsField(fieldName)) {
        return containsPath(parquetGroupType.getType(fieldName), path, depth + 1)
      }
    }
    false
  }

  private def checkColumn(column: ParquetColumn): Unit = {
    val paths = column.path.toArray
    if (containsPath(inMemCacheParquetSchema, paths)) {
      if (column.isPrimitive) {
        val desc = column.descriptor.get
        val fd = inMemCacheParquetSchema.getColumnDescription(desc.getPath)
        if (!fd.equals(desc)) {
          throw new UnsupportedOperationException("Schema evolution not supported")
        }
      } else {
        for (childColumn <- column.children) {
          checkColumn(childColumn)
        }
      }
    } else {
      if (column.required) {
        if (column.required) {
          // Column is missing in data but the required data is non-nullable. This file is invalid.
          throw new IOException("Required column is missing in data file. Col: " + paths)
        }
        missingColumns.add(column);
      }
    }

  }

  @throws[IOException]
  private def initColumnReader(pages: PageReadStore, cv: ParquetColumnVector): Unit = {
    if (!missingColumns.contains(cv.getColumn)) {
      if (cv.getColumn.isPrimitive) {
        val column = cv.getColumn
        val reader = RapidsVectorizedColumnReader(
          column.descriptor.get,
          column.required,
          pages,
          null,
          LegacyBehaviorPolicyShim.CORRECTED_STR,
          LegacyBehaviorPolicyShim.EXCEPTION_STR,
          LegacyBehaviorPolicyShim.EXCEPTION_STR,
          null,
          writerVersion)
        cv.setColumnReader(reader)
      }
      else { // Not in missing columns and is a complex type: this must be a struct
        for (childCv <- cv.getChildren.asScala) {
          initColumnReader(pages, childCv)
        }
      }
    }
  }

  @throws[IOException]
  def checkEndOfRowGroup(): Unit = {
    if (rowsReturned != totalCountLoadedSoFar) return
    val pages = parquetFileReader.readNextRowGroup
    if (pages == null) {
      throw new IOException("expecting more rows but reached last" +
          " block. Read " + rowsReturned + " out of " + totalRowCount)
    }
    for (cv <- parquetColumnVectors) {
      initColumnReader(pages, cv)
    }
    totalCountLoadedSoFar += pages.getRowCount
  }

  /**
   * Read the next RowGroup and read each column and return the columnarBatch
   */
  def nextBatch: Boolean = {
    for (vector <- parquetColumnVectors) {
      vector.reset()
    }
    columnarBatch.setNumRows(0)
    if (rowsReturned >= totalRowCount) return false
    checkEndOfRowGroup()

    val num = Math.min(capacity.toLong, totalCountLoadedSoFar - rowsReturned).toInt
    for (cv <- parquetColumnVectors){
      for (leafCv <- cv.getLeaves.asScala) {
        val columnReader = leafCv.getColumnReader
        if (columnReader != null) {
          ParquetVectorizedReader.readBatchMethod.invoke(
            columnReader,
            num.asInstanceOf[AnyRef],
            leafCv.getValueVector.asInstanceOf[AnyRef],
            leafCv.getRepetitionLevelVector.asInstanceOf[AnyRef],
            leafCv.getDefinitionLevelVector.asInstanceOf[AnyRef])
        }
      }
      cv.assemble()
    }
    rowsReturned += num
    columnarBatch.setNumRows(num)
    true
  }
}
