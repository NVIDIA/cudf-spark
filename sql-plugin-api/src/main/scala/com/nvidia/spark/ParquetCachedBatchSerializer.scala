/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.ShimLoader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel


trait GpuCachedBatchSerializer extends CachedBatchSerializer {
  def gpuConvertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch]
}

/**
 * User facing wrapper class that calls into the internal version.
 */
class ParquetCachedBatchSerializer extends GpuCachedBatchSerializer {

  private lazy val realImpl = ShimLoader.newParquetCachedBatchSerializer()

  /**
   * Can `convertColumnarBatchToCachedBatch()` be called instead of
   * `convertInternalRowToCachedBatch()` for this given schema? True if it can and false if it
   * cannot. Columnar input is only supported if the plan could produce columnar output. Currently
   * this is mostly supported by input formats like parquet and orc, but more operations are likely
   * to be supported soon.
   * @param schema the schema of the data being stored.
   * @return True if columnar input can be supported, else false.
   */
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = {
    realImpl.supportsColumnarInput(schema)
  }

  /**
   * Convert an `RDD[InternalRow]` into an `RDD[CachedBatch]` in preparation for caching the data.
   * @param input the input `RDD` to be converted.
   * @param schema the schema of the data being stored.
   * @param storageLevel where the data will be stored.
   * @param conf the config for the query.
   * @return The data converted into a format more suitable for caching.
   */
  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    realImpl.convertInternalRowToCachedBatch(input, schema, storageLevel, conf)
  }

  /**
   * Convert an `RDD[ColumnarBatch]` into an `RDD[CachedBatch]` in preparation for caching the data.
   * This will only be called if `supportsColumnarInput()` returned true for the given schema and
   * the plan up to this point would could produce columnar output without modifying it.
   * @param input the input `RDD` to be converted.
   * @param schema the schema of the data being stored.
   * @param storageLevel where the data will be stored.
   * @param conf the config for the query.
   * @return The data converted into a format more suitable for caching.
   */
  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    realImpl.convertColumnarBatchToCachedBatch(input, schema, storageLevel, conf)
  }

  /**
   * Builds a function that can be used to filter batches prior to being decompressed.
   * In most cases extending [[org.apache.spark.sql.columnar.SimpleMetricsCachedBatchSerializer]]
   * will provide the filter logic necessary. You will need to provide metrics for this to work.
   * [[org.apache.spark.sql.columnar.SimpleMetricsCachedBatch]] provides the APIs to hold those
   * metrics and explains the metrics used, really just min and max.
   * Note that this is intended to skip batches that are not needed, and the actual filtering of
   * individual rows is handled later.
   * @param predicates the set of expressions to use for filtering.
   * @param cachedAttributes the schema/attributes of the data that is cached. This can be helpful
   *                         if you don't store it with the data.
   * @return a function that takes the partition id and the iterator of batches in the partition.
   *         It returns an iterator of batches that should be decompressed.
   */
  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    realImpl.buildFilter(predicates, cachedAttributes)
  }

  /**
   * Can `convertCachedBatchToColumnarBatch()` be called instead of
   * `convertCachedBatchToInternalRow()` for this given schema? True if it can and false if it
   * cannot. Columnar output is typically preferred because it is more efficient. Note that
   * `convertCachedBatchToInternalRow()` must always be supported as there are other checks that
   * can force row based output.
   * @param schema the schema of the data being checked.
   * @return true if columnar output should be used for this schema, else false.
   */
  override def supportsColumnarOutput(schema: StructType): Boolean = {
    realImpl.supportsColumnarOutput(schema)
  }

  /**
   * Convert the cached data into a ColumnarBatch. This currently is only used if
   * `supportsColumnarOutput()` returns true for the associated schema, but there are other checks
   * that can force row based output. One of the main advantages of doing columnar output over row
   * based output is that the code generation is more standard and can be combined with code
   * generation for downstream operations.
   * @param input the cached batches that should be converted.
   * @param cacheAttributes the attributes of the data in the batch.
   * @param selectedAttributes the fields that should be loaded from the data and the order they
   *                           should appear in the output batch.
   * @param conf the configuration for the job.
   * @return an RDD of the input cached batches transformed into the ColumnarBatch format.
   */
  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    realImpl.convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes,
      conf)
  }

  /**
   * Convert the cached batch into `InternalRow`s. If you want this to be performant, code
   * generation is advised.
   * @param input the cached batches that should be converted.
   * @param cacheAttributes the attributes of the data in the batch.
   * @param selectedAttributes the field that should be loaded from the data and the order they
   *                           should appear in the output rows.
   * @param conf the configuration for the job.
   * @return RDD of the rows that were stored in the cached batches.
   */
  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    realImpl.convertCachedBatchToInternalRow(input, cacheAttributes, selectedAttributes, conf)
  }

  /**
   * This method decodes the CachedBatch leaving it on the GPU to avoid the extra copying back to
   * the host
   *
   * @param input              the cached batches that should be converted.
   * @param cacheAttributes    the attributes of the data in the batch.
   * @param selectedAttributes the fields that should be loaded from the data and the order they
   *                           should appear in the output batch.
   * @param conf               the configuration for the job.
   * @return an RDD of the input cached batches transformed into the ColumnarBatch format.
   */
  override def gpuConvertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    realImpl.gpuConvertCachedBatchToColumnarBatch(input, cacheAttributes,
      selectedAttributes, conf)
  }

}
