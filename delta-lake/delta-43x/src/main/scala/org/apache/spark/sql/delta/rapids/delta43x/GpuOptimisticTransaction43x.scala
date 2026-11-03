/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from TransactionalWrite.scala in the
 * Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.rapids.delta43x

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.GpuStatisticsCollection

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, Snapshot}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.expressions.EncodeNestedVariantAsZ85String
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.rapids.GpuOptimisticTransaction
import org.apache.spark.sql.delta.util.TableParquetVersionOption
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.types.VariantType

/** Delta 4.3-specific GPU transaction write behavior. */
class GpuOptimisticTransaction43x(
    deltaLog: DeltaLog,
    catalogTable: Option[CatalogTable],
    snapshot: Option[Snapshot],
    rapidsConf: RapidsConf,
    autoCompactHook: PostCommitHook)
  extends GpuOptimisticTransaction(
    deltaLog, catalogTable, snapshot, rapidsConf, autoCompactHook) {

  override protected def getGpuStatsColExpr(
      statsDataSchema: Seq[Attribute],
      statsCollection: GpuStatisticsCollection): Expression = {
    val statsCollector = statsCollection.statsCollector.expr
    val statsJsonInput = encodeVariantStatsIfNeeded(statsDataSchema, statsCollector)
    val analyzedExpr = createDataFrameForStats(
      getActiveSparkSession,
      LocalRelation(statsDataSchema))
      .select(to_json(Column(statsJsonInput)))
      .queryExecution.analyzed.expressions.head
    postProcessStatsExpr(analyzedExpr)
  }

  private[rapids] def encodeVariantStatsIfNeeded(
      statsDataSchema: Seq[Attribute],
      statsCollector: Expression): Expression = {
    if (statsDataSchema.exists(_.dataType.existsRecursively(_ == VariantType))) {
      EncodeNestedVariantAsZ85String(statsCollector)
    } else {
      // EncodeNestedVariantAsZ85String uses CodegenFallback and recursively copies the entire
      // statistics struct. Avoid that per-row work when the data schema has no variant columns.
      statsCollector
    }
  }

  override protected def getWriterOptions(
      writeOptions: Option[DeltaOptions]): Map[String, String] = {
    val filteredOptions = writeOptions match {
      case None => Map.empty[String, String]
      case Some(options) =>
        options.options.filterKeys { key =>
          key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
              key.equalsIgnoreCase(DeltaOptions.COMPRESSION) ||
              key.equalsIgnoreCase(DeltaOptions.PARQUET_VERSION) ||
              key.equalsIgnoreCase(DeltaOptions.PARQUET_OUTPUT_TIMESTAMP_TYPE)
        }.toMap
    }

    filteredOptions ++ TableParquetVersionOption.getWriterOptions(
      spark = getActiveSparkSession,
      writerOptions = filteredOptions,
      tableProperties = metadata.configuration)
  }
}
