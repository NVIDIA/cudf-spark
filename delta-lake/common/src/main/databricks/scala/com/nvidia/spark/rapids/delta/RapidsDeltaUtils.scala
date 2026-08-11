/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import com.databricks.sql.transaction.tahoe.{DeltaConfigs, DeltaLog, DeltaOptions, DeltaParquetFileFormat}
import com.nvidia.spark.rapids.{DeltaFormatType, FileFormatChecks, GpuOverrides, GpuParquetFileFormat, RapidsMeta, WriteFileOp}
import com.nvidia.spark.rapids.delta.shims.DeltaLogShim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object RapidsDeltaUtils {
  def tagForDeltaWrite(
      meta: RapidsMeta[_, _, _],
      schema: StructType,
      deltaLog: Option[DeltaLog],
      options: Map[String, String],
      spark: SparkSession): Unit = {
    FileFormatChecks.tag(meta, schema, DeltaFormatType, WriteFileOp)
    val format = deltaLog.map(log => DeltaLogShim.fileFormat(log).getClass)
      .getOrElse(classOf[DeltaParquetFileFormat])
    if (format == classOf[DeltaParquetFileFormat]) {
      GpuParquetFileFormat.tagGpuSupport(meta, spark, options, schema)
    } else {
      meta.willNotWorkOnGpu(s"file format $format is not supported")
    }
    checkIncompatibleConfs(meta, schema, deltaLog, spark.sessionState.conf, options)
  }

  private def checkIncompatibleConfs(
      meta: RapidsMeta[_, _, _],
      schema: StructType,
      deltaLog: Option[DeltaLog],
      sqlConf: SQLConf,
      options: Map[String, String]): Unit = {
    def getSQLConf(key: String): Option[String] = {
      try {
        Option(sqlConf.getConfString(key))
      } catch {
        case _: NoSuchElementException => None
      }
    }

    // Optimized writes for non-partitioned tables involves a round-robin partitioning, and that
    // can involve a sort on all columns. The GPU doesn't currently support sorting on all types,
    // so we fallback if the GPU cannot support the round-robin partitioning.
    if (sqlConf.sortBeforeRepartition) {
      val orderableTypeSig = GpuOverrides.pluginSupportedOrderableSig
      val unorderableTypes = schema.map(_.dataType).filterNot { t =>
        orderableTypeSig.isSupportedByPlugin(t)
      }
      if (unorderableTypes.nonEmpty) {
        val metadata = deltaLog.map(log => DeltaLogShim.getMetadata(log))
        val hasPartitioning = metadata.exists(_.partitionColumns.nonEmpty) ||
            options.get(DataSourceUtils.PARTITIONING_COLUMNS_KEY).exists(_.nonEmpty)
        if (!hasPartitioning) {
          val optimizeWriteEnabled = {
            val deltaOptions = new DeltaOptions(options, sqlConf)
            deltaOptions.optimizeWrite.orElse {
              getSQLConf("spark.databricks.delta.optimizeWrite.enabled").map(_.toBoolean).orElse {
                metadata.flatMap { m =>
                  DeltaConfigs.AUTO_OPTIMIZE.fromMetaData(m).orElse {
                    m.configuration.get("delta.autoOptimize.optimizeWrite").orElse {
                      getSQLConf(
                        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite")
                    }.map(_.toBoolean)
                  }
                }
              }
            }.getOrElse(false)
          }
          if (optimizeWriteEnabled) {
            unorderableTypes.foreach { t =>
              meta.willNotWorkOnGpu(s"round-robin partitioning cannot sort $t")
            }
          }
        }
      }
    }
  }

  def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean =
    spark.sessionState.conf
      .getConfString("deletionVectors.disableTightBoundOnFileCreationForDevOnly", "false")
      .toBoolean
}
