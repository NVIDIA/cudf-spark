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

package com.nvidia.spark.rapids.delta.delta43x

import java.util.Locale

import com.nvidia.spark.rapids.RapidsMeta
import com.nvidia.spark.rapids.delta.DeltaConfigChecker
import org.apache.parquet.column.ParquetProperties.WriterVersion
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOptions, IcebergCompat,
  MaterializePartitionColumnsTableFeature, Snapshot}
import org.apache.spark.sql.delta.util.TableParquetVersionOption
import org.apache.spark.sql.internal.SQLConf

object Delta43xConfigChecker extends DeltaConfigChecker {
  override def checkIncompatibleConfs(
      meta: RapidsMeta[_, _, _],
      deltaLog: Option[DeltaLog],
      sqlConf: SQLConf,
      options: Map[String, String]): Unit = {
    val deltaOptions = new DeltaOptions(options, sqlConf)
    if (deltaOptions.useNullIntolerantEqualityWithDPO.isDefined) {
      meta.willNotWorkOnGpu(
        "Delta 4.3 null-intolerant dynamic partition overwrite is not supported on GPU")
    }
    if (deltaLog.exists(_.unsafeVolatileSnapshot.isCatalogOwned)) {
      meta.willNotWorkOnGpu("Delta 4.3 catalog-managed table writes are not supported on GPU")
    }
    deltaLog.map(_.unsafeVolatileSnapshot).foreach { snapshot =>
      if (DeltaConfigs.ENABLE_VARIANT_SHREDDING.fromMetaData(snapshot.metadata)) {
        meta.willNotWorkOnGpu("Delta 4.3 variant shredding writes are not supported on GPU")
      }
      if (snapshot.metadata.partitionColumns.nonEmpty &&
          (IcebergCompat.isAnyEnabled(snapshot.metadata) ||
            snapshot.protocol.isFeatureSupported(MaterializePartitionColumnsTableFeature))) {
        meta.willNotWorkOnGpu(
          "Delta 4.3 materialized partition column writes are not supported on GPU")
      }
    }
  }

  override def checkTableProperties(
      meta: RapidsMeta[_, _, _],
      spark: SparkSession,
      options: Map[String, String],
      snapshot: => Snapshot): Unit = {
    val current = snapshot
    val properties = if (current.version < 0) {
      // No table exists yet, so Delta creates it with these options and the session defaults.
      creationProperties(spark, current.metadata.configuration, options)
    } else {
      current.metadata.configuration
    }
    tagParquetFormatVersion(meta, spark, options, properties)
  }

  private def normalizeKey(properties: Map[String, String], key: String): Map[String, String] =
    properties.map {
      case (k, v) if k.equalsIgnoreCase(key) => key -> v
      case kv => kv
    }

  /**
   * The table properties a new table is created with: the explicit properties, the `delta.*`
   * write options that Delta persists as properties when a write creates the table, and the
   * session's table property defaults for whatever is still unset. Delta normalizes the case of
   * `delta.*` keys on its own, so the Parquet format version key is normalized before the
   * defaults are merged, or an explicit value would not replace a default spelled differently.
   */
  def creationProperties(
      spark: SparkSession,
      properties: Map[String, String],
      writeOptions: Map[String, String]): Map[String, String] = {
    val deltaOptions = writeOptions.filter { case (k, _) =>
      k.toLowerCase(Locale.ROOT).startsWith("delta.")
    }
    val explicit = normalizeKey(properties, DeltaConfigs.PARQUET_FORMAT_VERSION.key) ++
      normalizeKey(deltaOptions, DeltaConfigs.PARQUET_FORMAT_VERSION.key)
    DeltaConfigs.mergeGlobalConfigs(spark.sessionState.conf, explicit)
  }

  /**
   * `delta.parquet.format.version` selects the Parquet data page version via the writer version
   * Delta computes in `TableParquetVersionOption`. The GPU Parquet writer only emits DataPageV1,
   * so anything other than Parquet v1 must run on the CPU. Invalid values also fall back so the
   * CPU path raises Delta's own validation error. `effectiveProperties` are the properties of the
   * table being written, or for a table being created the result of [[creationProperties]].
   */
  def tagParquetFormatVersion(
      meta: RapidsMeta[_, _, _],
      spark: SparkSession,
      options: Map[String, String],
      effectiveProperties: Map[String, String]): Unit = {
    // Delta matches these keys exactly, but its catalog and commit path normalize their case.
    val writerVersionKey = ParquetOutputFormat.WRITER_VERSION
    val normalizedOptions = normalizeKey(options, writerVersionKey)
    val normalizedProperties =
      normalizeKey(effectiveProperties, DeltaConfigs.PARQUET_FORMAT_VERSION.key)
    try {
      val writerOptions = normalizedOptions ++ TableParquetVersionOption.getWriterOptions(
        spark, normalizedOptions, normalizedProperties)
      writerOptions.get(writerVersionKey).foreach { version =>
        if (WriterVersion.fromString(version) != WriterVersion.PARQUET_1_0) {
          meta.willNotWorkOnGpu(s"Parquet writer version '$version' (DataPageV2) is not " +
            "supported on GPU, which only writes DataPageV1")
        }
      }
    } catch {
      case e: IllegalArgumentException =>
        meta.willNotWorkOnGpu(s"invalid Parquet format version: ${e.getMessage}")
    }
  }
}
