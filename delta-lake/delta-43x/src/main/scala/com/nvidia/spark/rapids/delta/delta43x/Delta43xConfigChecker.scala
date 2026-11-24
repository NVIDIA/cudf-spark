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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOptions, IcebergCompat,
  MaterializePartitionColumnsTableFeature, Snapshot}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
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
    if (current.version < 0) {
      // No table exists yet, so Delta creates it with these options and this session's defaults.
      // The empty snapshot's own configuration is left out: Delta caches it per path with the
      // defaults of whichever session first looked, which may differ from this session's. The
      // write's partitioning is not visible here, so a materialized partition column request is
      // treated as if the table were partitioned.
      tagCreationProperties(meta, spark, Map.empty, options, hasPartitionColumns = true)
    } else {
      tagParquetFormatVersion(meta, spark, options, current.metadata.configuration)
    }
  }

  /**
   * Tags a write that creates a table, using the properties Delta would create it with. This
   * covers the writer features that the GPU cannot write for such a table: variant shredding,
   * materialized partition columns, and a Parquet format version other than 1. Properties that
   * fail Delta's own validation fall back so the CPU raises the error, or does not if the write
   * turns out to target an existing table.
   */
  def tagCreationProperties(
      meta: RapidsMeta[_, _, _],
      spark: SparkSession,
      properties: Map[String, String],
      execWriteOptions: Map[String, String],
      hasPartitionColumns: Boolean): Unit = {
    // SQL `OPTIONS (...)` reach the exec only as properties with an "option." prefix, and Delta's
    // catalog uses them as the write options when the exec has none. They are write options, so
    // their keys match whatever their case, unlike the same key given as a table property.
    val writeOptions = if (execWriteOptions.isEmpty) {
      properties.collect {
        case (k, v) if k.startsWith("option.") => k.stripPrefix("option.") -> v
      }
    } else {
      execWriteOptions
    }
    val created = try {
      Right(creationProperties(spark, properties, writeOptions))
    } catch {
      case e @ (_: IllegalArgumentException | _: AnalysisException) => Left(e.getMessage)
    }
    created match {
      case Left(error) =>
        meta.willNotWorkOnGpu(s"invalid Delta table properties: $error")
      case Right(effectiveProperties) =>
        tagParquetFormatVersion(meta, spark, writeOptions, effectiveProperties)
        if (DeltaConfigs.ENABLE_VARIANT_SHREDDING.fromMap(effectiveProperties)) {
          meta.willNotWorkOnGpu("Delta 4.3 variant shredding writes are not supported on GPU")
        }
        val supportedFeatures =
          TableFeatureProtocolUtils.getSupportedFeaturesFromTableConfigs(effectiveProperties)
        val materializesPartitionColumns =
          IcebergCompat.isAnyEnabled(effectiveProperties) ||
            DeltaConfigs.ENABLE_MATERIALIZE_PARTITION_COLUMNS_FEATURE
              .fromMap(effectiveProperties).contains(true) ||
            supportedFeatures.contains(MaterializePartitionColumnsTableFeature)
        if (hasPartitionColumns && materializesPartitionColumns) {
          meta.willNotWorkOnGpu(
            "Delta 4.3 materialized partition column writes are not supported on GPU")
        }
    }
  }

  private val V1_SAVE_AS_TABLE_OVERWRITE_OPTION = "__v1_save_as_table_overwrite"
  private val CLASSIC_DATAFRAME_WRITER = "org.apache.spark.sql.classic.DataFrameWriter."

  /**
   * Whether a replace is DataFrameWriter V1 `saveAsTable` in overwrite mode, for which Delta
   * keeps the existing table's metadata instead of replacing it. Delta marks it with an internal
   * write option, except on Spark 4.0 where it inspects the calling stack, so this does too.
   */
  def isV1SaveAsTableOverwrite(writeOptions: Map[String, String]): Boolean =
    writeOptions.exists { case (k, v) =>
      k.equalsIgnoreCase(V1_SAVE_AS_TABLE_OVERWRITE_OPTION) && v.equalsIgnoreCase("true")
    } || Thread.currentThread().getStackTrace.exists(_.toString.contains(CLASSIC_DATAFRAME_WRITER))

  /**
   * Tags a replace that keeps the existing table's metadata: the existing properties apply unless
   * this write sets them explicitly. Session defaults do not apply to an existing table.
   */
  def tagRetainedProperties(
      meta: RapidsMeta[_, _, _],
      spark: SparkSession,
      existingProperties: Map[String, String],
      properties: Map[String, String],
      writeOptions: Map[String, String]): Unit = {
    val explicit = normalizeDeltaKeys(properties) ++ normalizeDeltaKeys(
      writeOptions.filter { case (k, _) => k.toLowerCase(Locale.ROOT).startsWith("delta.") })
    tagParquetFormatVersion(meta, spark, writeOptions,
      normalizeDeltaKeys(existingProperties) ++ explicit)
  }

  private def normalizeKey(properties: Map[String, String], key: String): Map[String, String] =
    properties.map {
      case (k, v) if k.equalsIgnoreCase(key) => key -> v
      case kv => kv
    }

  // Delta accepts `delta.*` keys in any case and stores them under the canonical spelling.
  private def normalizeDeltaKeys(properties: Map[String, String]): Map[String, String] =
    properties.map { case (k, v) =>
      DeltaConfigs.normalizeConfigKey(Some(k)).getOrElse(k) -> v
    }

  /**
   * The table properties a new table is created with: the explicit properties, the `delta.*`
   * write options that Delta persists as properties when a write creates the table, and the
   * session's table property defaults for whatever is still unset. Delta normalizes the case of
   * `delta.*` keys on its own, so all of them are normalized before the defaults are merged, or
   * an explicit value would not replace a default spelled differently and mixed-case keys would
   * be missed by the fallback checks that read these properties.
   */
  def creationProperties(
      spark: SparkSession,
      properties: Map[String, String],
      writeOptions: Map[String, String]): Map[String, String] = {
    val deltaOptions = writeOptions.filter { case (k, _) =>
      k.toLowerCase(Locale.ROOT).startsWith("delta.")
    }
    val explicit = normalizeDeltaKeys(properties) ++ normalizeDeltaKeys(deltaOptions)
    DeltaConfigs.mergeGlobalConfigs(spark.sessionState.conf, explicit)
  }

  /**
   * The Parquet writer version requested by resolved writer options, when it is one the GPU
   * cannot write: the GPU Parquet writer only emits DataPageV1. Throws IllegalArgumentException
   * for a value that is not a Parquet writer version at all.
   */
  def unsupportedWriterVersion(writerOptions: Map[String, String]): Option[String] =
    writerOptions.get(ParquetOutputFormat.WRITER_VERSION)
      .filter(WriterVersion.fromString(_) != WriterVersion.PARQUET_1_0)

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
    // Delta normalizes the case of `delta.*` keys itself, but it matches `parquet.writer.version`
    // exactly in both options and properties, so another spelling of it is ignored and the CPU
    // writes Parquet v1. A write option in another case is still treated as the real option here:
    // falling back for a spelling that Delta may start to accept is cheaper than guessing.
    val writerVersionKey = ParquetOutputFormat.WRITER_VERSION
    val normalizedOptions = normalizeKey(options, writerVersionKey)
    val normalizedProperties =
      normalizeKey(effectiveProperties, DeltaConfigs.PARQUET_FORMAT_VERSION.key)
    try {
      val writerOptions = normalizedOptions ++ TableParquetVersionOption.getWriterOptions(
        spark, normalizedOptions, normalizedProperties)
      unsupportedWriterVersion(writerOptions).foreach { version =>
        meta.willNotWorkOnGpu(s"Parquet writer version '$version' (DataPageV2) is not " +
          "supported on GPU, which only writes DataPageV1")
      }
    } catch {
      case e: IllegalArgumentException =>
        meta.willNotWorkOnGpu(s"invalid Parquet format version: ${e.getMessage}")
    }
  }
}
