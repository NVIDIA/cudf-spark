/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Portions of this class have been taken from DeltaTableV2 class
 * in the Delta Lake project at https://github.com/delta-io/delta.
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

package com.nvidia.spark.rapids.delta

import com.databricks.sql.execution.metric.IncrementMetric
import com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.{RapidsConf, RapidsMeta, SparkPlanMeta}

import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec}

/** Reader eligibility checks shared by file-format tagging and provider selection. */
object DeltaReadSupport {
  private val LOW_SHUFFLE_MERGE_OPTION_PREFIX =
    "spark.rapids.internal.delta.lowShuffleMerge."

  val LOW_SHUFFLE_MERGE_REWRITE_SCAN_OPTION =
    LOW_SHUFFLE_MERGE_OPTION_PREFIX + "rewriteScan"
  val LOW_SHUFFLE_MERGE_DISCOVERY_SCAN_OPTION =
    LOW_SHUFFLE_MERGE_OPTION_PREFIX + "discoveryScan"
  val LOW_SHUFFLE_MERGE_ROW_INDEX_COLUMN_OPTION =
    LOW_SHUFFLE_MERGE_OPTION_PREFIX + "rowIndexColumn"

  def isPushDVPredicateDownEnabled(conf: RapidsConf): Boolean = {
    val dvConf = DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX
    val useMetadataRowIndex = conf.getStr(dvConf.key)
      .getOrElse(dvConf.defaultValueString).toBoolean
    // Delta deletion-vector filtering is row-position based in both DBR modes. The native GPU
    // pushdown below only handles the DBR metadata-row-index plan shape, where Spark provides the
    // temporary row-index metadata column that we can prune after moving DV filtering into cuDF.
    useMetadataRowIndex && conf.isDeltaDeletionVectorPredicatePushdownEnabled
  }

  def isDVScan(meta: SparkPlanMeta[FileSourceScanExec]): Boolean = {
    val maybeDVScan = meta.parent.flatMap(_.parent).flatMap(_.parent).map(_.wrapped)
    maybeDVScan.exists {
      case ProjectExec(outputList, FilterExec(condition, ProjectExec(inputList, _))) =>
        condition.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name)) &&
          inputList.exists(_.name == "_metadata") &&
          !outputList.flatMap(_.references).exists(_.name == "_metadata")
      case _ => false
    }
  }

  def isDmlMetricDVScan(meta: SparkPlanMeta[FileSourceScanExec]): Boolean = {
    def hasMetricSkipRowFilter(parent: Option[RapidsMeta[_, _, _]]): Boolean = {
      parent.exists { p =>
        p.wrapped match {
          case FilterExec(condition, _) =>
            condition.references.exists(ref => isDeletionVectorSkipRowColumn(ref.name)) &&
              condition.exists(_.isInstanceOf[IncrementMetric])
          case _ =>
            hasMetricSkipRowFilter(p.parent)
        }
      }
    }

    hasMetricSkipRowFilter(meta.parent)
  }

  private[delta] def isDeletionVectorSkipRowColumn(name: String): Boolean =
    name == DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME ||
      name == GpuDeltaParquetFileFormat.EDGE_COMPUTED_COLUMN_SKIP_ROW
}
