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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.DeltaParquetFileFormat
import com.databricks.sql.transaction.tahoe.files.TahoeFileIndex
import com.nvidia.spark.rapids.delta.{GpuDeltaParquetFileFormatBase, RapidsDeltaWrite}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.types.StructType

private[rapids] case class GpuTargetScan(
    dataFrame: DataFrame,
    filePathColumn: Column,
    rowIndexColumn: Column)

/** DBR 17.3 adapters for the target scan used by persistent-DV DML. */
private[rapids] object DMLWithDeletionVectorsHelperShims {
  private val GpuFilePathColumnPrefix = "__delta_internal_gpu_file_path"

  def withGpuExecutionContext(spark: SparkSession, df: DataFrame): DataFrame = {
    TrampolineConnectShims.createDataFrame(
      spark.asInstanceOf[TrampolineConnectShims.SparkSession],
      RapidsDeltaWrite(df.queryExecution.analyzed))
  }

  /**
   * Builds a target scan containing the per-file physical row position used to create DVs.
   *
   * Unlike the OSS implementation, DBR native DV reads keep scan optimizations enabled and retain
   * the table path. Both are required for DBR to provide row-index filters to the native reader.
   */
  def createTargetDfForGpuScanningForMatches(
      spark: SparkSession,
      target: LogicalPlan,
      fileIndex: TahoeFileIndex,
      reservedColumnNames: Seq[String] = Seq.empty): GpuTargetScan = {
    val resolver = spark.sessionState.conf.resolver
    val usedNames = target.output.map(_.name) ++ reservedColumnNames
    val filePathColumnName = Iterator.from(0).map { suffix =>
      if (suffix == 0) GpuFilePathColumnPrefix else s"${GpuFilePathColumnPrefix}_${suffix}"
    }.find(name => !usedNames.exists(resolver(_, name))).get
    // Preserve DBR's private metadata-column contract, then add the neutral RAPIDS marker used by
    // the GPU reader to retain the physical row index through schema evolution.
    val rowIndexField = GpuDeltaParquetFileFormatBase.markGpuRowIndexColumn(
      DeltaParquetFileFormat.ROW_INDEX_STRUCT_FIELD)
    val rowIndexCol = AttributeReference(
      rowIndexField.name, rowIndexField.dataType, metadata = rowIndexField.metadata)()

    val newTarget = target.transformUp {
      case relation: LogicalRelation
          if relation.relation.isInstanceOf[HadoopFsRelation] &&
            relation.relation.asInstanceOf[HadoopFsRelation].fileFormat
              .isInstanceOf[DeltaParquetFileFormat] =>
        val hfsr = relation.relation.asInstanceOf[HadoopFsRelation]
        val format = hfsr.fileFormat.asInstanceOf[DeltaParquetFileFormat]
        val newDataSchema = StructType(hfsr.dataSchema).add(rowIndexField)
        val newBaseRelation = hfsr.copy(
          location = fileIndex,
          dataSchema = newDataSchema,
          fileFormat = format.copyWithDVInfo(fileIndex.path.toString, true))(hfsr.sparkSession)
        relation.copy(relation = newBaseRelation, output = relation.output :+ rowIndexCol)
      case project @ Project(projectList, _) =>
        project.copy(projectList = projectList :+ rowIndexCol)
    }
    val targetDf = TrampolineConnectShims.createDataFrame(
      spark.asInstanceOf[TrampolineConnectShims.SparkSession], newTarget)
      .withColumn(filePathColumnName, input_file_name())
    val filePathAttr = targetDf.queryExecution.analyzed.output
      .find(attr => resolver(attr.name, filePathColumnName)).get
    GpuTargetScan(
      targetDf,
      org.apache.spark.sql.nvidia.DFUDFShims.exprToColumn(filePathAttr),
      org.apache.spark.sql.nvidia.DFUDFShims.exprToColumn(rowIndexCol))
  }
}
