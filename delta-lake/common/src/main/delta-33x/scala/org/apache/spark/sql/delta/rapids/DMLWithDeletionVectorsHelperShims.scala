/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * This file was derived from DMLWithDeletionVectorsHelper.scala
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

package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids.delta.RapidsDeltaWrite
import com.nvidia.spark.rapids.delta.common.GpuDeltaParquetFileFormatBase2

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.commands.{DMLWithDeletionVectorsHelper, TouchedFileWithDV}
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelationWithTable}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.StructType

/** Version-specific ports of Delta's DMLWithDeletionVectorsHelper methods used by GPU DML. */
object DMLWithDeletionVectorsHelperShims {

  private val GpuFilePathColumnPrefix = "__delta_internal_gpu_file_path"

  def withGpuExecutionContext(spark: SparkSession, df: DataFrame): DataFrame = {
    Dataset.ofRows(spark, RapidsDeltaWrite(df.queryExecution.analyzed))
  }

  /**
   * GPU equivalent of Delta's createTargetDfForScanningForMatches. Both values of Delta's
   * useMetadataRowIndex setting use the internal physical row-index column here. Requesting the
   * hidden metadata struct would force the scan to CPU, while this column contains the same
   * per-file physical row positions needed to build deletion vectors.
   */
  def createTargetDfForGpuScanningForMatches(
      spark: SparkSession,
      target: LogicalPlan,
      fileIndex: TahoeFileIndex,
      candidateFilesHaveDVs: Boolean,
      reservedColumnNames: Seq[String] = Seq.empty): GpuTargetScan = {
    val resolver = spark.sessionState.conf.resolver
    val usedNames = target.output.map(_.name) ++ reservedColumnNames
    val filePathColumnName = Iterator.from(0).map { suffix =>
      if (suffix == 0) GpuFilePathColumnPrefix else s"${GpuFilePathColumnPrefix}_$suffix"
    }.find(name => !usedNames.exists(resolver(_, name))).get
    val rowIndexField = GpuDeltaParquetFileFormatBase2.GPU_ROW_INDEX_STRUCT_FIELD
    val rowIndexCol = AttributeReference(
      ROW_INDEX_COLUMN_NAME, rowIndexField.dataType, metadata = rowIndexField.metadata)()

    val newTarget = target.transformUp {
      case relation @ LogicalRelationWithTable(
          hfsr @ HadoopFsRelation(_, _, _, _, format: DeltaParquetFileFormat, _), _) =>
        val newDataSchema = StructType(hfsr.dataSchema).add(rowIndexField)
        val newFormat = if (candidateFilesHaveDVs) {
          format.copy(optimizationsEnabled = false)
        } else {
          format.copy(optimizationsEnabled = false, tablePath = None)
        }
        val newBaseRelation = hfsr.copy(
          location = fileIndex,
          dataSchema = newDataSchema,
          fileFormat = newFormat)(hfsr.sparkSession)
        relation.copy(relation = newBaseRelation, output = relation.output :+ rowIndexCol)
      case project @ Project(projectList, _) =>
        project.copy(projectList = projectList :+ rowIndexCol)
    }
    val targetDf = Dataset.ofRows(spark, newTarget)
      .withColumn(filePathColumnName, input_file_name())
    val filePathAttr = targetDf.queryExecution.analyzed.output
      .find(attr => resolver(attr.name, filePathColumnName)).get
    GpuTargetScan(targetDf, new Column(filePathAttr), new Column(rowIndexCol))
  }

  /** Port of Delta's processUnmodifiedData for Delta 3.3. */
  def processUnmodifiedData(
      spark: SparkSession,
      touchedFiles: Seq[TouchedFileWithDV],
      txn: GpuOptimisticTransactionBase): (Seq[FileAction], Map[String, Long]) = {
    DMLWithDeletionVectorsHelper.processUnmodifiedData(spark, touchedFiles, txn.snapshot)
  }
}
