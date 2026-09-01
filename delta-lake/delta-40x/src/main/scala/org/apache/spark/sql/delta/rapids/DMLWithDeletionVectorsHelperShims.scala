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

import org.apache.spark.sql.{DataFrame, SparkSession => SqlSparkSession}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.classic.{Dataset, SparkSession}
import org.apache.spark.sql.delta.{DeltaParquetFileFormat, OptimisticTransaction}
import org.apache.spark.sql.delta.DeltaParquetFileFormat.{ROW_INDEX_COLUMN_NAME,
  ROW_INDEX_STRUCT_FIELD}
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.commands.{DMLWithDeletionVectorsHelper, TouchedFileWithDV}
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelationWithTable}
import org.apache.spark.sql.functions.{input_file_name, struct}
import org.apache.spark.sql.types.StructType

object DMLWithDeletionVectorsHelperShims {
  def withGpuExecutionContext(spark: SqlSparkSession, df: DataFrame): DataFrame = {
    val classicSpark = spark.asInstanceOf[SparkSession]
    Dataset.ofRows(classicSpark, RapidsDeltaWrite(df.queryExecution.logical))
  }

  def createTargetDfForGpuScanningForMatches(
      spark: SqlSparkSession,
      target: LogicalPlan,
      fileIndex: TahoeFileIndex): DataFrame = {
    val classicSpark = spark.asInstanceOf[SparkSession]
    val rowIndexCol =
      AttributeReference(ROW_INDEX_COLUMN_NAME, ROW_INDEX_STRUCT_FIELD.dataType)()

    val newTarget = target.transformUp {
      case l @ LogicalRelationWithTable(
          hfsr @ HadoopFsRelation(_, _, _, _, format: DeltaParquetFileFormat, _), _) =>
        val newDataSchema = StructType(hfsr.dataSchema).add(ROW_INDEX_STRUCT_FIELD)
        val newFormat = format.copy(optimizationsEnabled = false)
        val newBaseRelation = hfsr.copy(
          location = fileIndex,
          dataSchema = newDataSchema,
          fileFormat = newFormat)(hfsr.sparkSession)
        l.copy(relation = newBaseRelation, output = l.output :+ rowIndexCol)
      case p @ Project(projectList, _) =>
        p.copy(projectList = projectList :+ rowIndexCol)
    }
    Dataset.ofRows(classicSpark, newTarget)
      .withColumn("_metadata", struct(input_file_name().as("file_path")))
  }

  def processUnmodifiedData(
      spark: SparkSession,
      touchedFiles: Seq[TouchedFileWithDV],
      txn: OptimisticTransaction): (Seq[FileAction], Map[String, Long]) = {
    DMLWithDeletionVectorsHelper.processUnmodifiedData(spark, touchedFiles, txn.snapshot)
  }
}
