/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.shims.LogicalPlanShims
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{CalendarIntervalType, StructType}

case class GpuDataSource(
    sparkSession: SparkSession,
    className: String,
    paths: Seq[String] = Nil,
    userSpecifiedSchema: Option[StructType] = None,
    partitionColumns: Seq[String] = Seq.empty,
    bucketSpec: Option[BucketSpec] = None,
    options: Map[String, String] = Map.empty,
    catalogTable: Option[CatalogTable] = None,
    origProvider: Class[_])
      extends GpuDataSourceBase(sparkSession, className, paths, userSpecifiedSchema, 
      partitionColumns, bucketSpec, options, catalogTable, origProvider) {

  /**
   * Creates a command node to write the given [[LogicalPlan]] out to the given
   * [[FileFormat]].The returned command is unresolved and need to be analyzed.
   * This specifically is a CPU InsertIntoHadoopFsRelationCommand for Spark 3.4. This is because
   * in Spark 3.4, the command is executed within the context of the SparkSession, which
   * will then use the plugin to replace this with a GpuInsertIntoHadoopFsCommand.
   */
  private def planForWritingFileFormat(
      format: FileFormat,
      mode: SaveMode,
      data: LogicalPlan): InsertIntoHadoopFsRelationCommand = {
    // Don't glob path for the write path.  The contracts here are:
    //  1. Only one output path can be specified on the write path;
    //  2. Output path must be a legal HDFS style file system path;
    //  3. It's OK that the output path doesn't exist yet;
    val allPaths = paths ++ caseInsensitiveOptions.get("path")
    val outputPath = if (allPaths.length == 1) {
      val path = new Path(allPaths.head)
      val fs = path.getFileSystem(newHadoopConfiguration())
      path.makeQualified(fs.getUri, fs.getWorkingDirectory)
    } else {
      throw new IllegalArgumentException("Expected exactly one path to be specified, but " +
        s"got: ${allPaths.mkString(", ")}")
    }

    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    PartitioningUtils.validatePartitionColumn(data.schema, partitionColumns, caseSensitive)

    val fileIndex = catalogTable.map(_.identifier).map { tableIdent =>
      LogicalPlanShims.getLocations(sparkSession.table(tableIdent).queryExecution.analyzed).head
    }

    // For partitioned relation r, r.schema's column ordering can be different from the column
    // ordering of data.logicalPlan (partition columns are all moved after data column).  This
    // will be adjusted within InsertIntoHadoopFsRelation.
    InsertIntoHadoopFsRelationCommand(
      outputPath = outputPath,
      staticPartitions = Map.empty,
      ifPartitionNotExists = false,
      partitionColumns = partitionColumns.map(UnresolvedAttribute.quoted),
      bucketSpec = bucketSpec,
      fileFormat = format,
      options = options,
      query = data,
      mode = mode,
      catalogTable = catalogTable,
      fileIndex = fileIndex,
      outputColumnNames = data.output.map(_.name))
  }

  /**
   * Writes the given `LogicalPlan` out to this `DataSource` and returns a `BaseRelation` for
   * the following reading.
   *
   * @param mode The save mode for this writing.
   * @param data The input query plan that produces the data to be written. Note that this plan
   *             is analyzed and optimized.
   * @param outputColumnNames The original output column names of the input query plan. The
   *                          optimizer may not preserve the output column's names' case, so we need
   *                          this parameter instead of `data.output`.
   */
  def writeAndRead(
      mode: SaveMode,
      data: LogicalPlan,
      outputColumnNames: Seq[String]): BaseRelation = {

    val format = originalProvidingInstance()
    if (!format.isInstanceOf[FileFormat]) {
      throw new IllegalArgumentException(s"Original provider does not extend FileFormat: $format")
    }

    val outputColumns = DataWritingCommand.logicalPlanOutputWithNames(data, outputColumnNames)
    outputColumns.toStructType.foreach { field =>
      if (field.dataType.isInstanceOf[CalendarIntervalType]) {
        throw QueryCompilationErrors.dataTypeUnsupportedByDataSourceError(format.toString, field)
      }
    }

    val cmd = planForWritingFileFormat(format.asInstanceOf[FileFormat], mode, data)
    // Spark 3.4 doesn't need the child physical plan for metrics anymore, this is now
    // cleaned up, so we need to run the DataWritingCommand using SparkSession. This actually
    // calls the plugin transformation process again for a new physical plan with the
    // DataWritingCommandExec.
    val qe = sparkSession.sessionState.executePlan(cmd)
    qe.assertCommandExecuted()
  
    // Replace the schema with that of the DataFrame we just wrote out to avoid re-inferring
    copy(userSpecifiedSchema = Some(outputColumns.toStructType.asNullable)).resolveRelation()
  }

}
