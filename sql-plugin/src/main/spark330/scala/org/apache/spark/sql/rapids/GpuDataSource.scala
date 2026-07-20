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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.ColumnarFileFormat
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources._
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
    origProvider: Class[_],
    gpuFileFormat: ColumnarFileFormat) 
      extends GpuDataSourceBase(sparkSession, className, paths, userSpecifiedSchema, 
      partitionColumns, bucketSpec, options, catalogTable, origProvider) {

  /**
   * Creates a command node to write the given [[LogicalPlan]] out to the given [[FileFormat]].
   * The returned command is unresolved and need to be analyzed.
   */
  private def planForWritingFileFormat(
      format: ColumnarFileFormat,
      mode: SaveMode,
      data: LogicalPlan, useStableSort: Boolean,
      concurrentWriterPartitionFlushSize: Long,
      baseDebugOutputPath: Option[String]): GpuInsertIntoHadoopFsRelationCommand = {
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
      sparkSession.table(tableIdent).queryExecution.analyzed.collect {
        case LogicalRelation(t: HadoopFsRelation, _, _, _) => t.location
      }.head
    }
    // For partitioned relation r, r.schema's column ordering can be different from the column
    // ordering of data.logicalPlan (partition columns are all moved after data column).  This
    // will be adjusted within InsertIntoHadoopFsRelation.
    GpuInsertIntoHadoopFsRelationCommand(
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
      outputColumnNames = data.output.map(_.name),
      useStableSort = useStableSort,
      concurrentWriterPartitionFlushSize = concurrentWriterPartitionFlushSize,
      baseDebugOutputPath = baseDebugOutputPath)
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
   * @param physicalPlan The physical plan of the input query plan. We should run the writing
   *                     command with this physical plan instead of creating a new physical plan,
   *                     so that the metrics can be correctly linked to the given physical plan and
   *                     shown in the web UI.
   */
  def writeAndRead(
      mode: SaveMode,
      data: LogicalPlan,
      outputColumnNames: Seq[String],
      physicalPlan: SparkPlan,
      useStableSort: Boolean,
      concurrentWriterPartitionFlushSize: Long,
      baseDebugOutputPath: Option[String]): BaseRelation = {
    val outputColumns = DataWritingCommand.logicalPlanOutputWithNames(data, outputColumnNames)
    if (outputColumns.map(_.dataType).exists(_.isInstanceOf[CalendarIntervalType])) {
      throw new AnalysisException("Cannot save interval data type into external storage.")
    }

    // Only currently support ColumnarFileFormat
    val cmd = planForWritingFileFormat(gpuFileFormat, mode, data, useStableSort,
      concurrentWriterPartitionFlushSize, baseDebugOutputPath)
    val resolvedPartCols = cmd.partitionColumns.map { col =>
      // The partition columns created in `planForWritingFileFormat` should always be
      // `UnresolvedAttribute` with a single name part.
      assert(col.isInstanceOf[UnresolvedAttribute])
      val unresolved = col.asInstanceOf[UnresolvedAttribute]
      assert(unresolved.nameParts.length == 1)
      val name = unresolved.nameParts.head
      outputColumns.find(a => equality(a.name, name)).getOrElse {
        throw new AnalysisException(
          s"Unable to resolve $name given [${data.output.map(_.name).mkString(", ")}]")
      }
    }
    val resolved = cmd.copy(
      partitionColumns = resolvedPartCols,
      outputColumnNames = outputColumnNames)
    resolved.runColumnar(sparkSession, physicalPlan)
    // Replace the schema with that of the DataFrame we just wrote out to avoid re-inferring
    copy(userSpecifiedSchema = Some(outputColumns.toStructType.asNullable)).resolveRelation()
  }

}
