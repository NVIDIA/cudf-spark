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

/*** spark-rapids-shim-json-lines
{"spark":["405","421"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.shims.GpuFileWriteJobShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{
  FileFormatWriter,
  InsertIntoHadoopFsRelationCommand,
  SQLHadoopMapReduceCommitProtocol
}
import org.apache.spark.sql.rapids.{GpuFileFormatWriter, GpuInsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.types.StructType

class GpuFileFormatWriter405Suite extends SparkQueryCompareTestSuite {
  private class PrepareWriteFailure extends RuntimeException

  private class GpuPrepareWriteFailureFormat extends GpuParquetFileFormat {
    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        dataSchema: StructType): ColumnarOutputWriterFactory = {
      throw new PrepareWriteFailure
    }
  }

  test("beforeSetupJob is not called when prepareWrite fails") {
    withGpuSparkSession { spark =>
      var callbackCalled = false

      withTempPath { outputPath =>
        val plan = spark.range(1).queryExecution.executedPlan
        intercept[PrepareWriteFailure] {
          GpuFileFormatWriter.write(
            sparkSession = spark,
            plan = plan,
            fileFormat = new GpuPrepareWriteFailureFormat,
            committer = new SQLHadoopMapReduceCommitProtocol(
              "prepare-write-failure", outputPath.getAbsolutePath, false),
            outputSpec = FileFormatWriter.OutputSpec(
              outputPath.getAbsolutePath, Map.empty, plan.output),
            hadoopConf = new Configuration(false),
            partitionColumns = Seq.empty,
            bucketSpec = None,
            statsTrackers = Seq.empty,
            options = Map.empty,
            useStableSort = false,
            concurrentWriterPartitionFlushSize = 0L,
            baseDebugOutputPath = None,
            beforeSetupJob = () => callbackCalled = true)
        }
      }

      assert(!callbackCalled)
    }
  }

  test("static overwrite preserves existing data when GPU prepareWrite fails") {
    withGpuSparkSession { spark =>
      withTempPath { outputPath =>
        spark.range(3).write.parquet(outputPath.getAbsolutePath)
        val input = spark.range(5)
        val command = GpuInsertIntoHadoopFsRelationCommand(
          outputPath = new Path(outputPath.toURI),
          staticPartitions = Map.empty,
          ifPartitionNotExists = false,
          partitionColumns = Seq.empty,
          bucketSpec = None,
          fileFormat = new GpuPrepareWriteFailureFormat,
          options = Map.empty,
          query = input.queryExecution.analyzed,
          mode = SaveMode.Overwrite,
          catalogTable = None,
          fileIndex = None,
          outputColumnNames = input.columns.toSeq,
          useStableSort = false,
          concurrentWriterPartitionFlushSize = 0L,
          baseDebugOutputPath = None)

        intercept[PrepareWriteFailure] {
          command.runColumnar(spark, input.queryExecution.executedPlan)
        }

        assert(spark.read.parquet(outputPath.getAbsolutePath)
          .orderBy("id").collect().map(_.getLong(0)).toSeq === Seq(0L, 1L, 2L))
      }
    }
  }

  test("saveAsTable overwrite preserves existing table when prepareWrite fails") {
    withGpuSparkSession { spark =>
      object PlanWriteWithPrepareFailure extends SparkStrategy {
        override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
          case command: InsertIntoHadoopFsRelationCommand
              if command.getTagValue(
                InsertIntoHadoopFsRelationCommand.BEFORE_WRITE_JOB).isDefined =>
            val replacement = GpuInsertIntoHadoopFsRelationCommand(
              command.outputPath,
              command.staticPartitions,
              command.ifPartitionNotExists,
              command.partitionColumns,
              command.bucketSpec,
              new GpuPrepareWriteFailureFormat,
              command.options,
              command.query,
              command.mode,
              command.catalogTable,
              command.fileIndex,
              command.outputColumnNames,
              useStableSort = false,
              concurrentWriterPartitionFlushSize = 0L,
              baseDebugOutputPath = None)
            GpuFileWriteJobShim.copyBeforeWriteJob(command, replacement)
            GpuDataWritingCommandExec(replacement, planLater(command.query)) :: Nil
          case _ => Nil
        }
      }

      val tableName = "gpu_file_format_writer_405_prepare_write_failure"
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      try {
        spark.range(3).write.saveAsTable(tableName)
        val originalStrategies = spark.experimental.extraStrategies
        try {
          spark.experimental.extraStrategies = PlanWriteWithPrepareFailure +: originalStrategies
          withSQLConf("spark.sql.optimizer.plannedWrite.enabled" -> "false") {
            intercept[PrepareWriteFailure] {
              spark.range(5).write.mode(SaveMode.Overwrite).saveAsTable(tableName)
            }
          }
        } finally {
          spark.experimental.extraStrategies = originalStrategies
        }

        assert(spark.table(tableName).orderBy("id")
          .collect().map(_.getLong(0)).toSeq === Seq(0L, 1L, 2L))
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }

  test("beforeWrite runs when a strategy replaces the tagged write command") {
    withGpuSparkSession { spark =>
      object PlanWriteWithoutTags extends SparkStrategy {
        override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
          case command: InsertIntoHadoopFsRelationCommand
              if command.getTagValue(
                InsertIntoHadoopFsRelationCommand.BEFORE_WRITE_JOB).isDefined =>
            DataWritingCommandExec(command.copy(), planLater(command.query)) :: Nil
          case _ => Nil
        }
      }

      val tableName = "gpu_file_format_writer_405_replaced_command"
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      try {
        spark.range(3).write.saveAsTable(tableName)
        val originalStrategies = spark.experimental.extraStrategies
        try {
          spark.experimental.extraStrategies = PlanWriteWithoutTags +: originalStrategies
          spark.range(5, 7).write.mode(SaveMode.Overwrite).saveAsTable(tableName)
        } finally {
          spark.experimental.extraStrategies = originalStrategies
        }

        assert(spark.table(tableName).collect().map(_.getLong(0)).toSeq === Seq(5L, 6L))
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }
}
