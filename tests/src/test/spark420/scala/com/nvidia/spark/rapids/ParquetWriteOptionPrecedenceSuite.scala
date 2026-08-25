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
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FileFormatWriter, SQLHadoopMapReduceCommitProtocol}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{ExecutionPlanCaptureCallback, GpuFileFormatWriter}
import org.apache.spark.sql.types.StructType

@scala.annotation.nowarn("msg=method readFooter in class ParquetFileReader is deprecated")
class ParquetWriteOptionPrecedenceSuite extends SparkQueryCompareTestSuite {
  private class PrepareWriteReached extends RuntimeException

  test("GpuFileFormatWriter merges Parquet options before prepareWrite") {
    val sparkConf = new SparkConf()
      .set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "true")
      .set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "INT96")
      .set(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key, "true")
      .set(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key, "true")
      .set(SQLConf.PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE.key, "true")

    withGpuSparkSession(spark => {
      val writeOptions = Map(
        SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "false",
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS",
        SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key -> "false",
        SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key -> "false",
        SQLConf.PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE.key -> "false")

      var preparedConf: Option[Configuration] = None
      val fileFormat = new GpuParquetFileFormat() {
        override def prepareWrite(
            sparkSession: SparkSession,
            job: Job,
            options: Map[String, String],
            dataSchema: StructType): ColumnarOutputWriterFactory = {
          super.prepareWrite(sparkSession, job, options, dataSchema)
          preparedConf = Some(new Configuration(job.getConfiguration))
          throw new PrepareWriteReached
        }
      }

      withTempPath { outputPath =>
        val plan = spark.range(1).queryExecution.executedPlan
        intercept[PrepareWriteReached] {
          GpuFileFormatWriter.write(
            sparkSession = spark,
            plan = plan,
            fileFormat = fileFormat,
            committer = new SQLHadoopMapReduceCommitProtocol(
              "write-option-precedence", outputPath.getAbsolutePath, false),
            outputSpec = FileFormatWriter.OutputSpec(
              outputPath.getAbsolutePath, Map.empty, plan.output),
            hadoopConf = new Configuration(false),
            partitionColumns = Seq.empty,
            bucketSpec = None,
            statsTrackers = Seq.empty,
            options = writeOptions,
            useStableSort = false,
            concurrentWriterPartitionFlushSize = 0L,
            baseDebugOutputPath = None)
        }
      }

      val conf = preparedConf.getOrElse(fail("GpuParquetFileFormat.prepareWrite was not reached"))
      assert(!conf.getBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, true))
      assert(conf.get(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key) === "TIMESTAMP_MICROS")
      assert(!conf.getBoolean(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key, true))
      assert(!conf.getBoolean(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key, true))
      assert(!conf.getBoolean(SQLConf.PARQUET_ANNOTATE_VARIANT_LOGICAL_TYPE.key, true))
    }, sparkConf)
  }

  test("per-write timestamp option takes precedence over session config") {
    val sparkConf = new SparkConf()
      .set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "INT96")

    withGpuSparkSession(spark => {
      import spark.implicits._

      withTempPath { outputPath =>
        ExecutionPlanCaptureCallback.startCapture()
        Seq(Timestamp.valueOf("2024-01-01 12:00:00"))
          .toDF("ts")
          .coalesce(1)
          .write
          .option(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "TIMESTAMP_MICROS")
          .parquet(outputPath.getAbsolutePath)

        val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
        assert(plans.nonEmpty, "Did not capture GPU write plan")
        ExecutionPlanCaptureCallback.assertContains(plans.head, "GpuDataWritingCommandExec")

        val parquetFile = outputPath.listFiles()
          .find(file => file.getName.startsWith("part-") && file.getName.endsWith(".parquet"))
          .getOrElse(fail("Expected a Parquet data file"))
        val footer = ParquetFileReader.readFooter(
          spark.sparkContext.hadoopConfiguration,
          new Path(parquetFile.getAbsolutePath))
        val timestampField = footer.getFileMetaData.getSchema.getFields.get(0).asPrimitiveType()
        assert(timestampField.getPrimitiveTypeName === PrimitiveTypeName.INT64)
      }
    }, sparkConf)
  }

  test("per-write legacy format option prevents session-config fallback") {
    val sparkConf = new SparkConf()
      .set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "true")

    withGpuSparkSession(spark => {
      withTempPath { outputPath =>
        ExecutionPlanCaptureCallback.startCapture()
        spark.range(1)
          .write
          .option(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "false")
          .parquet(outputPath.getAbsolutePath)

        val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
        assert(plans.nonEmpty, "Did not capture GPU write plan")
        ExecutionPlanCaptureCallback.assertContains(plans.head, "GpuDataWritingCommandExec")
      }
    }, sparkConf)
  }
}
