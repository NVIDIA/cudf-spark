/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import ai.rapids.cudf.CompressionType
import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil.fullyDelete
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, JobContext, TaskAttemptContext, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetOutputFormat, ParquetWriter}

import org.apache.spark.SparkConf
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.sql.hive.rapids.GpuHiveParquetFileFormat
import org.apache.spark.sql.rapids.BasicColumnarWriteJobStatsTracker
import org.apache.spark.sql.rapids.shims.SparkUpgradeExceptionShims
import org.apache.spark.sql.types.StructType

/**
 * Tests for writing Parquet files with the GPU.
 */
@scala.annotation.nowarn(
  "msg=method readFooters in class ParquetFileReader is deprecated"
)
class ParquetWriterSuite extends SparkQueryCompareTestSuite {
  test("file metadata") {
    val tempFile = File.createTempFile("stats", ".parquet")
    try {
      withGpuSparkSession(spark => {
        val df = mixedDfWithNulls(spark)
        df.write.mode("overwrite").parquet(tempFile.getAbsolutePath)

        val footer = ParquetFileReader.readFooters(spark.sparkContext.hadoopConfiguration,
          new Path(tempFile.getAbsolutePath)).get(0)

        val parquetMeta = footer.getParquetMetadata
        val fileMeta = footer.getParquetMetadata.getFileMetaData
        val extra = fileMeta.getKeyValueMetaData
        assert(extra.containsKey("org.apache.spark.version"))
        assert(extra.containsKey("org.apache.spark.sql.parquet.row.metadata"))

        val blocks = parquetMeta.getBlocks
        assertResult(1) { blocks.size }
        val block = blocks.get(0)
        assertResult(11) { block.getRowCount }
        val cols = block.getColumns
        assertResult(4) { cols.size }

        assertResult(3) { cols.get(0).getStatistics.getNumNulls }
        assertResult(-700L) { cols.get(0).getStatistics.genericGetMin }
        assertResult(1200L) { cols.get(0).getStatistics.genericGetMax }

        assertResult(4) { cols.get(1).getStatistics.getNumNulls }
        assertResult(1.0) { cols.get(1).getStatistics.genericGetMin }
        assertResult(9.0) { cols.get(1).getStatistics.genericGetMax }

        assertResult(4) { cols.get(2).getStatistics.getNumNulls }
        assertResult(90) { cols.get(2).getStatistics.genericGetMin }
        assertResult(99) { cols.get(2).getStatistics.genericGetMax }

        assertResult(1) { cols.get(3).getStatistics.getNumNulls }
        assertResult("A") {
          new String(cols.get(3).getStatistics.getMinBytes, StandardCharsets.UTF_8)
        }
        assertResult("\ud720\ud721") {
          new String(cols.get(3).getStatistics.getMaxBytes, StandardCharsets.UTF_8)
        }
      })
    } finally {
      fullyDelete(tempFile)
    }
  }

  test("sorted partitioned write") {
    val conf = new SparkConf().set(RapidsConf.SQL_ENABLED.key, "true")
    val tempFile = File.createTempFile("partitioned", ".parquet")
    try {
      SparkSessionHolder.withSparkSession(conf, spark => {
        import spark.implicits._
        val df = spark.sparkContext.parallelize((1L to 10000000L))
            .map{i => ("a", f"$i%010d", i)}.toDF("partkey", "val", "val2")
        df.repartition(1, $"partkey").sortWithinPartitions($"partkey", $"val", $"val2")
            .write.mode("overwrite").partitionBy("partkey").parquet(tempFile.getAbsolutePath)
        val firstRow = spark.read.parquet(tempFile.getAbsolutePath).head
        assertResult("0000000001")(firstRow.getString(0))
      })
    } finally {
      fullyDelete(tempFile)
    }
  }

  test("parquet writer row group size rows config") {
    // cuDF row group boundaries are page-fragment based, so use a value that is observable.
    val rowGroupRows = 5000
    val numRows = rowGroupRows * 2 + 2000
    val conf = new SparkConf()
      .set(RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_ROWS.key, rowGroupRows.toString)
      .set(RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_BYTES.key, "1m")
    withGpuSparkSession(spark => {
      withTempPath { writePath =>
        spark.range(0, numRows, 1, 1).write.mode("overwrite").parquet(writePath.getAbsolutePath)

        val rowGroupCounts = getSingleParquetFileRowGroupCounts(spark, writePath)
        assert(rowGroupCounts.length > 1, s"Expected multiple row groups, got $rowGroupCounts")
        assert(rowGroupCounts.forall(_ <= rowGroupRows.toLong),
          s"Expected all row groups <= $rowGroupRows rows, got $rowGroupCounts")
        assertResult(numRows.toLong) {
          rowGroupCounts.sum
        }
      }
    }, conf)
  }

  test("hive parquet writer row group size bytes config partition flush size") {
    val rowGroupSizeBytes = 1024L
    val conf = new SparkConf()
      .set(RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_BYTES.key, rowGroupSizeBytes.toString)
    withGpuSparkSession(spark => {
      val job = Job.getInstance(spark.sparkContext.hadoopConfiguration)
      val factory = new GpuHiveParquetFileFormat(CompressionType.NONE)
        .prepareWrite(spark, job, Map.empty, new StructType())
      val attemptId = new TaskAttemptID("job", 0, TaskType.MAP, 0, 0)
      val context = new TaskAttemptContextImpl(job.getConfiguration, attemptId)

      assertResult(rowGroupSizeBytes) {
        factory.partitionFlushSize(context)
      }
    }, conf)
  }

  test("parquet writer row group size bytes config") {
    val rowGroupRows = 1000000
    val rowGroupSizeBytes = 1024L
    val bytesPerRow = java.lang.Long.BYTES * 2
    val rowGroupByteSizeOverhead = 512L
    val conf = new SparkConf()
      .set(RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_ROWS.key, rowGroupRows.toString)
      .set(RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_BYTES.key, rowGroupSizeBytes.toString)
    withGpuSparkSession(spark => {
      withTempPath { writePath =>
        spark.range(0, 10000, 1, 1)
          .selectExpr("id", "id + 1 as id2")
          .write.mode("overwrite")
          .parquet(writePath.getAbsolutePath)

        val rowGroups = getSingleParquetFileRowGroups(spark, writePath)
        assert(rowGroups.length > 1, s"Expected multiple row groups, got $rowGroups")
        assert(rowGroups.forall(_.rowCount <= rowGroupRows.toLong),
          s"Expected all row groups <= $rowGroupRows rows, got $rowGroups")
        // cuDF sizes row groups from uncompressed data estimates; footer sizes
        // include page overhead.
        assert(rowGroups.forall(_.rowCount * bytesPerRow <= rowGroupSizeBytes),
          s"Expected estimated data bytes <= $rowGroupSizeBytes, got $rowGroups")
        assert(rowGroups.forall(_.totalByteSize <= rowGroupSizeBytes + rowGroupByteSizeOverhead),
          s"Expected row group byte sizes <= $rowGroupSizeBytes plus " +
            s"$rowGroupByteSizeOverhead bytes of page overhead, got $rowGroups")
        assertResult(10000L) {
          rowGroups.map(_.rowCount).sum
        }
      }
    }, conf)
  }

  test("parquet writer dictionary policy NEVER config") {
    val conf = new SparkConf()
      .set(RapidsConf.PARQUET_WRITER_DICTIONARY_POLICY.key, "NEVER")
    withGpuSparkSession(spark => {
      withTempPath { writePath =>
        // Small low-cardinality column: ADAPTIVE would otherwise dictionary-encode it.
        spark.range(0, 1000, 1, 1)
          .selectExpr("cast(id % 10 as string) as s")
          .write.mode("overwrite")
          .parquet(writePath.getAbsolutePath)

        val encodings = getSingleParquetFileColumnEncodings(spark, writePath)
        assert(encodings.nonEmpty)
        assert(encodings.forall(es => !isDictionaryEncoding(es)),
          s"Expected no dictionary encoding under NEVER policy, got $encodings")
      }
    }, conf)
  }

  test("parquet writer dictionary policy ALWAYS overrides max dictionary size cap") {
    // Use a repeated wide payload whose dictionary is useful but exceeds the tiny cap.
    // Lowercase policy value verifies case-insensitive config parsing.
    val conf = new SparkConf()
      .set(RapidsConf.PARQUET_WRITER_DICTIONARY_POLICY.key, "always")
      .set(RapidsConf.PARQUET_WRITER_MAX_DICTIONARY_SIZE.key, "1024")
    withGpuSparkSession(spark => {
      withTempPath { writePath =>
        spark.range(0, 5000, 1, 1)
          .selectExpr("concat(cast(id % 100 as string), repeat('x', 64)) as s")
          .write.mode("overwrite")
          .parquet(writePath.getAbsolutePath)

        val encodings = getSingleParquetFileColumnEncodings(spark, writePath)
        assert(encodings.nonEmpty)
        assert(encodings.exists(isDictionaryEncoding),
          s"Expected dictionary encoding under ALWAYS (cap should be ignored), got $encodings")
      }
    }, conf)
  }

  test("parquet writer max dictionary size config bails under ADAPTIVE") {
    val baseConf = new SparkConf()
      .set(RapidsConf.PARQUET_WRITER_DICTIONARY_POLICY.key, "ADAPTIVE")

    // Repeated values keep dictionary encoding useful while the wide payload makes the
    // dictionary trip a 1 KiB cap and still fit comfortably under a 1 MiB cap.
    def writeWidePayload(spark: SparkSession, path: File): Unit = {
      spark.range(0, 5000, 1, 1)
        .selectExpr("concat(cast(id % 100 as string), repeat('x', 64)) as s")
        .write.mode("overwrite")
        .parquet(path.getAbsolutePath)
    }

    // Tiny cap: ADAPTIVE should bail to non-dictionary encoding.
    val tinyCapConf = baseConf.clone()
      .set(RapidsConf.PARQUET_WRITER_MAX_DICTIONARY_SIZE.key, "1024")
    withGpuSparkSession(spark => {
      withTempPath { writePath =>
        writeWidePayload(spark, writePath)
        val encodings = getSingleParquetFileColumnEncodings(spark, writePath)
        assert(encodings.nonEmpty)
        assert(encodings.forall(es => !isDictionaryEncoding(es)),
          s"Expected ADAPTIVE to bail to non-dictionary under tiny cap, got $encodings")
      }
    }, tinyCapConf)

    // Generous cap (1 MiB == cuDF default): dictionary encoding should be used.
    val largeCapConf = baseConf.clone()
      .set(RapidsConf.PARQUET_WRITER_MAX_DICTIONARY_SIZE.key, (1024 * 1024).toString)
    withGpuSparkSession(spark => {
      withTempPath { writePath =>
        writeWidePayload(spark, writePath)
        val encodings = getSingleParquetFileColumnEncodings(spark, writePath)
        assert(encodings.nonEmpty)
        assert(encodings.exists(isDictionaryEncoding),
          s"Expected ADAPTIVE to dictionary-encode under generous cap, got $encodings")
      }
    }, largeCapConf)
  }

  test("parquet block size warning") {
    val unsetConf = new Configuration(false)
    assert(GpuParquetFileFormat.parquetBlockSizeWarning(unsetConf, Map.empty).isEmpty)

    val defaultConf = new Configuration(false)
    defaultConf.setLong(ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE)
    assert(GpuParquetFileFormat.parquetBlockSizeWarning(defaultConf, Map.empty).isEmpty)

    val defaultOptions = Map(ParquetOutputFormat.BLOCK_SIZE ->
      ParquetWriter.DEFAULT_BLOCK_SIZE.toString)
    assert(GpuParquetFileFormat.parquetBlockSizeWarning(unsetConf, defaultOptions).isEmpty)

    val nonDefaultConf = new Configuration(false)
    nonDefaultConf.setLong(ParquetOutputFormat.BLOCK_SIZE,
      ParquetWriter.DEFAULT_BLOCK_SIZE.toLong * 2)
    val warning = GpuParquetFileFormat.parquetBlockSizeWarning(nonDefaultConf, Map.empty)
    assert(warning.exists(_.contains(ParquetOutputFormat.BLOCK_SIZE)))
    assert(warning.exists(_.contains(RapidsConf.ENABLE_PARQUET_WRITE.key)))
    assert(warning.exists(_.contains(RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_ROWS.key)))
    assert(warning.exists(_.contains(RapidsConf.PARQUET_WRITER_ROW_GROUP_SIZE_BYTES.key)))
    assert(warning.exists(_.contains("not equivalent")))

    // Options override the Hadoop conf: a non-default option warns even when the
    // conf is set to the default.
    val mixedConf = new Configuration(false)
    mixedConf.setLong(ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE)
    val nonDefaultOptions = Map(ParquetOutputFormat.BLOCK_SIZE ->
      (ParquetWriter.DEFAULT_BLOCK_SIZE.toLong * 2).toString)
    assert(GpuParquetFileFormat.parquetBlockSizeWarning(mixedConf, nonDefaultOptions).nonEmpty)
  }

  private def listAllFiles(f: File): Array[File] = {
    if (f.isFile()) {
      Array(f)
    } else if (f.isDirectory()) {
      f
        .listFiles()
        .flatMap(f => listAllFiles(f))
    } else {
      Array.empty
    }
  }

  private def getSingleParquetFileRowGroupCounts(
      spark: SparkSession,
      parquetPath: File): Seq[Long] = {
    getSingleParquetFileRowGroups(spark, parquetPath).map(_.rowCount)
  }

  private case class ParquetRowGroup(rowCount: Long, totalByteSize: Long)

  private def getSingleParquetFileRowGroups(
      spark: SparkSession,
      parquetPath: File): Seq[ParquetRowGroup] = {
    val parquetFiles = listAllFiles(parquetPath).filter(_.getName.endsWith(".parquet"))
    assertResult(1) {
      parquetFiles.length
    }
    val footer = ParquetFileReader.readFooters(spark.sparkContext.hadoopConfiguration,
      new Path(parquetFiles.head.getAbsolutePath)).get(0)
    val blocks = footer.getParquetMetadata.getBlocks
    (0 until blocks.size()).map { i =>
      val block = blocks.get(i)
      ParquetRowGroup(block.getRowCount, block.getTotalByteSize)
    }
  }

  /** Encodings per column chunk across all row groups in a single Parquet file. */
  private def getSingleParquetFileColumnEncodings(
      spark: SparkSession,
      parquetPath: File): Seq[Set[Encoding]] = {
    val parquetFiles = listAllFiles(parquetPath).filter(_.getName.endsWith(".parquet"))
    assertResult(1) {
      parquetFiles.length
    }
    val footer = ParquetFileReader.readFooters(spark.sparkContext.hadoopConfiguration,
      new Path(parquetFiles.head.getAbsolutePath)).get(0)
    footer.getParquetMetadata.getBlocks.asScala
      .flatMap(_.getColumns.asScala)
      .map(_.getEncodings.asScala.toSet)
      .toSeq
  }

  @scala.annotation.nowarn("cat=deprecation")
  private def isDictionaryEncoding(encodings: Set[Encoding]): Boolean =
    encodings.contains(Encoding.PLAIN_DICTIONARY) ||
      encodings.contains(Encoding.RLE_DICTIONARY)

  test("set max records per file no partition") {
    val conf = new SparkConf()
        .set("spark.sql.files.maxRecordsPerFile", "50")
        .set(RapidsConf.SQL_ENABLED.key, "true")
    val tempFile = File.createTempFile("maxRecords", ".parquet")
    val assertRowCount50 = assertResult(50) _

    try {
      SparkSessionHolder.withSparkSession(conf, spark => {
        import spark.implicits._
        val df = (1 to 16000).toDF()
        df.write.mode("overwrite").parquet(tempFile.getAbsolutePath())

        listAllFiles(tempFile)
          .map(f => f.getAbsolutePath())
          .filter(p => p.endsWith("parquet"))
          .map(p => {
            assertRowCount50 (spark.read.parquet(p).count()) 
          })
      })
    } finally {
      fullyDelete(tempFile)
    }
  }

  test("set max records per file with partition") {
    val conf = new SparkConf()
        .set("spark.rapids.sql.batchSizeBytes", "1") // forces multiple batches per partition
        .set("spark.sql.files.maxRecordsPerFile", "50")
        .set(RapidsConf.SQL_ENABLED.key, "true")
    val tempFile = File.createTempFile("maxRecords", ".parquet")
    val assertRowCount50 = assertResult(50) _

    try {
      SparkSessionHolder.withSparkSession(conf, spark => {
        import spark.implicits._
        val df = (1 to 16000).map(i => (i, i % 2)).toDF()
        df.write.mode("overwrite").partitionBy("_2").parquet(tempFile.getAbsolutePath())

        listAllFiles(tempFile)
          .map(f => f.getAbsolutePath())
          .filter(p => p.endsWith("parquet"))
          .map(p => {
            assertRowCount50 (spark.read.parquet(p).count()) 
          })
      })
    } finally {
      fullyDelete(tempFile)
    }
  }

  test("set maxRecordsPerFile with partition concurrently") {
    val tempFile = File.createTempFile("maxRecords", ".parquet")

    Seq(("40", 40), ("200", 80)).foreach{ case (maxRecordsPerFile, expectedRecordsPerFile) =>
      val conf = new SparkConf()
        .set("spark.rapids.sql.batchSizeBytes", "1") // forces multiple batches per partition
        .set("spark.sql.files.maxRecordsPerFile", maxRecordsPerFile)
        .set("spark.sql.maxConcurrentOutputFileWriters", "30")
        .set(RapidsConf.SQL_ENABLED.key, "true")
      try {
        SparkSessionHolder.withSparkSession(conf, spark => {
          import spark.implicits._
          val df = (1 to 1600).map(i => (i, i % 20)).toDF()
          df
            .repartition(1)
            .write
            .mode("overwrite")
            .partitionBy("_2")
            .parquet(tempFile.getAbsolutePath())
          // check the whole number of rows
          assertResult(1600) (spark.read.parquet(tempFile.getAbsolutePath()).count())
          // check number of rows in each file
          listAllFiles(tempFile)
            .map(f => f.getAbsolutePath())
            .filter(p => p.endsWith("parquet"))
            .map(p => {
              assertResult(expectedRecordsPerFile) (spark.read.parquet(p).count())
            })
        })
      } finally {
        fullyDelete(tempFile)
      }
    }
  }

  test("set maxRecordsPerFile with partition concurrently fallback") {
    val tempFile = File.createTempFile("maxRecords", ".parquet")

    Seq(("40", 40), ("200", 80)).foreach { case (maxRecordsPerFile, expectedRecordsPerFile) =>
      val conf = new SparkConf()
          .set("spark.rapids.sql.batchSizeBytes", "1") // forces multiple batches per partition
          .set("spark.sql.files.maxRecordsPerFile", maxRecordsPerFile)
          .set("spark.sql.maxConcurrentOutputFileWriters", "10")
          .set(RapidsConf.SQL_ENABLED.key, "true")
      try {
        SparkSessionHolder.withSparkSession(conf, spark => {
          import spark.implicits._
          val df = (1 to 1600).map(i => (i, i % 20)).toDF()
          df
              .repartition(1)
              .write
              .mode("overwrite")
              .partitionBy("_2")
              .parquet(tempFile.getAbsolutePath())
          // check the whole number of rows
          assertResult(1600)(spark.read.parquet(tempFile.getAbsolutePath()).count())
          // check number of rows in each file
          listAllFiles(tempFile)
              .map(f => f.getAbsolutePath())
              .filter(p => p.endsWith("parquet"))
              .map(p => {
                assertResult(expectedRecordsPerFile)(spark.read.parquet(p).count())
              })
        })
      } finally {
        fullyDelete(tempFile)
      }
    }
  }

  testExpectedGpuException(
    "Old dates in EXCEPTION mode",
    if (isSpark400OrLater) {
      classOf[org.apache.spark.sql.AnalysisException]
    } else {
      SparkUpgradeExceptionShims.getSparkUpgradeExceptionClass
    },
    oldDatesDf,
    new SparkConf().set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "EXCEPTION")) {
    val tempFile = File.createTempFile("oldDates", "parquet")
    tempFile.delete()
    frame => {
      frame.write.mode("overwrite").parquet(tempFile.getAbsolutePath)
      frame
    }
  }


  testExpectedGpuException(
    "Old timestamps millis in EXCEPTION mode",
    if (isSpark400OrLater) {
      classOf[org.apache.spark.sql.AnalysisException]
    } else {
      SparkUpgradeExceptionShims.getSparkUpgradeExceptionClass
    },
    oldTsDf,
    new SparkConf()
      .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "EXCEPTION")
      .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")) {
    val tempFile = File.createTempFile("oldTimeStamp", "parquet")
    tempFile.delete()
    frame => {
      frame.write.mode("overwrite").parquet(tempFile.getAbsolutePath)
      frame
    }
  }

  testExpectedGpuException(
    "Old timestamps in EXCEPTION mode",
    if (isSpark400OrLater) {
      classOf[org.apache.spark.sql.AnalysisException]
    } else {
      SparkUpgradeExceptionShims.getSparkUpgradeExceptionClass
    },
    oldTsDf,
    new SparkConf()
      .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "EXCEPTION")
      .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")) {
    val tempFile = File.createTempFile("oldTimeStamp", "parquet")
    tempFile.delete()
    frame => {
      frame.write.mode("overwrite").parquet(tempFile.getAbsolutePath)
      frame
    }
  }

  test("Job commit time metrics") {
    val slowCommitClass = "com.nvidia.spark.rapids.SlowFileCommitProtocolForTest"
    withGpuSparkSession(spark => {
      try {
        spark.sql("CREATE TABLE t(id STRING) USING PARQUET")
        val df = spark.sql("INSERT INTO TABLE t SELECT 'abc'")
        val insert = SparkShimImpl.findOperators(df.queryExecution.executedPlan,
          _.isInstanceOf[GpuDataWritingCommandExec]).head
          .asInstanceOf[GpuDataWritingCommandExec]
        assert(insert.metrics.contains(BasicColumnarWriteJobStatsTracker.JOB_COMMIT_TIME))
        assert(insert.metrics.contains(BasicColumnarWriteJobStatsTracker.TASK_COMMIT_TIME))
        assert(insert.metrics(BasicColumnarWriteJobStatsTracker.JOB_COMMIT_TIME).value > 0)
        assert(insert.metrics(BasicColumnarWriteJobStatsTracker.TASK_COMMIT_TIME).value > 0)
      } finally {
        spark.sql("DROP TABLE IF EXISTS t")
        spark.sql("DROP TABLE IF EXISTS tempmetricstable")
      }
    }, new SparkConf()
      .set("spark.sql.sources.commitProtocolClass", slowCommitClass))
  }
}

/** File committer that sleeps before committing each task and the job. */
case class SlowFileCommitProtocolForTest(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
    extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {
  override def commitTask(
      taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    Thread.sleep(100)
    super.commitTask(taskContext)
  }

  override def commitJob(
      jobContext: JobContext,
      taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    Thread.sleep(100)
    super.commitJob(jobContext, taskCommits)
  }
}
