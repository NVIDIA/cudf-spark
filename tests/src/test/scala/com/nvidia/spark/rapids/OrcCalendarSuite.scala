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

package com.nvidia.spark.rapids

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.time.{LocalDate, ZoneId}

import ai.rapids.cudf.{ColumnVector, Table}
import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.RapidsReaderType.RapidsReaderType
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.{
  DateColumnVector, ListColumnVector, LongColumnVector, StructColumnVector}
import org.apache.orc.{OrcFile, TypeDescription}

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession

class OrcCalendarSuite extends SparkQueryCompareTestSuite {

  private val legacyDateResource = "test-data/before_1582_date_v2_4.snappy.orc"
  private val dateValue = LocalDate.of(1200, 1, 1).toEpochDay
  private val modernDateValue = LocalDate.of(2000, 1, 1).toEpochDay

  private def calendarConf(
      readerType: RapidsReaderType,
      useChunkedReader: Boolean,
      v1SourceList: String): SparkConf = {
    new SparkConf()
      .set(SQLConf.USE_V1_SOURCE_LIST.key, v1SourceList)
      .set(RapidsConf.ORC_READER_TYPE.key, readerType.toString)
      .set(RapidsConf.CHUNKED_READER.key, useChunkedReader.toString)
      .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, Integer.MAX_VALUE.toString)
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, (1L << 30).toString)
      .set("spark.sql.files.maxPartitionBytes", (1L << 30).toString)
  }

  private def readLegacyDateResource(spark: SparkSession) = {
    val resource = Option(Thread.currentThread().getContextClassLoader
      .getResource(legacyDateResource)).getOrElse {
      throw new IllegalStateException(s"Missing Spark test resource: $legacyDateResource")
    }
    val file = File.createTempFile("spark-24-date", ".orc")
    file.deleteOnExit()
    val input = resource.openStream()
    try {
      Files.copy(input, file.toPath, StandardCopyOption.REPLACE_EXISTING)
    } finally {
      input.close()
    }
    spark.read.orc(file.getCanonicalPath)
  }

  private def setDate(vector: DateColumnVector): Unit = {
    vector.setUsingProlepticCalendar(true)
    vector.vector(0) = dateValue
  }

  private def writeCalendarFile(
      spark: SparkSession,
      base: File,
      id: Int,
      writerUsedProlepticGregorian: Boolean): Unit = {
    val schema = TypeDescription.createStruct()
      .addField("id", TypeDescription.createInt())
      .addField("top_date", TypeDescription.createDate())
      .addField("modern_date", TypeDescription.createDate())
      .addField("nested", TypeDescription.createStruct()
        .addField("nested_date", TypeDescription.createDate()))
      .addField("dates", TypeDescription.createList(TypeDescription.createDate()))
    val options = OrcFile.writerOptions(spark.sparkContext.hadoopConfiguration)
      .setSchema(schema)
      .setProlepticGregorian(writerUsedProlepticGregorian)
    val path = new Path(base.getCanonicalPath, s"calendar-$id.orc")
    val writer = OrcFile.createWriter(path, options)
    try {
      val batch = schema.createRowBatch()
      batch.cols(0).asInstanceOf[LongColumnVector].vector(0) = id
      setDate(batch.cols(1).asInstanceOf[DateColumnVector])
      val modernDate = batch.cols(2).asInstanceOf[DateColumnVector]
      modernDate.setUsingProlepticCalendar(true)
      modernDate.vector(0) = modernDateValue

      val nested = batch.cols(3).asInstanceOf[StructColumnVector]
      setDate(nested.fields(0).asInstanceOf[DateColumnVector])

      val dates = batch.cols(4).asInstanceOf[ListColumnVector]
      dates.offsets(0) = 0
      dates.lengths(0) = 1
      dates.childCount = 1
      setDate(dates.child.asInstanceOf[DateColumnVector])

      batch.size = 1
      writer.addRowBatch(batch)
    } finally {
      writer.close()
    }

    withResourceIfAllowed(OrcFile.createReader(path,
      OrcFile.readerOptions(spark.sparkContext.hadoopConfiguration))) { reader =>
      assert(reader.writerUsedProlepticGregorian() === writerUsedProlepticGregorian,
        s"unexpected calendar metadata in $path")
    }
  }

  private def writeMixedCalendarFiles(spark: SparkSession, base: File): Unit = {
    assert(base.mkdirs())
    writeCalendarFile(spark, base, id = 0, writerUsedProlepticGregorian = false)
    writeCalendarFile(spark, base, id = 1, writerUsedProlepticGregorian = true)
  }

  test("proleptic nested ORC date rebase reuses the unchanged struct column") {
    withGpuSparkSession { _ =>
      withResource(ColumnVector.daysFromInts(0)) { dateColumn =>
        withResource(ColumnVector.makeStruct(dateColumn)) { structColumn =>
          withResource(GpuOrcTimezoneUtils.rebaseOrcDateTime(
            new Table(structColumn), ZoneId.systemDefault(),
            writerUsedProlepticGregorian = true)) { result =>
            assert(result.getColumn(0) eq structColumn)
          }
        }
      }
    }
  }

  for {
    v1SourceList <- Seq("orc", "")
    useChunkedReader <- Seq(false, true)
  } {
    testSparkResultsAreEqual(
      s"read Spark 2.4 legacy ORC date, source list is ($v1SourceList), " +
        s"chunked=$useChunkedReader",
      readLegacyDateResource,
      conf = calendarConf(RapidsReaderType.PERFILE, useChunkedReader, v1SourceList),
      repart = 0,
      skipCanonicalizationCheck = true,
      existClasses = if (v1SourceList == "orc") "GpuFileSourceScanExec" else "GpuBatchScan") {
      frame => frame
    }
  }

  for {
    readerType <- Seq(RapidsReaderType.COALESCING, RapidsReaderType.MULTITHREADED)
    useChunkedReader <- Seq(false, true)
  } {
    val v1SourceList = if (useChunkedReader) "" else "orc"
    testSparkReadResultsAreEqual(
      s"read mixed legacy and proleptic ORC dates with $readerType, " +
        s"source list is ($v1SourceList), chunked=$useChunkedReader",
      file => spark => {
        val frame = spark.read.orc(file.getCanonicalPath)
        assert(frame.queryExecution.executedPlan.execute().getNumPartitions === 1,
          "the legacy and proleptic ORC files must be assigned to one reader")
        frame
      },
      writeMixedCalendarFiles,
      conf = calendarConf(readerType, useChunkedReader, v1SourceList),
      repart = 0,
      skipCanonicalizationCheck = true,
      existClasses = if (v1SourceList == "orc") "GpuFileSourceScanExec" else "GpuBatchScan") {
      frame => frame.selectExpr(
        "id",
        "top_date",
        "modern_date",
        "nested.nested_date AS nested_date",
        "dates[0] AS list_date").orderBy("id")
    }
  }

}
