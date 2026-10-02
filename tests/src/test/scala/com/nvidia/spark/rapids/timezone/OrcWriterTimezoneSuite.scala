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

package com.nvidia.spark.rapids.timezone

import java.io.File
import java.sql.Timestamp
import java.time.Instant
import java.util.TimeZone

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{RapidsConf, SparkQueryCompareTestSuite}
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile
import org.apache.orc.impl.RecordReaderImpl

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession

class OrcWriterTimezoneSuite extends SparkQueryCompareTestSuite {
  private val timezones =
    Seq("UTC", "Asia/Shanghai", "America/New_York", "US/Pacific", "EST", "PST")

  private def dataFrame(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val instants = Seq(
      "1901-01-01T00:00:00Z",
      "1969-12-31T23:59:58.999999Z",
      "1969-12-31T23:59:59Z",
      "1969-12-31T23:59:59.000001Z",
      "1969-12-31T23:59:59.999999Z",
      "1970-01-01T00:00:00Z",
      "1970-01-01T00:00:00.000001Z",
      "2015-01-15T12:00:00Z",
      "2015-07-01T12:00:00Z",
      "2020-03-08T06:59:59.999999Z",
      "2020-03-08T07:00:00Z",
      "2020-11-01T05:59:59.999999Z",
      "2020-11-01T06:00:00Z",
      "2400-07-01T12:00:00Z",
      "9999-12-30T12:00:00Z")
    val timestamps = instants.map(value => Timestamp.from(Instant.parse(value))) :+ null
    timestamps.zipWithIndex.map { case (ts, id) => (id, ts) }.toDF("id", "ts")
      .selectExpr("id", "ts", "named_struct('value', ts) AS struct_ts",
        "array(ts, CAST(NULL AS TIMESTAMP)) AS array_ts",
        "map('value', ts) AS map_ts")
  }

  private def setTimezones(spark: SparkSession, jvm: String, session: String): Unit = {
    spark.conf.set("spark.sql.session.timeZone", session)
    TimeZone.setDefault(TimeZone.getTimeZone(jvm))
  }

  private def assertWriterTimezone(spark: SparkSession, dir: File, expected: String): Unit = {
    val files = dir.listFiles(_.getName.endsWith(".orc"))
    assert(files != null && files.nonEmpty)
    files.foreach { file =>
      val reader = OrcFile.createReader(new Path(file.getCanonicalPath),
        OrcFile.readerOptions(spark.sparkContext.hadoopConfiguration))
      val rows = reader.rows().asInstanceOf[RecordReaderImpl]
      try {
        assert(reader.getStripes.asScala.nonEmpty)
        reader.getStripes.asScala.foreach { stripe =>
          assert(rows.readStripeFooter(stripe).getWriterTimezone === expected)
        }
      } finally {
        rows.close()
        reader.close()
      }
    }
  }

  for {
    writerTimezone <- timezones
    sessionTimezone <- Seq("UTC", "Asia/Shanghai")
  } {
    test(s"ORC GPU writer: JVM=$writerTimezone session=$sessionTimezone") {
      val originalTimezone = TimeZone.getDefault
      try {
        withTempPath { root =>
          val cpuPath = new File(root, "cpu")
          val gpuPath = new File(root, "gpu")
          withCpuSparkSession { spark =>
            setTimezones(spark, writerTimezone, sessionTimezone)
            dataFrame(spark).coalesce(1).write.orc(cpuPath.getCanonicalPath)
          }
          withGpuSparkSession { spark =>
            setTimezones(spark, writerTimezone, sessionTimezone)
            dataFrame(spark).coalesce(1).write.orc(gpuPath.getCanonicalPath)
            val footerTimezone =
              if (writerTimezone == "PST") "America/Los_Angeles" else writerTimezone
            assertWriterTimezone(spark, gpuPath, footerTimezone)
          }
          timezones.foreach { readerTimezone =>
            withClue(s"readerTimezone=$readerTimezone: ") {
              val expected = withCpuSparkSession { spark =>
                setTimezones(spark, readerTimezone, sessionTimezone)
                spark.read.orc(cpuPath.getCanonicalPath).orderBy("id").collect()
              }
              val cpuRead = withCpuSparkSession { spark =>
                setTimezones(spark, readerTimezone, sessionTimezone)
                spark.read.orc(gpuPath.getCanonicalPath).orderBy("id").collect()
              }
              val gpuRead = withGpuSparkSession { spark =>
                setTimezones(spark, readerTimezone, sessionTimezone)
                spark.read.orc(gpuPath.getCanonicalPath).orderBy("id").collect()
              }
              compareResults(false, 0.0, expected, cpuRead)
              compareResults(false, 0.0, expected, gpuRead)
            }
          }
        }
      } finally {
        TimeZone.setDefault(originalTimezone)
      }
    }
  }

  Seq("GMT+05:30", "SystemV/EST5").foreach { unsupportedTimezone =>
    test(s"ORC timestamp writes fall back for JVM timezone $unsupportedTimezone") {
      val originalTimezone = TimeZone.getDefault
      val conf = new SparkConf().set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "DataWritingCommandExec,WriteFilesExec")
      try {
        val (cpu, gpu) = writeWithCpuAndGpu(
          spark => {
            setTimezones(spark, unsupportedTimezone, "UTC")
            dataFrame(spark)
          },
          (df, path) => df.write.orc(path),
          (spark, path) => {
            setTimezones(spark, unsupportedTimezone, "UTC")
            spark.read.orc(path).orderBy("id")
          }, conf)
        compareResults(false, 0.0, cpu, gpu)
      } finally {
        TimeZone.setDefault(originalTimezone)
      }
    }
  }

  test("ORC CTAS uses the JVM writer timezone") {
    val originalTimezone = TimeZone.getDefault
    try {
      val (cpu, gpu) = writeWithCpuAndGpu(
        spark => {
          setTimezones(spark, "America/New_York", "UTC")
          dataFrame(spark)
        },
        (df, path) => {
          val tableName = "orc_writer_timezone_ctas"
          try {
            df.write.format("orc").option("path", path).saveAsTable(tableName)
          } finally {
            df.sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
          }
        },
        (spark, path) => {
          setTimezones(spark, "America/New_York", "UTC")
          spark.read.orc(path).orderBy("id")
        })
      compareResults(false, 0.0, cpu, gpu)
    } finally {
      TimeZone.setDefault(originalTimezone)
    }
  }
}
