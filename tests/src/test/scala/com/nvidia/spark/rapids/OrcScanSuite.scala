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

import java.io.FileNotFoundException

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType}

class OrcScanSuite extends SparkQueryCompareTestSuite {

  private val fileSplitsOrc = frameFromOrc("file-splits.orc")

  testSparkResultsAreEqual("Test ORC chunks", fileSplitsOrc,
    new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "2048")) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"), col("zip"))
  }

  testSparkResultsAreEqual("Test ORC count chunked by rows", fileSplitsOrc,
    new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "2048"))(frameCount)

  testSparkResultsAreEqual("Test ORC count chunked by bytes", fileSplitsOrc,
    new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "100"))(frameCount)

  testSparkResultsAreEqual("schema-can-prune dis-order read schema",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c2_string", StringType),
      StructField("c3_long", LongType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune dis-order read schema 1",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c2_string", StringType),
      StructField("c1_int", IntegerType),
      StructField("c3_long", LongType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune dis-order read schema 2",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c3_long", LongType),
      StructField("c2_string", StringType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune dis-order read schema 3",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c3_long", LongType),
      StructField("c2_string", StringType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune dis-order read schema 4",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c2_string", StringType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune dis-order read schema 5",
    frameFromOrcWithSchema("schema-can-prune.orc", StructType(Seq(
      StructField("c3_long", LongType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  testSparkResultsAreEqual("schema-can-prune reordered columns reordered",
    frameFromOrcWithSchema("schema-cant-prune.orc",
        StructType(Seq(
          StructField("_col3", LongType),
          StructField("_col2", StringType),
          StructField("_col1", LongType))))) { frame => frame }

  test("ORC coalescing reader honors ignoreMissingFiles") {
    def collectAfterDeletingPlannedFiles(spark: SparkSession, checkGpu: Boolean): Seq[String] = {
      import spark.implicits._

      withTempPath { base =>
        val basePath = base.getCanonicalPath

        Seq("0").toDF("a").write.mode("overwrite").format("orc")
          .save(new Path(basePath, "second").toString)
        Seq("1").toDF("a").write.mode("overwrite").format("orc")
          .save(new Path(basePath, "fourth").toString)

        val firstPath = new Path(basePath, "first")
        val thirdPath = new Path(basePath, "third")
        val fs = thirdPath.getFileSystem(spark.sessionState.newHadoopConf())

        Seq("2").toDF("a").write.mode("overwrite").format("orc").save(firstPath.toString)
        Seq("3").toDF("a").write.mode("overwrite").format("orc").save(thirdPath.toString)

        val filesToDelete = Seq(firstPath, thirdPath).flatMap { path =>
          fs.listStatus(path).filter(_.isFile).map(_.getPath)
        }
        val df = spark.read.format("orc").load(
          firstPath.toString,
          new Path(basePath, "second").toString,
          thirdPath.toString,
          new Path(basePath, "fourth").toString)
        val hasGpuScan = df.queryExecution.executedPlan.collect {
          case scan: GpuFileSourceScanExec =>
            scan.selectedPartitions
            true
          case scan: FileSourceScanExec =>
            scan.selectedPartitions
            false
        }
        assert(hasGpuScan.nonEmpty, "ORC read does not have a file source scan")
        if (checkGpu) {
          assert(hasGpuScan.contains(true), "ORC read is not running on GPU")
        }

        filesToDelete.foreach(file => fs.delete(file, false))
        assert(fs.delete(thirdPath, true))

        df.collect().map(_.getString(0)).sorted.toSeq
      }
    }

    val conf = new SparkConf()
      .set(SQLConf.USE_V1_SOURCE_LIST.key, "orc")
      .set(SQLConf.IGNORE_MISSING_FILES.key, "true")
      .set(RapidsConf.ORC_READER_TYPE.key, RapidsReaderType.COALESCING.toString)

    val cpuResult = withCpuSparkSession(collectAfterDeletingPlannedFiles(_, checkGpu = false), conf)
    val gpuResult = withGpuSparkSession(collectAfterDeletingPlannedFiles(_, checkGpu = true), conf)

    assertResult(Seq("0", "1"))(cpuResult)
    assertResult(cpuResult)(gpuResult)
  }

  private def causedByFileNotFound(t: Throwable): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)
      .exists(_.isInstanceOf[FileNotFoundException])

  test("ORC coalescing reader throws FileNotFoundException when ignoreMissingFiles is false") {
    def collectAfterDeletingPlannedFiles(spark: SparkSession, checkGpu: Boolean): Unit = {
      import spark.implicits._

      withTempPath { base =>
        val basePath = base.getCanonicalPath

        Seq("0").toDF("a").write.mode("overwrite").format("orc")
          .save(new Path(basePath, "second").toString)
        Seq("1").toDF("a").write.mode("overwrite").format("orc")
          .save(new Path(basePath, "fourth").toString)

        val firstPath = new Path(basePath, "first")
        val thirdPath = new Path(basePath, "third")
        val fs = thirdPath.getFileSystem(spark.sessionState.newHadoopConf())

        Seq("2").toDF("a").write.mode("overwrite").format("orc").save(firstPath.toString)
        Seq("3").toDF("a").write.mode("overwrite").format("orc").save(thirdPath.toString)

        val filesToDelete = Seq(firstPath, thirdPath).flatMap { path =>
          fs.listStatus(path).filter(_.isFile).map(_.getPath)
        }
        val df = spark.read.format("orc").load(
          firstPath.toString,
          new Path(basePath, "second").toString,
          thirdPath.toString,
          new Path(basePath, "fourth").toString)
        val hasGpuScan = df.queryExecution.executedPlan.collect {
          case scan: GpuFileSourceScanExec =>
            scan.selectedPartitions
            true
          case scan: FileSourceScanExec =>
            scan.selectedPartitions
            false
        }
        assert(hasGpuScan.nonEmpty, "ORC read does not have a file source scan")
        if (checkGpu) {
          assert(hasGpuScan.contains(true), "ORC read is not running on GPU")
        }

        filesToDelete.foreach(file => fs.delete(file, false))
        assert(fs.delete(thirdPath, true))

        val e = intercept[Exception](df.collect())
        assert(causedByFileNotFound(e),
          s"Expected a FileNotFoundException when ignoreMissingFiles=false, but got: $e")
      }
    }

    val conf = new SparkConf()
      .set(SQLConf.USE_V1_SOURCE_LIST.key, "orc")
      .set(SQLConf.IGNORE_MISSING_FILES.key, "false")
      .set(RapidsConf.ORC_READER_TYPE.key, RapidsReaderType.COALESCING.toString)

    withCpuSparkSession(collectAfterDeletingPlannedFiles(_, checkGpu = false), conf)
    withGpuSparkSession(collectAfterDeletingPlannedFiles(_, checkGpu = true), conf)
  }

  /**
   *
   * The calendar of hybrid-Julian-calendar.orc file is hybrid Julian Gregorian
   * This file has one date column one row, value is 1582-10-03
   * When specify "orc.proleptic.gregorian" calendar to filter the file with c1 >= 1582-10-03,
   * then no result returned. Because of 1582-10-03 in hybrid calender
   * is actually 1582-09-23 in proleptic Gregorian calendar.
   */
  ignore("test hybrid Julian Gregorian calendar vs proleptic Gregorian calendar") {
    // After Spark 3.1.1, Orc failed to prune when converting Hybrid calendar to Proleptic calendar
    // Orc bug: https://issues.apache.org/jira/browse/ORC-1083

    withCpuSparkSession(spark => {
      val df = frameFromOrcWithSchema("hybrid-Julian-calendar.orc",
        StructType(Seq(StructField("c1", DateType))))(spark)
      val ret = df.collect()
      ret.length == 1
      assert(ret(0).toString() == "[1582-10-03]")
    })

    def check(spark: SparkSession) = {
      val df = frameFromOrcWithSchema("hybrid-Julian-calendar.orc",
        StructType(Seq(StructField("c1", DateType))))(spark)
      df.createOrReplaceTempView("df1")
      // should not have data, if orc.proleptic.gregorian is specified
      val ret = spark.sql("select c1 from df1 where c1 >= to_date('1582-10-03', 'yyyy-MM-dd') ")
          .collect()
      assert(ret.isEmpty)
    }

    val conf: SparkConf = new SparkConf()
    // indicate convert to proleptic Gregorian calendar
    conf.set("orc.proleptic.gregorian", "true")

    // both cpu and gpu should return empty
    withGpuSparkSession(check, conf)
    withCpuSparkSession(check, conf)
  }

}
