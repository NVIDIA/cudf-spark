/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.RapidsReaderType._
import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.rapids.{ExternalSource, GpuFileSourceScanExec}

trait ReaderTypeSuite extends SparkQueryCompareTestSuite {

  /** File format */
  protected def format: String
  protected def expectedV2ScanClassName: String

  protected def otherConfs: Iterable[(String, String)] = Seq.empty
  protected def testContextOk: Boolean = true

  private def checkReaderType(
      readerFactory: PartitionReaderFactory,
      inputFile: Array[String],
      expectedReaderType: RapidsReaderType) = {
    val actualReaderType = readerFactory match {
      case mf: MultiFilePartitionReaderFactoryBase =>
        if (mf.useMultiThread(inputFile)) {
          MULTITHREADED
        } else {
          COALESCING
        }
      case _ => PERFILE
    }
    assert(expectedReaderType == actualReaderType,
      s", expected $expectedReaderType, but got $actualReaderType")
  }

  /**
   * Test if the given reader type will be used.
   *
   * @param conf SparkConf
   * @param files input files
   * @param multithreadedReadingExpected true: multithreaded reading, false: coalescing reading.
   * @param hasInputExpression if has input expression
   */
  protected final def testReaderType(
      conf: SparkConf,
      files: Array[String],
      expectedReaderType: RapidsReaderType,
      hasInputExpression: Boolean = false): Unit = {
    withTempPath { file =>
      withCpuSparkSession(spark => {
        import spark.implicits._
        Seq(1).toDF("a").write.format(format).save(file.getCanonicalPath)
      })

      withGpuSparkSession(spark => {
        val rawDf = spark.read.format(format).load(file.toString)
        val df = if (hasInputExpression) rawDf.withColumn("input", input_file_name()) else rawDf
        val plans = df.queryExecution.executedPlan.collect {
          case plan: GpuBatchScanExec =>
            checkReaderType(plan.readerFactory, files, expectedReaderType)
            plan
          case plan: GpuFileSourceScanExec =>
            checkReaderType(plan.readerFactory, files, expectedReaderType)
            plan
        }
        assert(!plans.isEmpty, "File reader is not running on GPU")
      }, conf.setAll(otherConfs))
    }
  }

  test("Use coalescing reading for local files") {
    assume(testContextOk)
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testReaderType(conf, testFile, COALESCING)
    })
  }

  test("Use multithreaded reading for cloud files") {
    assume(testContextOk)
    val testFile = Array("s3:/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testReaderType(conf, testFile, MULTITHREADED)
    })
  }

  test("Force coalescing reading for cloud files when setting COALESCING ") {
    assume(testContextOk)
    val testFile = Array("s3:/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, COALESCING)
    })
  }

  test("Force multithreaded reading for local files when setting MULTITHREADED") {
    assume(testContextOk)
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "MULTITHREADED")
      testReaderType(conf, testFile, MULTITHREADED)
    })
  }

  test("Use multithreaded reading for input expression even setting COALESCING") {
    assume(testContextOk)
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, MULTITHREADED, hasInputExpression=true)
    })
  }

  test("Use multithreaded reading for ignoreCorruptFiles even setting COALESCING") {
    assume(testContextOk)
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set("spark.sql.files.ignoreCorruptFiles", "true")
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, MULTITHREADED)
    })
  }

  test("[SPARK-16818] V2 GPU partition-pruned scans implement sameResult correctly") {
    assume(testContextOk)
    withTempPath { file =>
      withCpuSparkSession(spark => {
        import spark.implicits._
        Seq((1, 10), (2, 20), (3, 30))
          .toDF("id", "b")
          .write
          .partitionBy("id")
          .format(format)
          .save(file.getCanonicalPath)
      })

      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", "")
        .set("spark.sql.adaptive.enabled", "false")

      withGpuSparkSession(spark => {
        val df = spark.read.format(format).load(file.toString)

        def getPlanAndScan(partition: Int): (DataFrame, SparkPlan, GpuBatchScanExec) = {
          val filtered = df.where(s"id = $partition")
          val plan = filtered.queryExecution.executedPlan
          val scans = plan.collect { case scan: GpuBatchScanExec => scan }
          assert(scans.length == 1, s"Expected one GPU V2 scan, found ${scans.length}")
          assert(scans.head.scan.getClass.getName == expectedV2ScanClassName,
            s"Expected $expectedV2ScanClassName, found ${scans.head.scan.getClass.getName}")
          (filtered, plan, scans.head)
        }

        val (df2a, p2a, s2a) = getPlanAndScan(2)
        val (_, p2b, s2b) = getPlanAndScan(2)
        val (_, p3, s3) = getPlanAndScan(3)

        val rows = df2a.collect()
        assert(rows.length == 1)
        assert(rows.head.getAs[Int]("b") == 20)

        assert(p2a.sameResult(p2b), "Equivalent V2 scan plans should have the same result")
        assert(s2a.sameResult(s2b), "Equivalent V2 batch scans should have the same result")
        assert(s2a.scan == s2b.scan, "Equivalent V2 scans should be equal")
        s2a.scan match {
          case scan: GpuOrcScan =>
            def withFloatToString(enabled: Boolean): GpuOrcScan =
              scan.copy(rapidsConf = new RapidsConf(Map(
                RapidsConf.ENABLE_ORC_FLOAT_TYPES_TO_STRING.key -> enabled.toString)))
            val enabled1 = withFloatToString(enabled = true)
            val enabled2 = withFloatToString(enabled = true)
            val disabled = withFloatToString(enabled = false)
            assert(enabled1 == enabled2,
              "ORC V2 scans with equal float-to-string settings should be equal")
            assert(enabled1 != disabled,
              "ORC V2 scans with different float-to-string settings should not be equal")
          case _ =>
        }
        assert(!p2a.sameResult(p3), "Different partition filters should not have the same result")
        assert(!s2a.sameResult(s3),
          "V2 batch scans with different partition filters should not have the same result")
        assert(s2a.scan != s3.scan, "V2 scans with different partition filters should not be equal")
      }, conf.setAll(otherConfs))
    }
  }
}

class GpuParquetReaderTypeSuites extends ReaderTypeSuite {
  override protected def format: String = "parquet"
  override protected def expectedV2ScanClassName: String =
    "com.nvidia.spark.rapids.parquet.GpuParquetScan"
}

class GpuOrcReaderTypeSuites extends ReaderTypeSuite {
  override protected def format: String = "orc"
  override protected def expectedV2ScanClassName: String =
    "com.nvidia.spark.rapids.GpuOrcScan"
}

class GpuAvroReaderTypeSuites extends ReaderTypeSuite {
  override lazy val format: String = "avro"
  override protected def expectedV2ScanClassName: String =
    "org.apache.spark.sql.rapids.GpuAvroScan"
  override lazy val otherConfs: Iterable[(String, String)] = Seq(
    ("spark.rapids.sql.format.avro.read.enabled", "true"),
    ("spark.rapids.sql.format.avro.enabled", "true"))
  override lazy val testContextOk = ExternalSource.hasSparkAvroJar
}
