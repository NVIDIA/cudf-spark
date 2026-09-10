/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import scala.collection.mutable

import com.nvidia.spark.rapids.{GpuCSVScan, GpuOrcScan}
import com.nvidia.spark.rapids.parquet.GpuParquetScan
import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, FileBasedDataSourceSuite, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.json.rapids.GpuJsonScan
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsFileBasedDataSourceSuite extends FileBasedDataSourceSuite with RapidsSQLTestsTrait {
  import testImplicits._

  private val rapidsV2FileBasedDataSources = Seq("orc", "parquet", "csv", "json")

  private def gpuScanFilters(
      scan: GpuBatchScanExec): Option[(Seq[Expression], Seq[Expression])] = scan.scan match {
    case s: GpuOrcScan => Some((s.partitionFilters, s.dataFilters))
    case s: GpuParquetScan => Some((s.partitionFilters, s.dataFilters))
    case s: GpuCSVScan => Some((s.partitionFilters, s.dataFilters))
    case s: GpuJsonScan => Some((s.partitionFilters, s.dataFilters))
    case _ => None
  }

  private def getGpuBatchFileScan(df: DataFrame): GpuBatchScanExec = {
    val scans = collect(df.queryExecution.executedPlan) {
      case scan: GpuBatchScanExec if gpuScanFilters(scan).isDefined => scan
    }
    scans.headOption.getOrElse(
      fail(s"Expected a GPU file scan in plan:\n${df.queryExecution.executedPlan}"))
  }

  private def assertPartitionFiltersAreNotReevaluated(df: DataFrame): Unit = {
    val filterCondition = df.queryExecution.optimizedPlan.collectFirst {
      case f: Filter => f.condition
    }
    assert(filterCondition.isDefined)
    assert(!filterCondition.get.exists {
      case a: AttributeReference => a.name == "p1" || a.name == "p2"
      case _ => false
    })
  }

  private def assertGpuScanReadsOnlySelectedPartitions(scan: GpuBatchScanExec): Unit = {
    val files = scan.inputPartitions.flatMap {
      case partition: FilePartition => partition.files
      case other => fail(s"Expected FilePartition from GPU scan, got ${other.getClass}")
    }
    assert(files.nonEmpty)
    assert(files.forall { file =>
      file.filePath.contains("p1=1") && file.filePath.contains("p2=2")
    }, s"Unexpected file list: ${files.map(_.filePath).mkString(", ")}")
  }

  testRapids("SPARK-25237 compute correct input metrics in FileScanRDD") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "csv") {
      withTempPath { p =>
        val path = p.getAbsolutePath
        spark.range(1000).repartition(1).write.csv(path)
        val bytesReads = new mutable.ArrayBuffer[Long]()
        val bytesReadListener = new SparkListener() {
          override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
            bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
          }
        }
        sparkContext.addSparkListener(bytesReadListener)
        try {
          val df = spark.read.csv(path).limit(1)
          df.collect()
          sparkContext.listenerBus.waitUntilEmpty()
          assert(bytesReads.sum > 0)

          val scans = collect(df.queryExecution.executedPlan) {
            case scan: GpuFileSourceScanExec => scan
          }
          assert(scans.nonEmpty, s"Expected GpuFileSourceScanExec in plan:\n${df.queryExecution}")
          val scan = scans.head
          assert(scan.metrics("numFiles").value === 1)
          assert(scan.metrics("filesSize").value > 0)
        } finally {
          sparkContext.removeSparkListener(bytesReadListener)
        }
      }
    }
  }

  testRapids("Option recursiveFileLookup: disable partition inferring") {
    val dataPath = testFile("test-data/text-partitioned")

    val df = spark.read.format("binaryFile")
      .option("recursiveFileLookup", true)
      .load(dataPath)

    assert(!df.columns.contains("year"), "Expect partition inferring disabled")
    val fileList = df.select("path").collect().map(_.getString(0))

    val expectedFileList = Array(
      new java.io.File(dataPath, "year=2014/data.txt").toURI.toString,
      new java.io.File(dataPath, "year=2015/data.txt").toURI.toString)

    assert(fileList.toSet === expectedFileList.toSet)
  }

  testRapids("SPARK-22790,SPARK-27668: spark.sql.sources.compressionFactor takes effect") {
    withTempPath { workDir =>
      val workDirPath = workDir.getAbsolutePath
      val data1Path = workDirPath + "/data1"
      val data2Path = workDirPath + "/data2"
      Seq(100, 200, 300, 400).toDF("count").write.orc(data1Path)
      Seq(100, 200, 300, 400).toDF("count").write.orc(data2Path)

      var baseSize = BigInt(0)
      withSQLConf(
        SQLConf.FILE_COMPRESSION_FACTOR.key -> "1.0",
        SQLConf.USE_V1_SOURCE_LIST.key -> "orc") {
        baseSize = spark.read.orc(data1Path).queryExecution.optimizedPlan.stats.sizeInBytes
      }
      assert(baseSize > 0)
      val broadcastThreshold = (baseSize * 3 / 4).toString

      Seq(1.0, 0.5).foreach { compressionFactor =>
        withSQLConf(
          SQLConf.FILE_COMPRESSION_FACTOR.key -> compressionFactor.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> broadcastThreshold,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.USE_V1_SOURCE_LIST.key -> "orc") {
          val df1FromFile = spark.read.orc(data1Path)
          val df2FromFile = spark.read.orc(data2Path)
          val estimatedSize = df1FromFile.queryExecution.optimizedPlan.stats.sizeInBytes
          val joinedDF = df1FromFile.join(df2FromFile, Seq("count"))
          checkAnswer(joinedDF, Seq(Row(100), Row(200), Row(300), Row(400)))

          val hasGpuBroadcastHashJoin = collect(joinedDF.queryExecution.executedPlan) {
            case p if p.getClass.getName.contains("GpuBroadcastHashJoinExec") => p
          }.nonEmpty
          val hasGpuShuffledSymmetricHashJoin = collect(joinedDF.queryExecution.executedPlan) {
            case p if p.getClass.getName.contains("GpuShuffledSymmetricHashJoinExec") => p
          }.nonEmpty

          if (compressionFactor == 0.5) {
            assert(estimatedSize <= BigInt(broadcastThreshold))
            assert(hasGpuBroadcastHashJoin,
              "Expected GpuBroadcastHashJoinExec in plan:\n" +
                joinedDF.queryExecution.executedPlan)
            assert(!hasGpuShuffledSymmetricHashJoin,
              s"Did not expect GpuShuffledSymmetricHashJoinExec in plan:\n" +
                joinedDF.queryExecution.executedPlan)
          } else {
            assert(estimatedSize > BigInt(broadcastThreshold))
            assert(!hasGpuBroadcastHashJoin,
              s"Did not expect GpuBroadcastHashJoinExec in plan:\n" +
                joinedDF.queryExecution.executedPlan)
            assert(hasGpuShuffledSymmetricHashJoin,
              "Expected GpuShuffledSymmetricHashJoinExec in plan:\n" +
                joinedDF.queryExecution.executedPlan)
          }
        }
      }
    }
  }

  testRapids("File source v2: support partition pruning") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      rapidsV2FileBasedDataSources.foreach { format =>
        withTempPath { dir =>
          Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1))
            .toDF("value", "p1", "p2")
            .write
            .format(format)
            .partitionBy("p1", "p2")
            .option("header", true)
            .save(dir.getCanonicalPath)
          val df = spark
            .read
            .format(format)
            .option("header", true)
            .load(dir.getCanonicalPath)
            .where("p1 = 1 and p2 = 2 and value != \"a\"")

          assertPartitionFiltersAreNotReevaluated(df)
          val gpuScan = getGpuBatchFileScan(df)
          val (partitionFilters, dataFilters) = gpuScanFilters(gpuScan).get
          assert(partitionFilters.nonEmpty)
          assert(dataFilters.nonEmpty)
          assertGpuScanReadsOnlySelectedPartitions(gpuScan)
          checkAnswer(df, Row("b", 1, 2) :: Nil)
        }
      }
    }
  }

  testRapids("File source v2: support passing data filters to FileScan without partitionFilters") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      rapidsV2FileBasedDataSources.foreach { format =>
        withTempPath { dir =>
          Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1))
            .toDF("value", "p1", "p2")
            .write
            .format(format)
            .partitionBy("p1", "p2")
            .option("header", true)
            .save(dir.getCanonicalPath)
          val df = spark
            .read
            .format(format)
            .option("header", true)
            .load(dir.getCanonicalPath)
            .where("value = \"a\"")

          val filterCondition = df.queryExecution.optimizedPlan.collectFirst {
            case f: Filter => f.condition
          }
          assert(filterCondition.isDefined)

          val gpuScan = getGpuBatchFileScan(df)
          val (partitionFilters, dataFilters) = gpuScanFilters(gpuScan).get
          assert(partitionFilters.isEmpty)
          assert(dataFilters.nonEmpty)
          checkAnswer(df, Row("a", 1, 2) :: Nil)
        }
      }
    }
  }
}
