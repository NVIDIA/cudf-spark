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
package com.nvidia.spark.rapids

import java.io.{File, FileNotFoundException}

import com.nvidia.spark.rapids.shims.{GpuBatchScanExec, PartitionedFileUtilsShim}

import org.apache.spark.{SparkConf, SparkException, SparkThrowable}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.shims.GpuFileScanRDD

class MissingFileStructuredErrorSuite extends SparkQueryCompareTestSuite {
  private val missingFileCondition = "FAILED_READ_FILE.FILE_NOT_EXIST"

  private def deleteOneDataFile(directory: File): File = {
    val file = directory.listFiles().find { candidate =>
      !candidate.getName.startsWith("_") && !candidate.getName.startsWith(".")
    }.getOrElse(fail(s"No data file found in $directory"))
    assert(file.delete(), s"Failed to delete $file")
    file
  }

  private def structuredMissingFile(error: Throwable): SparkThrowable = {
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).collectFirst {
      case sparkError: SparkThrowable if sparkError.getCondition == missingFileCondition =>
        sparkError
    }.getOrElse(fail(s"No $missingFileCondition error found in $error"))
  }

  private def readAfterDeletingPlannedFile(
      spark: SparkSession,
      useV1: Boolean,
      verifyGpuPlan: Boolean): String = {
    withTempPath { location =>
      spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
        .write.orc(location.getAbsolutePath)

      val df = spark.read.orc(location.getAbsolutePath)
      assert(df.count() == 100)

      if (verifyGpuPlan) {
        val plan = df.queryExecution.executedPlan
        val hasExpectedGpuScan = if (useV1) {
          plan.find(_.isInstanceOf[GpuFileSourceScanExec]).nonEmpty
        } else {
          plan.find(_.isInstanceOf[GpuBatchScanExec]).nonEmpty
        }
        assert(hasExpectedGpuScan, s"Expected a GPU ${if (useV1) "V1" else "V2"} scan:\n$plan")
      }

      val deletedFile = deleteOneDataFile(location)
      val error = structuredMissingFile(intercept[SparkException](df.count()))
      val expectedPath = deletedFile.toPath.toUri.toString
      assert(error.getMessageParameters.get("path") == expectedPath)
      error.getCondition
    }
  }

  Seq(("V1", true, "orc"), ("V2", false, "")).foreach {
    case (sourceName, useV1, v1Sources) =>
      val readerTypes = Seq(RapidsReaderType.COALESCING, RapidsReaderType.MULTITHREADED) ++
        (if (useV1) Seq(RapidsReaderType.PERFILE) else Seq.empty)
      readerTypes.foreach { readerType =>
        test(s"Spark 4 missing-file structured error parity - $sourceName - $readerType") {
          val conf = new SparkConf()
            .set(SQLConf.USE_V1_SOURCE_LIST.key, v1Sources)
            .set(SQLConf.IGNORE_MISSING_FILES.key, "false")
            .set(RapidsConf.ORC_READER_TYPE.key, readerType.toString)

          val cpuCondition = withCpuSparkSession(
            readAfterDeletingPlannedFile(_, useV1, verifyGpuPlan = false), conf)
          val gpuCondition = withGpuSparkSession(
            readAfterDeletingPlannedFile(_, useV1, verifyGpuPlan = true), conf)

          assert(gpuCondition == cpuCondition)
          assert(gpuCondition == missingFileCondition)
        }
      }
  }

  test("Spark 4 copied V1 per-file RDD attaches the missing-file path") {
    withCpuSparkSession { spark =>
      withTempPath { missingFile =>
        val missingPath = missingFile.toPath.toUri.toString
        val partitionedFile = PartitionedFileUtilsShim.newPartitionedFile(
          InternalRow.empty, missingPath, start = 0L, length = 1L)
        val filePartitions = FilePartition.getFilePartitions(
          spark, Seq(partitionedFile), maxSplitBytes = Long.MaxValue)
        val readFunction = (_: PartitionedFile) => new Iterator[InternalRow] {
          override def hasNext: Boolean =
            throw new FileNotFoundException("missing ORC file")
          override def next(): InternalRow = throw new NoSuchElementException
        }
        val rdd = new GpuFileScanRDD(spark, readFunction, filePartitions)

        val error = structuredMissingFile(intercept[SparkException](rdd.collect()))
        assert(error.getMessageParameters.get("path") === missingPath)
      }
    }
  }
}
