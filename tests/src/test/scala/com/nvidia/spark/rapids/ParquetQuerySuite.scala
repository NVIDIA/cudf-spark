/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import org.apache.hadoop.fs.FileUtil.fullyDelete

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.rapids.{TestArray, TestArrayUDT, TestNestedStructUDT, TestPrimitive, TestPrimitiveUDT}
import org.apache.spark.sql.rapids.TestingUDT._
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.types._

/**
 * This corresponds to the Spark class:
 * org.apache.spark.sql.execution.datasources.parquet.ParquetQuerySuite
 */
class ParquetQuerySuite extends SparkQueryCompareTestSuite {

  private def getSchema: StructType = new StructType(Array(
      StructField("col1", ArrayType(new TestPrimitiveUDT())),
      StructField("col2", ArrayType(new TestArrayUDT())),
      StructField("col3", ArrayType(new TestNestedStructUDT()))
  ))

  private def getData: Seq[Row] = (0 until 2).map { _ =>
    Row(
      Seq(new TestPrimitive(1)),
      Seq(new TestArray(Seq(1L, 2L, 3L))),
      Seq(new TestNestedStruct(1, 2L, 3.0)))
  }

  private def getDf(spark: SparkSession): DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(getData),
      getSchema)
  }

  Seq("parquet", "").foreach { v1List =>
    val sparkConf = new SparkConf().set("spark.sql.sources.useV1SourceList", v1List)
    testGpuWriteFallback(
      "Writing UDT in ColumnVector fall back, source list is (" + v1List + ")",
      "DataWritingCommandExec",
      spark => getDf(spark),
      // WriteFilesExec is a new operator from Spark version 340, for simplicity, add it here for
      // all Spark versions.
      execsAllowedNonGpu = Seq("DataWritingCommandExec", "WriteFilesExec", "ShuffleExchangeExec"),
      conf = sparkConf
    ) { frame =>
      val tempFile = File.createTempFile("parquet-test-udt-write", ".parquet")
      try {
        frame.write.mode("overwrite").parquet(tempFile.getAbsolutePath())
      } finally {
        fullyDelete(tempFile)
      }
    }
  }

  testGpuReadFallback("Reading UDT in ColumnVector, falls back when specify schema",
    "FileSourceScanExec",
    (file: File) => (spark: SparkSession) => {
      spark.read.parquet(file.getCanonicalPath)
    },
    (spark: SparkSession, file: File) => {
      val df = getDf(spark)
      df.write.mode("overwrite").parquet(file.getCanonicalPath)
    },
    execsAllowedNonGpu = Seq("ColumnarToRowExec", "FileSourceScanExec", "ShuffleExchangeExec")
  ) {
    frame => frame
  }
}
