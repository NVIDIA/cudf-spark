/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

class GpuFileScanPrunePartitionSuite extends SparkQueryCompareTestSuite {

  private def testGpuFileScanOutput(
      func: DataFrame => DataFrame,
      conf: SparkConf,
      exPartitionCols: String*): Unit = {
    withTempPath { file =>
      // Generate partitioned files.
      withCpuSparkSession(spark => {
        import spark.implicits._
        Seq((1, 11, "s1"), (2, 22, "s2"), (3, 33, "s3"), (4, 44, "s4"), (5, 55, "s5"))
          .toDF("a", "b", "c").write.partitionBy("b", "c").parquet(file.getCanonicalPath)
      })

      val allConf = conf.clone().set("spark.sql.sources.useV1SourceList", "parquet")
      // Run the input function
      val df = withGpuSparkSession(spark => func(spark.read.parquet(file.toString)), allConf)

      // check the output of GpuFileSourceScan
      val plans = df.queryExecution.executedPlan.collect {
        case plan: GpuFileSourceScanExec =>
          // all the excluded partition columns should not exist in the output
          exPartitionCols.foreach(colName =>
            assert(!plan.output.exists(a => a.name == colName),
              s"Partition column '$colName' is excluded, but found in the output of FileScan")
          )
          plan
      }
      assert(plans.nonEmpty, "GpuFileSourceScan is not found")
    }
  }

  test("Prune Partition Columns when GpuProject(GpuFileSourceScan)") {
    Seq(true, false).foreach { enabled =>
      val conf = new SparkConf()
        .set("spark.rapids.sql.exec.ProjectExec", enabled.toString)
      if (!enabled) {
        conf.set(RapidsConf.TEST_ALLOWED_NONGPU.key, "ProjectExec")
      }
      testGpuFileScanOutput(_.groupBy("b").max("a"), conf,"c")
      testGpuFileScanOutput(_.select("a"), conf, "b", "c")
      testGpuFileScanOutput(_.select("a", "c"), conf, "b")
    }
  }

  test("Prune Partition Columns when GpuProject(GpuFilter(GpuFileSourceScan))") {
    Seq((true, true), (true, false), (false, true), (false, false))
      .foreach { case (gpuProjectEnabled, gpuFilterEnabled) =>
        val conf = new SparkConf()
          .set("spark.rapids.sql.exec.ProjectExec", gpuProjectEnabled.toString)
          .set("spark.rapids.sql.exec.FilterExec", gpuFilterEnabled.toString)
        if (!gpuProjectEnabled || !gpuFilterEnabled) {
          val nonGpuExecStr = if (!gpuProjectEnabled && gpuFilterEnabled) {
            "ProjectExec"
          } else if (gpuProjectEnabled && !gpuFilterEnabled) {
            "FilterExec"
          } else {
            "ProjectExec,FilterExec"
          }
          conf.set(RapidsConf.TEST_ALLOWED_NONGPU.key, nonGpuExecStr)
        }
        testGpuFileScanOutput(_.select("a", "c").filter("a != 1"), conf, "b")
        testGpuFileScanOutput(_.select("a").filter("a != 1"), conf, "b", "c")
    }
  }

  test("Canonicalized GpuFileSourceScans with partition columns pruned should be equal") {
    withTempPath { file =>
      // Generate partitioned files.
      withCpuSparkSession(spark => {
        import spark.implicits._
        Seq((1, 11, "s1"), (2, 22, "s2"), (3, 33, "s3"), (4, 44, "s4"), (5, 55, "s5"))
          .toDF("a", "b", "c").write.partitionBy("b", "c").parquet(file.getCanonicalPath)
      })

      def getDF: DataFrame = withGpuSparkSession(spark =>
        spark.read.parquet(file.toString).where("b == 33").select("a", "c"),
        new SparkConf().set("spark.sql.sources.useV1SourceList", "parquet")
      )

      val plans = getDF.unionAll(getDF).queryExecution.executedPlan.collect {
        case gfss: GpuFileSourceScanExec if gfss.requiredPartitionSchema.nonEmpty => gfss
      }
      assert(plans.length == 2, "Expect 2 GPU file scans")
      assert(plans.head.canonicalized == plans.last.canonicalized,
          "Canonicalized GPU file scans should be equal")
    }
  }
}
