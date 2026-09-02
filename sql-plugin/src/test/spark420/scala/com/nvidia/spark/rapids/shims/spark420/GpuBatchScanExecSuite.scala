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
package com.nvidia.spark.rapids.shims.spark420

import com.nvidia.spark.rapids.{FQSuiteName, GpuMetric, GpuScan}
import com.nvidia.spark.rapids.GpuMetric.OP_TIME_NEW
import com.nvidia.spark.rapids.shims.GpuBatchScanExec
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table

class GpuBatchScanExecSuite
    extends AnyFunSuite with FQSuiteName with BeforeAndAfterAll with MockitoSugar {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[1]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.rapids.sql.metrics.level", "MODERATE")
      .getOrCreate()
    SparkSession.setActiveSession(spark)
  }

  override def afterAll(): Unit = {
    try {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      spark.stop()
    } finally {
      super.afterAll()
    }
  }

  test("batch scan publishes op-time companion at MODERATE level") {
    val scan = mock[GpuScan]
    when(scan.supportedCustomMetrics()).thenReturn(Array.empty)
    val batchScan = GpuBatchScanExec(
      output = Seq.empty,
      scan = scan,
      table = mock[Table])

    val publishedMetrics = GpuMetric.unwrap(batchScan.allMetrics)
    assert(publishedMetrics.keySet.contains(OP_TIME_NEW))
    assert(publishedMetrics.keySet.contains(s"${OP_TIME_NEW}_exSemWait"))
  }
}
