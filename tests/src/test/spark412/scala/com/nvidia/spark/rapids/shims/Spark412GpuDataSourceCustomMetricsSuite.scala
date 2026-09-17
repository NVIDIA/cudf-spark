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
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.mockito.Mockito.{verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

class Spark412GpuDataSourceCustomMetricsSuite extends AnyFunSuite with MockitoSugar {
  test("carries cumulative custom metrics into the next grouped reader") {
    val sqlMetric = mock[SQLMetric]
    val firstReader = mock[PartitionReader[ColumnarBatch]]
    val secondReader = mock[PartitionReader[ColumnarBatch]]
    val firstMetric = new CustomTaskMetric {
      override def name(): String = "metric"
      override def value(): Long = 17L
    }
    val firstSnapshot = Array(firstMetric)
    when(firstReader.currentMetricsValues()).thenReturn(firstSnapshot)

    val factory = new Spark4GpuDataSourceCustomMetricsFactory(Map("metric" -> sqlMetric))
    val handler = factory.create()
    assert(factory.create() ne handler)
    handler.readerOpened(firstReader)
    handler.readerFinished(firstReader)
    handler.readerOpened(secondReader)

    verify(sqlMetric).set(17L)
    verify(secondReader).initMetricsValues(firstSnapshot)
  }
}
