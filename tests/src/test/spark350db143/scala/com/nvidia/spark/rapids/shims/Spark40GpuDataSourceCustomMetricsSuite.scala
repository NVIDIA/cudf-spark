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
{"spark": "350db143"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.mockito.Mockito.{verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

class Spark40And41GpuDataSourceCustomMetricsSuite extends AnyFunSuite with MockitoSugar {
  test("updates Spark 4.0 and 4.1.1 custom metrics on progress and reader finish") {
    val sqlMetric = mock[SQLMetric]
    val reader = mock[PartitionReader[ColumnarBatch]]
    when(reader.currentMetricsValues())
      .thenReturn(Array(metric(17L)))
      .thenReturn(Array(metric(19L)))

    val factory = new Spark4GpuDataSourceCustomMetricsFactory(Map("metric" -> sqlMetric))
    val handler = factory.create()
    assert(factory.create() ne handler)
    handler.readerOpened(reader)
    handler.readerProgress(reader)
    handler.readerFinished(reader)

    verify(sqlMetric).set(17L)
    verify(sqlMetric).set(19L)
  }

  private def metric(metricValue: Long): CustomTaskMetric = new CustomTaskMetric {
    override def name(): String = "metric"
    override def value(): Long = metricValue
  }
}
