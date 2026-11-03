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
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.mockito.Mockito.{verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

class Spark42GpuDataSourceCustomMetricsSuite extends AnyFunSuite with MockitoSugar {
  test("merges custom metrics from all readers in a coalesced task") {
    val sqlMetric = mock[SQLMetric]
    val firstReader = mock[PartitionReader[ColumnarBatch]]
    val secondReader = mock[PartitionReader[ColumnarBatch]]
    when(firstReader.currentMetricsValues()).thenReturn(Array(metric(3L)))
    when(secondReader.currentMetricsValues()).thenReturn(Array(metric(5L)))

    val factory = new Spark42GpuDataSourceCustomMetricsFactory(Map("metric" -> sqlMetric))
    val handler = factory.create()
    assert(factory.create() ne handler)
    handler.readerOpened(firstReader)
    handler.readerFinished(firstReader)
    handler.readerOpened(secondReader)
    handler.readerProgress(secondReader)

    verify(sqlMetric).set(8L)
  }

  test("removes a finished reader when collecting its metrics fails") {
    val sqlMetric = mock[SQLMetric]
    val failure = new IllegalStateException("metric collection failed")
    val failingReader = new PartitionReader[ColumnarBatch] {
      private var failMetricCollection = true

      override def next(): Boolean = false
      override def get(): ColumnarBatch = throw new UnsupportedOperationException
      override def close(): Unit = {}
      override def currentMetricsValues(): Array[CustomTaskMetric] = {
        if (failMetricCollection) {
          failMetricCollection = false
          throw failure
        }
        Array(metric(3L))
      }
    }
    val activeReader = mock[PartitionReader[ColumnarBatch]]
    when(activeReader.currentMetricsValues()).thenReturn(Array(metric(5L)))

    val handler = new Spark42GpuDataSourceCustomMetricsFactory(
      Map("metric" -> sqlMetric)).create()
    handler.readerOpened(failingReader)
    val thrown = intercept[IllegalStateException](handler.readerFinished(failingReader))
    assert(thrown eq failure)

    handler.readerOpened(activeReader)
    handler.readerProgress(activeReader)
    verify(sqlMetric).set(5L)
  }

  private def metric(metricValue: Long): CustomTaskMetric = new CustomTaskMetric {
    override def name(): String = "metric"
    override def value(): Long = metricValue
    override def mergeWith(other: CustomTaskMetric): CustomTaskMetric = metric(
      metricValue + other.value())
  }
}
