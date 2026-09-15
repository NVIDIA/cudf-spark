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

import scala.collection.mutable

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.vectorized.ColumnarBatch

private[rapids] final class Spark42GpuDataSourceCustomMetricsFactory(
    customMetrics: Map[String, SQLMetric]) extends GpuDataSourceCustomMetricsFactory {
  override def create(): GpuDataSourceCustomMetrics =
    new Spark42GpuDataSourceCustomMetrics(customMetrics)
}

/** Merges custom metrics from sequential and concurrently consumed Spark 4.2+ readers. */
private[rapids] class Spark42GpuDataSourceCustomMetrics(
    customMetrics: Map[String, SQLMetric]) extends GpuDataSourceCustomMetrics {
  private val activeReaders = mutable.LinkedHashSet.empty[PartitionReader[ColumnarBatch]]
  private val finishedMetrics = mutable.HashMap.empty[String, CustomTaskMetric]

  override def readerOpened(reader: PartitionReader[ColumnarBatch]): Unit = {
    activeReaders += reader
  }

  override def readerProgress(reader: PartitionReader[ColumnarBatch]): Unit = {
    updateMetrics()
  }

  override def readerFinished(reader: PartitionReader[ColumnarBatch]): Unit = {
    try {
      reader.currentMetricsValues().foreach { metric =>
        finishedMetrics.update(
          metric.name(),
          finishedMetrics.get(metric.name()).fold(metric)(_.mergeWith(metric)))
      }
    } finally {
      activeReaders -= reader
    }
    updateMetrics()
  }

  private def updateMetrics(): Unit = {
    val merged = mutable.HashMap.empty[String, CustomTaskMetric]
    finishedMetrics.foreach { case (name, metric) => merged.update(name, metric) }
    activeReaders.iterator.flatMap(_.currentMetricsValues()).foreach { metric =>
      merged.update(metric.name(), merged.get(metric.name()).fold(metric)(_.mergeWith(metric)))
    }
    CustomMetrics.updateMetrics(merged.values.toSeq, customMetrics)
  }
}
