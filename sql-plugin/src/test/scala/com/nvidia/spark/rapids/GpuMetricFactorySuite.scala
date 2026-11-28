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

package com.nvidia.spark.rapids

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession

class GpuMetricFactorySuite extends AnyFunSuite with BeforeAndAfterAll {
  import GpuMetric._

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[1]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
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

  test("op-time companion metric levels") {
    val moderateFactory = new GpuMetricFactory(MODERATE_LEVEL, spark.sparkContext)
    val opTime = moderateFactory.createOpTime(DESCRIPTION_OP_TIME_NEW)
    assert(opTime != NoopMetric)
    assert(opTime.companionGpuMetric.isDefined)

    val publishedOpTime = unwrap(Map(OP_TIME_NEW -> opTime))
    assert(publishedOpTime.keySet == Set(OP_TIME_NEW, s"${OP_TIME_NEW}_exSemWait"))
    assert(publishedOpTime(OP_TIME_NEW).name.contains(DESCRIPTION_OP_TIME_NEW))
    assert(publishedOpTime(s"${OP_TIME_NEW}_exSemWait").name
      .contains(s"$DESCRIPTION_OP_TIME_NEW (excl. SemWait)"))

    val ordinaryTiming = moderateFactory.createNanoTiming(MODERATE_LEVEL, "ordinary timing")
    assert(ordinaryTiming != NoopMetric)
    assert(ordinaryTiming.companionGpuMetric.isEmpty)

    val debugFactory = new GpuMetricFactory(DEBUG_LEVEL, spark.sparkContext)
    assert(debugFactory.createNanoTiming(MODERATE_LEVEL, "debug timing")
      .companionGpuMetric.isDefined)

    val essentialFactory = new GpuMetricFactory(ESSENTIAL_LEVEL, spark.sparkContext)
    assert(essentialFactory.createOpTime(DESCRIPTION_OP_TIME_NEW) == NoopMetric)
  }

  test("op-time companions add one serialized accumulator per operator") {
    val metricCount = 100
    val moderateFactory = new GpuMetricFactory(MODERATE_LEVEL, spark.sparkContext)

    val rawOnlyMetrics = (0 until metricCount).map { index =>
      s"opTime$index" ->
        moderateFactory.createNanoTiming(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW)
    }.toMap
    val metricsWithCompanions = (0 until metricCount).map { index =>
      s"opTime$index" -> moderateFactory.createOpTime(DESCRIPTION_OP_TIME_NEW)
    }.toMap

    val rawOnlyAccumulators = unwrap(rawOnlyMetrics).values.toSeq
    val accumulatorsWithCompanions = unwrap(metricsWithCompanions).values.toSeq
    assert(rawOnlyAccumulators.size == metricCount)
    assert(accumulatorsWithCompanions.size == metricCount * 2)

    val serializer = SparkEnv.get.closureSerializer.newInstance()
    val rawOnlyBytes = serializer.serialize(rawOnlyAccumulators).remaining()
    val bytesWithCompanions = serializer.serialize(accumulatorsWithCompanions).remaining()
    info(s"Serialized accumulator payload for $metricCount op-time metrics: " +
      s"$rawOnlyBytes bytes without companions, $bytesWithCompanions bytes with companions")
    assert(bytesWithCompanions > rawOnlyBytes)
  }
}
