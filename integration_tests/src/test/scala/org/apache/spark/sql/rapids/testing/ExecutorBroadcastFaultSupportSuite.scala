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

package org.apache.spark.sql.rapids.testing

import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.rapids.shims.TrampolineUtilShim

class ExecutorBroadcastFaultSupportSuite extends AnyFunSuite {
  test("cancellation readiness accepts a completed build or a completed cache borrower") {
    TrampolineUtilShim.cleanupAnyExistingSession()
    val session = SparkSession.builder().master("local[2]")
      .appName("driverless-cancellation-readiness")
      .config("spark.plugins", "")
      .config("spark.sql.extensions", "")
      .config("spark.shuffle.manager", "sort")
      .config("spark.ui.enabled", false)
      .getOrCreate()
    try {
      session.withActive {
        withResource(ExecutorBroadcastFaultSupport.cancellationProbe(session)) { probe =>
          val builds = SQLMetrics.createMetric(session.sparkContext, "builds")
          val hits = SQLMetrics.createMetric(session.sparkContext, "hits")
          assert(!probe.awaitBuildMetric(builds, hits, 0))
          hits.set(1)
          assert(probe.awaitBuildMetric(builds, hits, 0))
          hits.set(0)
          builds.set(1)
          assert(probe.awaitBuildMetric(builds, hits, 0))
          builds.set(0)
          assert(!probe.awaitBuildMetric(builds, hits, 0))
        }
      }
    } finally {
      session.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  Seq(
    (1, 4, 1, 1),
    (2, 1, 1, 2),
    (2, 4, 2, 1),
    (4, 1, 2, 2)).foreach { case (cores, parallelism, taskCores, expectedSlots) =>
    test(s"local[$cores], defaultParallelism=$parallelism, taskCores=$taskCores") {
      // Each case needs its own context-level settings, regardless of preceding suites.
      TrampolineUtilShim.cleanupAnyExistingSession()
      val session = SparkSession.builder().master(s"local[$cores]")
        .appName("driverless-local-task-capacity")
        .config("spark.default.parallelism", parallelism)
        .config("spark.task.cpus", taskCores)
        .config("spark.plugins", "")
        .config("spark.sql.extensions", "")
        .config("spark.shuffle.manager", "sort")
        .config("spark.ui.enabled", false)
        .getOrCreate()
      try {
        assert(session.sparkContext.defaultParallelism == parallelism)
        assert(ExecutorBroadcastFaultSupport.localTaskSlots(session) == expectedSlots)
        if (expectedSlots < 2) {
          val error = intercept[IllegalArgumentException] {
            ExecutorBroadcastFaultSupport.awaitParallelBuildSlots(session)
          }
          assert(error.getMessage.contains("requires two local CPU task slots"))
        } else {
          ExecutorBroadcastFaultSupport.awaitParallelBuildSlots(session)
        }
      } finally {
        session.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }
}
