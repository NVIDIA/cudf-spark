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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

// Scala 2.12's Seq parameter accepts mutable collections; Scala 2.13's does not.
class GpuMetricMutableInputSuite extends AnyFunSuite {
  private val nvtxId = NvtxId("mutable metrics", NvtxColor.BLUE, "Test timer ownership")
  private val entryPoints: Seq[(String, (Seq[GpuMetric], () => Unit) => Unit)] = Seq(
    "GpuMetric.ns" -> ((metrics, body) => GpuMetric.ns(metrics, Seq.empty)(body())),
    "MetricRange" -> ((metrics, body) =>
      withResource(new MetricRange(metrics)) { range =>
        assert(range.needTracks == Seq(false, false, true, false, true))
        body()
        assert(range.needTracks == Seq(false, false, true, false, true))
      }),
    "NvtxWithMetrics" -> ((metrics, body) =>
      withResource(new NvtxWithMetrics("mutable metrics", NvtxColor.BLUE, metrics))(_ => body())),
    "NvtxIdWithMetrics" -> ((metrics, body) => NvtxIdWithMetrics(nvtxId, metrics: _*)(body())))

  for ((name, run) <- entryPoints; replace <- Seq(false, true)) {
    val mutation = if (replace) "replaced" else "cleared"
    test(s"$name retains timer ownership when the input is $mutation") {
      val outer = new LocalGpuMetric
      val first = new LocalGpuMetric
      val second = new LocalGpuMetric
      val replacement = new LocalGpuMetric
      assert(outer.tryActivateTimer(Seq.empty))
      assert(replacement.tryActivateTimer(Seq.empty))
      val metrics = ArrayBuffer[GpuMetric](NoopMetric, outer, first, first, second)
      var calls = 0
      run(metrics, () => {
        calls += 1
        assert(!first.tryActivateTimer(Seq.empty))
        assert(!second.tryActivateTimer(Seq.empty))
        metrics.clear()
        if (replace) {
          // Replace all positions and grow beyond the original activation array.
          metrics ++= Seq.fill(6)(replacement)
        }
      })
      assert(calls == 1)
      assert(first.value == second.value)
      assert(first.tryActivateTimer(Seq.empty))
      assert(second.tryActivateTimer(Seq.empty))
      first.deactivateTimer(0L, Seq.empty)
      second.deactivateTimer(0L, Seq.empty)
      // Timers owned by other scopes must still be active and untouched.
      assert(!outer.tryActivateTimer(Seq.empty))
      assert(!replacement.tryActivateTimer(Seq.empty))
      assert(outer.value == 0L && replacement.value == 0L)
      outer.deactivateTimer(17L, Seq.empty)
      replacement.deactivateTimer(23L, Seq.empty)
      assert(outer.value == 17L && replacement.value == 23L)
    }
  }
}
