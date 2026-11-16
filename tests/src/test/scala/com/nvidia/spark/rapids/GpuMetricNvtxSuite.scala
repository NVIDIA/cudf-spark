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

import ai.rapids.cudf.NvtxColor
import org.scalatest.funsuite.AnyFunSuite

class GpuMetricNvtxSuite extends AnyFunSuite {
  private def withDebugNvtx(f: NvtxId => Unit): Unit = {
    val keys = Seq("ai.rapids.cudf.nvtx.enabled", "ai.rapids.cudf.nvtx.debug")
    val saved = keys.map(key => key -> Option(System.getProperty(key)))
    // NvtxId captures these flags at construction. Restore the properties before the body
    // can initialize cuDF's NvtxRange, whose native enable flag is static for the whole JVM.
    val id = try {
      keys.foreach(System.setProperty(_, "true"))
      NvtxId("metric noop NVTX regression", NvtxColor.BLUE, "Verify NVTX ownership")
    } finally {
      saved.foreach {
        case (key, Some(value)) => System.setProperty(key, value)
        case (key, None) => System.clearProperty(key)
      }
    }
    f(id)
  }

  test("NVTX remains active when every metric is disabled or already active") {
    withDebugNvtx { id =>
      val stack = RangeDebugger.threadLocalStack.get()
      assert(stack.isEmpty)
      val outer = new LocalGpuMetric
      assert(outer.tryActivateTimer(Seq.empty))
      var calls = 0
      def body(): Int = {
        calls += 1
        assert(stack.size == 1 && stack.top == id)
        42
      }
      assert(NvtxIdWithMetrics(id, NoopMetric)(body()) == 42)
      assert(stack.isEmpty)
      assert(NvtxIdWithMetrics(id, outer)(body()) == 42)
      assert(stack.isEmpty)
      assert(NvtxIdWithMetrics(id, NoopMetric, outer, NoopMetric)(body()) == 42)
      assert(stack.isEmpty)
      assert(calls == 3)
      assert(!outer.tryActivateTimer(Seq.empty))
      outer.deactivateTimer(17L, Seq.empty)
      assert(outer.value == 17L)
    }
  }

  test("NVTX closes its nested range when a disabled-metric body throws") {
    withDebugNvtx { id =>
      val stack = RangeDebugger.threadLocalStack.get()
      assert(stack.isEmpty)
      val failure = new IllegalStateException("NVTX body failed")
      id {
        assert(stack.size == 1)
        assert(intercept[IllegalStateException] {
          NvtxIdWithMetrics[Unit](id, NoopMetric, NoopMetric) {
            assert(stack.size == 2 && stack.top == id)
            throw failure
          }
        } eq failure)
        assert(stack.size == 1 && stack.top == id)
      }
      assert(stack.isEmpty)
    }
  }
}
