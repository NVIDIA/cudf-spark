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
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{doThrow, mock, when}
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

  test("NVTX closes its range when metric cleanup throws") {
    val metric = mock(classOf[GpuMetric])
    val failure = new IllegalStateException("metric cleanup failed")
    when(metric.tryActivateTimer(Seq.empty)).thenReturn(true)
    doThrow(failure).when(metric).deactivateTimer(anyLong(), any[Seq[GpuMetric]]())
    var nvtxCloses = 0
    val range = new NvtxWithMetrics("metric cleanup failure", NvtxColor.BLUE, Seq(metric)) {
      override protected def closeNvtxRange(): Unit = {
        super.closeNvtxRange()
        nvtxCloses += 1
      }
    }

    assert(nvtxCloses == 0)
    assert(intercept[IllegalStateException] {
      range.close()
    } eq failure)
    assert(nvtxCloses == 1)
  }

  test("metric ranges finish cleanup and preserve failures in deactivation order") {
    def failingMetric(failure: Throwable): GpuMetric = {
      val metric = mock(classOf[GpuMetric])
      when(metric.tryActivateTimer(Seq.empty)).thenReturn(true)
      doThrow(failure).when(metric).deactivateTimer(anyLong(), any[Seq[GpuMetric]]())
      metric
    }

    val first = new IllegalStateException("first")
    val second = new IllegalStateException("second")
    val third = new IllegalStateException("third")
    val lastMetric = new LocalGpuMetric
    val range = new MetricRange(Seq(failingMetric(first), failingMetric(first),
      failingMetric(second), failingMetric(third), lastMetric))
    assert(!lastMetric.tryActivateTimer(Seq.empty))
    assert(intercept[IllegalStateException](range.close()) eq first)
    assert(first.getSuppressed.toSeq == Seq(second, third))
    assert(lastMetric.tryActivateTimer(Seq.empty))
    lastMetric.deactivateTimer(0L, Seq.empty)
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

  test("singleton NVTX timing preserves the enclosing range and releases its timer") {
    withDebugNvtx { id =>
      val stack = RangeDebugger.threadLocalStack.get()
      val metric = new LocalGpuMetric
      val failure = new IllegalStateException("singleton NVTX body failed")
      assert(stack.isEmpty)
      id {
        assert(NvtxIdWithMetrics(id, metric) {
          assert(stack.size == 2 && stack.top == id)
          assert(!metric.tryActivateTimer(Seq.empty))
          42
        } == 42)
        assert(stack.size == 1 && stack.top == id)
        assert(metric.tryActivateTimer(Seq.empty))
        metric.deactivateTimer(0L, Seq.empty)
        assert(intercept[IllegalStateException] {
          NvtxIdWithMetrics(id, metric) {
            assert(stack.size == 2 && stack.top == id)
            assert(!metric.tryActivateTimer(Seq.empty))
            throw failure
          }
        } eq failure)
        assert(stack.size == 1 && stack.top == id)
        assert(metric.tryActivateTimer(Seq.empty))
        metric.deactivateTimer(0L, Seq.empty)
      }
      assert(stack.isEmpty)
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
