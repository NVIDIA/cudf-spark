/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

class GpuMetricsSuite extends AnyFunSuite {

  test("GpuMetric.ns: duplicate timing on the same metrics") {
    val m1 = new LocalGpuMetric()
    m1.ns(
      m1.ns(
        Thread.sleep(100)
      )
    )
    // if the timing is duplicated, the value should be around 0.1s
    assert(m1.value < 100000000 * 1.2)
    assert(m1.value > 100000000 * 0.8)
  }

  test("MetricRange: duplicate timing on the same metrics") {
    val m1 = new LocalGpuMetric()
    val m2 = new LocalGpuMetric()
    withResource(new MetricRange(m1, m2)) { _ =>
      withResource(new MetricRange(m2, m1)) { _ =>
        Thread.sleep(100)
      }
    }

    // if the timing is duplicated, the value should be around 0.2s
    assert(m1.value < 100000000 * 1.2)
    assert(m1.value > 100000000 * 0.8)
    assert(m2.value < 100000000 * 1.2)
    assert(m2.value > 100000000 * 0.8)
  }

  test("GpuMetric.ns: exclude multiple child metrics functionality") {
    val parentOpTime = new LocalGpuMetric()
    val child1OpTime = new LocalGpuMetric()
    val child2OpTime = new LocalGpuMetric()
    
    // First, establish baseline - child metrics start with some time
    child1OpTime.ns {
      Thread.sleep(100)
    }
    child2OpTime.ns {
      Thread.sleep(100)
    }
    
    val child1InitialValue = child1OpTime.value
    val child2InitialValue = child2OpTime.value
    
    // Test the main functionality: parent metric should exclude time from child metrics
    val excludeMetrics = Seq(child1OpTime, child2OpTime)
    parentOpTime.ns(excludeMetrics) {
      // During parent execution, children accumulate more time (simulating iter.next() calls)
      child1OpTime.ns {
        Thread.sleep(100) // This time should be EXCLUDED from parent
      }
      child2OpTime.ns {
        Thread.sleep(100) // This time should be EXCLUDED from parent
      }
      // Parent's own work
      Thread.sleep(200) // This time should be INCLUDED in parent
    }
    
    // Verify results
    val child1AdditionalTime = child1OpTime.value - child1InitialValue
    val child2AdditionalTime = child2OpTime.value - child2InitialValue

    // Assertions
    assert(parentOpTime.value > 0, "Parent metric should have recorded some time")
    assert(child1AdditionalTime > 0, "Child1 should have additional time from parent execution")
    assert(child2AdditionalTime > 0, "Child2 should have additional time from parent execution")
    
    // Parent time should be reasonable for its own work (~200ms)
    val expectedParentTime = 200 * 1000000L // 200ms in nanoseconds
    val tolerance = 50 * 1000000L // 50ms tolerance for test variance
    assert(parentOpTime.value > expectedParentTime - tolerance, // At least 150ms
      s"Parent should have at least 150ms of recorded time, got ${parentOpTime.value}ns")
    assert(parentOpTime.value < expectedParentTime + tolerance,
      s"Parent time (${parentOpTime.value}ns) should be close to expected ~200ms with tolerance")
  }
}
