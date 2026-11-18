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

package org.apache.spark.sql.rapids

import org.scalatest.funsuite.AnyFunSuite

class GpuTaskMetricsHostSuite extends AnyFunSuite {
  test("owned peak tracks simultaneously live bytes and preserves its high water mark") {
    val metrics = new GpuTaskMetrics
    metrics.recordOwnedHost(100, isPinned = true)
    metrics.recordOwnedHost(200, isPinned = false)
    assert(metrics.ownedHostCurrent == 300)
    metrics.recordOwnedHost(-100, isPinned = true)
    metrics.recordOwnedHost(50, isPinned = false)
    assert(metrics.ownedHostCurrent == 250)
    assert(metrics.ownedHostPeak == 300)
  }

  test("release on a different thread updates the original owner's live bytes") {
    val owner = new GpuTaskMetrics
    val other = new GpuTaskMetrics
    owner.recordOwnedHost(100, isPinned = false)
    val thread = new Thread(new Runnable {
      override def run(): Unit = owner.recordOwnedHost(-100, isPinned = false)
    })
    thread.start()
    thread.join()
    assert(owner.ownedHostCurrent == 0)
    assert(owner.ownedHostPeak == 100)
    assert(other.ownedHostCurrent == 0)
    assert(other.ownedHostPeak == 0)
  }
}
