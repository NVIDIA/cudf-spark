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

import java.util.concurrent.Callable

import org.scalatest.funsuite.AnyFunSuite

class GpuCpuBridgeThreadPoolSuite extends AnyFunSuite {

  private case class TestPriorityRunnable(priority: Long, submitTime: Long)
      extends Runnable with CpuBridgeTaskPriority {
    override def run(): Unit = ()
  }

  test("CPU bridge task comparator orders prioritized tasks before ordinary tasks") {
    val highPriority = TestPriorityRunnable(priority = 1L, submitTime = 20L)
    val lowPriority = TestPriorityRunnable(priority = 2L, submitTime = 10L)
    val earlier = TestPriorityRunnable(priority = 1L, submitTime = 10L)
    val ordinary = new Runnable {
      override def run(): Unit = ()
    }

    assert(CpuBridgeTaskComparator.compare(highPriority, lowPriority) < 0)
    assert(CpuBridgeTaskComparator.compare(highPriority, earlier) > 0)
    assert(CpuBridgeTaskComparator.compare(highPriority, ordinary) < 0)
    assert(CpuBridgeTaskComparator.compare(ordinary, highPriority) > 0)
    assert(CpuBridgeTaskComparator.compare(ordinary, ordinary) === 0)
  }

  test("CPU bridge task without TaskContext preserves callable and priority metadata") {
    val task = PrioritizedCpuBridgeTask(
      new Callable[Int] {
        override def call(): Int = 42
      },
      taskContext = null,
      submitTime = 123L)

    assert(task.priority === Long.MaxValue)
    assert(task.call() === 42)

    val future = new ComparableFutureTask(task)
    assert(future.priority === Long.MaxValue)
    assert(future.submitTime === 123L)
  }

  test("CPU bridge keeps a below-threshold batch sequential") {
    assert(GpuCpuBridgeThreadPool.getSubBatchCount(499999) === 1)
  }
}
