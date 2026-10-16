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

package com.nvidia.spark.rapids.spill

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import ai.rapids.cudf.DeviceMemoryBuffer
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.SparkConf

private[spill] object SpillReleaseConformanceTrace {
  case class Observation(
      closed: Boolean,
      spilling: Boolean,
      copyPublished: Boolean,
      hostOwned: Boolean,
      handleOwnsDeviceRef: Boolean,
      syncComplete: Boolean) {
    def violatesReleaseOrder: Boolean =
      copyPublished && !syncComplete && !handleOwnsDeviceRef
  }

  case class Trace(states: Vector[Observation], actions: Vector[String]) {
    require(states.size == actions.size + 1)

    val edges: Set[(Long, Long)] =
      states.indices.dropRight(1).map(index => index.toLong -> (index + 1).toLong).toSet +
        ((states.size - 1).toLong -> (states.size - 1).toLong)

    def shortestViolation: Option[Vector[String]] = {
      val index = states.indexWhere(_.violatesReleaseOrder)
      if (index < 0) None else Some(actions.take(index))
    }
  }

  private class PausingDeviceStore extends SpillableDeviceStore {
    val beforeRelease = new CountDownLatch(1)
    val resumeRelease = new CountDownLatch(1)

    override def postSpill(plan: SpillPlan): Unit = {
      beforeRelease.countDown()
      assert(resumeRelease.await(15, TimeUnit.SECONDS))
      super.postSpill(plan)
    }
  }

  private def observe(handle: SpillableDeviceBufferHandle, copyPublished: Boolean,
      syncComplete: Boolean): Observation = {
    Observation(handle.closed, handle.spilling, copyPublished, handle.host.isDefined,
      handle.dev.isDefined, syncComplete)
  }

  def capture(): Trace = {
    val conf = new SparkConf()
      .set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1024")
      .set(RapidsConf.OFF_HEAP_LIMIT_ENABLED.key, "false")
    SpillFramework.initialize(new RapidsConf(conf))
    val deviceStore = new PausingDeviceStore
    SpillFramework.stores.deviceStore = deviceStore
    val executor = Executors.newSingleThreadExecutor()
    try {
      val handle = closeOnExcept(DeviceMemoryBuffer.allocate(64)) { buffer =>
        SpillableDeviceBufferHandle(buffer)
      }
      withResource(handle) { _ =>
        val initial = observe(handle, copyPublished = false, syncComplete = false)
        val spill = executor.submit(new Callable[Long] {
          override def call(): Long = deviceStore.spill(handle.approxSizeInBytes)
        })
        try {
          assert(deviceStore.beforeRelease.await(15, TimeUnit.SECONDS))
          val published = observe(handle, copyPublished = true, syncComplete = false)
          assert(published.hostOwned && published.handleOwnsDeviceRef)
          handle.close()
          val closed = observe(handle, copyPublished = true, syncComplete = false)
          deviceStore.resumeRelease.countDown()
          assert(spill.get(15, TimeUnit.SECONDS) == handle.approxSizeInBytes)
          val finished = observe(handle, copyPublished = true, syncComplete = true)
          assert(finished.closed && !finished.handleOwnsDeviceRef)
          Trace(Vector(initial, published, closed, finished),
            Vector("StartSpillAndPublishHost", "Close", "SynchronizeAndRelease"))
        } finally {
          deviceStore.resumeRelease.countDown()
        }
      }
    } finally {
      deviceStore.resumeRelease.countDown()
      executor.shutdown()
      if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
        executor.shutdownNow()
      }
      SpillFramework.shutdown()
    }
  }
}
