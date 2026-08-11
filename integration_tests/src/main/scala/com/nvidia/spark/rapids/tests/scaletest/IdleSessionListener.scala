/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests.scaletest

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}

class IdleSessionListener extends SparkListener{

  val runningJobCount = new AtomicLong(0)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // A new job has started; increment the running job count.
    runningJobCount.incrementAndGet()
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    // A job has ended; decrement the running job count.
    runningJobCount.decrementAndGet()
  }

  def isIdle(): Boolean = {
    // Determine idleness based on the running job count.
    runningJobCount.get() == 0
  }

  def isBusy(): Boolean = {
    !isIdle()
  }

}
