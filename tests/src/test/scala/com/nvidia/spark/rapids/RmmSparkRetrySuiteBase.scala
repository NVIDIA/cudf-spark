/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{Rmm, RmmAllocationMode, RmmEventHandler}
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.spill.SpillFramework
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait RmmSparkRetrySuiteBase extends AnyFunSuite with BeforeAndAfterEach {
  private var rmmWasInitialized = false
  override def beforeEach(): Unit = {
    super.beforeEach()
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    RmmSpark.clearEventHandler()
    if (!Rmm.isInitialized) {
      rmmWasInitialized = true
      Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    }
    val sc = new SparkConf
    sc.set(RapidsConf.HOST_SPILL_STORAGE_SIZE.key, "1MB")
    val conf = new RapidsConf(sc)
    SpillFramework.shutdown()
    SpillFramework.initialize(conf)

    RmmSpark.clearEventHandler()

    val mockEventHandler = new BaseRmmEventHandler()
    RmmSpark.setEventHandler(mockEventHandler)
    RmmSpark.currentThreadIsDedicatedToTask(1)
    HostAlloc.initialize(-1)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    SpillFramework.shutdown()
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    RmmSpark.removeAllCurrentThreadAssociation()
    RmmSpark.clearEventHandler()
    GpuSemaphore.shutdown()
    if (rmmWasInitialized) {
      Rmm.shutdown()
    }
    HostAlloc.initialize(-1)
  }

  private class BaseRmmEventHandler extends RmmEventHandler {
    override def getAllocThresholds: Array[Long] = null
    override def getDeallocThresholds: Array[Long] = null
    override def onAllocThreshold(totalAllocSize: Long): Unit = {}
    override def onDeallocThreshold(totalAllocSize: Long): Unit = {}
    override def onAllocFailure(sizeRequested: Long, retryCount: Int): Boolean = {
      false
    }
  }
}
