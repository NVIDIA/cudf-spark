/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.resource.ResourceInformation

class GpuDeviceManagerSuite extends AnyFunSuite {

  test("Test Spark gpu resource") {
    val sparkConf = new SparkConf()
    val conf = new RapidsConf(sparkConf)
    val gpu = new ResourceInformation("gpu", Array("3"))
    val resources = Map("gpu" -> gpu)
    val gpuAddr = GpuDeviceManager.getGPUAddrFromResources(resources, conf)
    assert(gpuAddr.nonEmpty)
    assert(gpuAddr.get == 3)
  }

  test("Test Spark custom resource missed") {
    val sparkConf = new SparkConf()
    val conf = new RapidsConf(sparkConf)
    val gpu = new ResourceInformation("nvidia/gpu", Array("2"))
    val resources = Map("nvidia/gpu" -> gpu)
    val gpuAddr = GpuDeviceManager.getGPUAddrFromResources(resources, conf)
    assert(gpuAddr.isEmpty)
  }

  test("Test Spark multiple GPUs throws") {
    val sparkConf = new SparkConf()
    val conf = new RapidsConf(sparkConf)
    val gpu = new ResourceInformation("gpu", Array("2", "3"))
    val resources = Map("gpu" -> gpu)
    assertThrows[IllegalArgumentException](
      GpuDeviceManager.getGPUAddrFromResources(resources, conf))
  }

  test("Test Spark custom resource") {
    val sparkConf = new SparkConf()
    sparkConf.set(RapidsConf.SPARK_GPU_RESOURCE_NAME.toString, "nvidia/gpu")
    val conf = new RapidsConf(sparkConf)
    val gpu = new ResourceInformation("nvidia/gpu", Array("1"))
    val resources = Map("nvidia/gpu" -> gpu)
    val gpuAddr = GpuDeviceManager.getGPUAddrFromResources(resources, conf)
    assert(gpuAddr.nonEmpty)
    assert(gpuAddr.get == 1)
  }
}
