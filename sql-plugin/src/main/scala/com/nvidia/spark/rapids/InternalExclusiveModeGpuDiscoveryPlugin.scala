/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.Cuda

import org.apache.spark.SparkConf
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.resource.{ResourceInformation, ResourceRequest}

/**
 * Note, this class should not be referenced directly in source code.
 * It should be loaded by reflection using ShimLoader.newInstanceOf, see ./docs/dev/shims.md
 */
protected class InternalExclusiveModeGpuDiscoveryPlugin
  extends ResourceDiscoveryPlugin with Logging {
  override def discoverResource(
    request: ResourceRequest,
    sparkconf: SparkConf
  ): Optional[ResourceInformation] = {

    val resourceName = request.id.resourceName
    if (!resourceName.equals("gpu")) {
      logInfo("ExclusiveModeGpuDiscoveryPlugin only handles gpu allocations, " +
        s"skipping $resourceName")
      return Optional.empty()
    }
    val ngpusRequested = request.amount
    val deviceCount: Int = Cuda.getDeviceCount
    logInfo(s"Running ExclusiveModeGpuDiscoveryPlugin to acquire $ngpusRequested GPU(s), " +
      s"host has $deviceCount GPU(s)")
    // loop multiple times to see if a GPU was released or something unexpected happened that
    // we couldn't acquire on first try
    var numRetries = 2
    val allocatedAddrs = ArrayBuffer[String]()
    val addrsToTry = ArrayBuffer.empty ++= (0 until deviceCount)
    while (numRetries > 0 && allocatedAddrs.size < ngpusRequested && addrsToTry.nonEmpty) {
      var addrLoc = 0
      val allAddrs = addrsToTry.size
      while (addrLoc < allAddrs && allocatedAddrs.size < ngpusRequested) {
        val addr = addrsToTry(addrLoc)
        if (GpuDeviceManager.tryToSetGpuDeviceAndAcquire(addr)) {
          allocatedAddrs += addr.toString
        }
        addrLoc += 1
      }
      addrsToTry --= allocatedAddrs.map(_.toInt)
      numRetries -= 1
    }
    if (allocatedAddrs.size < ngpusRequested) {
      // log warning here, Spark will throw exception if we return not enough
      logWarning(s"ExclusiveModeGpuDiscoveryPlugin did not find enough gpus, " +
        s"requested: $ngpusRequested found: ${allocatedAddrs.size}")
    }
    Optional.of(new ResourceInformation("gpu", allocatedAddrs.toArray))
  }
}
