/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark

import java.util.Optional

import com.nvidia.spark.rapids.ShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.api.resource.ResourceDiscoveryPlugin
import org.apache.spark.resource.{ResourceInformation, ResourceRequest}

/**
 *  A Spark Resource Discovery Plugin that relies on the NVIDIA GPUs being in PROCESS_EXCLUSIVE
 *  mode so that it can discover free GPUs.
 *  This plugin iterates through all the GPUs on the node and tries to initialize a CUDA context
 *  on each one. When the GPUs are in process exclusive mode this
 *  will result in that GPU being assigned to the specific process running this plugin and
 *  other executors will not be able to use it.
 *
 *  This plugin can be activated in spark with the configuration:
 *  `--conf spark.resources.discoveryPlugin=com.nvidia.spark.ExclusiveModeGpuDiscoveryPlugin`
 */
class ExclusiveModeGpuDiscoveryPlugin extends ResourceDiscoveryPlugin {
  private lazy val realImpl = ShimLoader.newInternalExclusiveModeGpuDiscoveryPlugin()

  override def discoverResource(
    request: ResourceRequest,
    sparkConf: SparkConf
  ): Optional[ResourceInformation] = realImpl.discoverResource(request, sparkConf)
}
