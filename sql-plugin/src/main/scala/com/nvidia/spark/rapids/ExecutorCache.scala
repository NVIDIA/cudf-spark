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

import java.lang.management.ManagementFactory

import ai.rapids.cudf.{Cuda, CudaComputeMode}

/**
 * A singleton object to cache executor related information.
 * Uses lazy mode to ensure the values are only computed once per executor.
 */
object ExecutorCache {

  /**
   * Cache the current device compute mode for current executor.
   * It's based on the assumption that executor has been assigned to a single device,
   * and will not change during the lifetime of the executor.
   * Should be called on executor side.
   */
  private[rapids] lazy val getCurrentDeviceComputeMode: CudaComputeMode = Cuda.getComputeMode()

  /**
   * Cache the current device UUID for current executor.
   * It's based on the assumption that executor has been assigned to a single device,
   * and will not change during the lifetime of the executor.
   * Should be called on executor side.
   */
  private[rapids] lazy val getCurrentDeviceUuid: Array[Byte] = Cuda.getGpuUuid()

  /**
   * Cache the current process name for current executor.
   * Should be called on executor side.
   */
  private[rapids] lazy val getProcessName: String = ManagementFactory.getRuntimeMXBean.getName
}

