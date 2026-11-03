/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}

/**
 * Spark BuildSide, BuildRight, BuildLeft moved packages in Spark 3.1
 * so create GPU versions of these that can be agnostic to Spark version.
 */
sealed abstract class GpuBuildSide

case object GpuBuildRight extends GpuBuildSide

case object GpuBuildLeft extends GpuBuildSide

object GpuJoinUtils {
  def getGpuBuildSide(buildSide: BuildSide): GpuBuildSide = {
    buildSide match {
      case BuildRight => GpuBuildRight
      case BuildLeft => GpuBuildLeft
      case _ => throw new Exception(s"unknown build side type $buildSide")
    }
  }
}
