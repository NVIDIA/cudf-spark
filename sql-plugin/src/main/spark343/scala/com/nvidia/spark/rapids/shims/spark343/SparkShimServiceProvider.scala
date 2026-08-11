/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "343"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.spark343

import com.nvidia.spark.rapids.SparkShimVersion

object SparkShimServiceProvider {
  val VERSION = SparkShimVersion(3, 4, 3)
  val VERSIONNAMES = Seq(s"$VERSION")
}

class SparkShimServiceProvider extends com.nvidia.spark.rapids.SparkShimServiceProvider {

  override def getShimVersion: SparkShimVersion = SparkShimServiceProvider.VERSION

  override def matchesVersion(version: String): Boolean = {
    SparkShimServiceProvider.VERSIONNAMES.contains(version)
  }
}
