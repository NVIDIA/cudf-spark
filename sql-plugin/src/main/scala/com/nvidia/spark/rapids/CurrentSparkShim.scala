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

/**
 * Access the selected Spark shim without leaving static references to SparkShimImpl in
 * root-layout classes. Those classes are loaded by Spark's conventional classloader,
 * while SparkShimImpl only exists in the selected parallel-world shim.
 */
object CurrentSparkShim {
  private lazy val current: SparkShims = {
    val shimClass = ShimReflectionUtils.loadClass("com.nvidia.spark.rapids.shims.SparkShimImpl$")
    shimClass.getField("MODULE$").get(null).asInstanceOf[SparkShims]
  }

  def get: SparkShims = current
}
