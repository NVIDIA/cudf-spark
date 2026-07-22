/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.GpuMetric
import org.apache.hadoop.conf.Configuration
import org.apache.orc.impl.DataReaderProperties

class GpuOrcDataReader(
    props: DataReaderProperties,
    conf: Configuration,
    metrics: Map[String, GpuMetric]) extends GpuOrcDataReader320Plus(props, conf, metrics) {
  override def releaseAllBuffers(): Unit = {
    throw new IllegalStateException("should not be trying to release buffers")
  }
}


object GpuOrcDataReader {
  // File cache is being used, so we want read ranges that can be cached separately
  val shouldMergeDiskRanges: Boolean = false
}
