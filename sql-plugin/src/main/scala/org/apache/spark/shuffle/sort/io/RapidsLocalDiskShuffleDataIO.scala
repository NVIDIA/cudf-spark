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

package org.apache.spark.shuffle.sort.io

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.api.{ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents}

/**
 * RAPIDS-optimized implementation of ShuffleDataIO that uses host memory buffers
 * for partial sorted files when possible, with automatic spill to disk support.
 */
class RapidsLocalDiskShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {

  override def executor(): ShuffleExecutorComponents = {
    new RapidsLocalDiskShuffleExecutorComponents(sparkConf)
  }

  override def driver(): ShuffleDriverComponents = {
    new LocalDiskShuffleDriverComponents()
  }
}

