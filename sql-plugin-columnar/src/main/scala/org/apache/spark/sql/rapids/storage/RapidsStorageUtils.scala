/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.storage

import java.nio.ByteBuffer

import org.apache.spark.storage.StorageUtils

object RapidsStorageUtils {
  // scalastyle:off line.size.limit
  /**
   * Calls into spark's `StorageUtils` to expose the [[dispose]] method.
   *
   * NOTE: This the spark code as of the writing of this function is:
   * https://github.com/apache/spark/blob/e9f3f62b2c0f521f3cc23fef381fc6754853ad4f/core/src/main/scala/org/apache/spark/storage/StorageUtils.scala#L206
   *
   * If the implementation in Spark later on breaks our build, we may need to replicate
   * the dispose method here.
   *
   * @param buffer byte buffer to dispose
   */
  // scalastyle:on line.size.limit
  def dispose(buffer: ByteBuffer): Unit = StorageUtils.dispose(buffer)
}
