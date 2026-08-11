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

object MapUtil {

  /** Converts an iterable of (k, v) pairs to a map, but checks for duplicates of keys.
   */
  def toMapStrict[K, V](values: Iterable[(K, V)]): Map[K, V] = {
    val mutableMap = collection.mutable.Map.empty[K, V]
    values.foreach { case (key, value) =>
      if (mutableMap.contains(key)) {
        throw new IllegalArgumentException(s"Duplicate key found: $key")
      } else {
        mutableMap += (key -> value)
      }
    }
    mutableMap.toMap
  }
}
