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

package org.apache.spark.sql.rapids

import java.util.{Set => JSet}
import java.util.concurrent.ConcurrentHashMap

/**
 * The driver's set of GPU shuffle ids that the UCX shuffle reclaim may reclaim. It exists only
 * while a driver plugin that admitted the reclaim runs. Spark's threads call `add` and `remove`
 * in every mode, so each does nothing without a registry and never throws.
 */
object GpuShuffleRegistry {
  @volatile private var shuffleIds: JSet[Int] = _

  /** Installs an empty registry, when the driver plugin admits the reclaim. */
  def init(): Unit = {
    shuffleIds = ConcurrentHashMap.newKeySet[Int]()
  }

  /** The candidate shuffle ids, or null when no running driver plugin admitted the reclaim. */
  def get: JSet[Int] = shuffleIds

  /** Removes the registry, when the driver plugin shuts down. */
  def shutdown(): Unit = {
    shuffleIds = null
  }

  /** Makes a shuffle a candidate, if a registry exists. */
  def add(shuffleId: Int): Unit = {
    val ids = shuffleIds
    if (ids != null) {
      ids.add(shuffleId)
    }
  }

  /** Forgets a shuffle, if a registry exists. */
  def remove(shuffleId: Int): Unit = {
    val ids = shuffleIds
    if (ids != null) {
      ids.remove(shuffleId)
    }
  }
}
