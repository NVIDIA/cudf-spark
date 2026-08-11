/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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
 * Utility methods for managing spillable buffer priorities.
 * The spill priority numerical space is divided into potentially overlapping
 * ranges based on the type of buffer.
 */
object SpillPriorities {
  /**
   * Priorities for task output buffers intended for shuffle.
   */
  val OUTPUT_FOR_SHUFFLE_INITIAL_TASK_PRIORITY: Long = Long.MinValue

  /**
   * Priorities for buffers received from shuffle.
   * Shuffle input buffers are about to be read by a task, so spill
   * them if there's no other choice, but leave some space at the end of the priority range
   * so there can be some things after it.
   */
  val INPUT_FROM_SHUFFLE_PRIORITY: Long = Long.MaxValue - 1000

  /**
   * Priority for buffers that are waiting for next to be called.  i.e. data held between
   * calls to `hasNext` and `next` or between different calls to `next`.
   */
  val ACTIVE_ON_DECK_PRIORITY: Long = INPUT_FROM_SHUFFLE_PRIORITY + 1

  /**
   * Priority for multiple buffers being buffered within a call to next.
   */
  val ACTIVE_BATCHING_PRIORITY: Long = ACTIVE_ON_DECK_PRIORITY + 100

  /**
   * Priority offset for host memory buffers for spilling.
   */
  val HOST_MEMORY_BUFFER_SPILL_OFFSET: Long = 0

  /**
   * Calculate a new priority based on an offset, clamping it to avoid wraparound.
   *
   * @param originalPriority the original priority
   * @param offset           the desired offset
   * @return the resulting priority, with clamping if needed
   */
  def applyPriorityOffset(originalPriority: Long, offset: Long): Long = {
    if (offset < 0 && originalPriority < Long.MinValue - offset) {
      Long.MinValue
    } else if (offset > 0 && originalPriority > Long.MaxValue - offset) {
      Long.MaxValue
    } else {
      originalPriority + offset
    }
  }
}
