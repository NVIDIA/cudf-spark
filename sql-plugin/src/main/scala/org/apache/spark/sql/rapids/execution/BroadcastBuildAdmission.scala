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
package org.apache.spark.sql.rapids.execution

/** Admission uses completed, uncompressed map output sizes, never logical-plan estimates. */
private[execution] object BroadcastBuildAdmission {
  def checkActualBytes(bytes: BigInt, maxBytes: Long, phase: String): Unit = {
    require(maxBytes > 0, "Broadcast build byte limit must be positive")
    if (bytes < 0 || bytes > maxBytes) {
      throw new IllegalStateException(s"Executor broadcast $phase size $bytes exceeds " +
        s"spark.rapids.shuffle.broadcast.maxSize=$maxBytes. " +
        "Disable spark.rapids.shuffle.broadcast.enabled to use standard broadcast; " +
        "executor assembly cannot fall back to a different query plan.")
    }
  }

  def accepts(partitionBytes: Array[Long], rows: Long, maxBytes: Long): Boolean = {
    val bytes = partitionBytes.foldLeft(BigInt(0))(_ + _)
    partitionBytes.forall(_ >= 0) && maxBytes > 0 && bytes <= maxBytes &&
      rows >= 0 && rows <= Int.MaxValue && (bytes == 0 || rows > 0)
  }
}
