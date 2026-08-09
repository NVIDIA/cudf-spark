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

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * A GPU expression whose result depends on the input RDD partition index, mirroring
 * Spark's `org.apache.spark.sql.catalyst.expressions.Nondeterministic`. Examples:
 * `spark_partition_id()`, `monotonically_increasing_id()`, `rand(seed)`.
 *
 * To preserve SPARK-14393's invariant that values stay stable across
 * `coalesce`/`union`, the hosting operator MUST call
 * `GpuNondeterministic.initializeAll(exprs, parentIndex)` once per parent partition
 * (when the parent RDD's iterator first fires), passing the parent RDD's partition
 * index — NOT `TaskContext.getPartitionId()`, which is the output task's index and
 * stays constant across coalesced inputs (see #14156).
 *
 * For operators that have not yet been updated to plumb the parent partition index
 * (Aggregate, Window, Generate, etc.), `ensureInitialized` falls back to
 * `TaskContext.getPartitionId()` on the first `columnarEval`. This matches the
 * pre-fix behavior for those operators — incorrect under coalesce/union but
 * non-regressing. Follow-up PRs should update each such operator to call
 * `initialize()` explicitly; once all operators are updated, this fallback can
 * be hardened into an assertion.
 */
trait GpuNondeterministic extends GpuExpression {

  /** Override to capture state derived from the partition index. */
  protected def initializeInternal(partitionIndex: Int): Unit

  @transient private var initialized: Boolean = false

  /**
   * Called by the hosting operator once per parent partition before any
   * `columnarEval` on a batch from that partition. Idempotent across operator
   * boundaries: re-initialization replaces prior state.
   */
  final def initialize(partitionIndex: Int): Unit = {
    initializeInternal(partitionIndex)
    initialized = true
  }

  /**
   * Subclasses MUST call this from `columnarEval`. If `initialize()` has not
   * yet been called (legacy hosting operator), falls back to
   * `TaskContext.getPartitionId()` to preserve pre-fix behavior.
   */
  protected def ensureInitialized(): Unit = {
    if (!initialized) {
      initializeInternal(TaskContext.getPartitionId())
      initialized = true
    }
  }
}

object GpuNondeterministic {
  /**
   * Recursively initialize every `GpuNondeterministic` node found inside `exprs`
   * with the given partition index. No-op when `exprs` contain no
   * non-deterministic GPU expressions.
   */
  def initializeAll(exprs: Iterable[Expression], partitionIndex: Int): Unit = {
    exprs.foreach { e =>
      e.foreach {
        case nd: GpuNondeterministic => nd.initialize(partitionIndex)
        case _ =>
      }
    }
  }
}
