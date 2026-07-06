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

package com.nvidia.spark.rapids.iceberg.parquet.staged;

/**
 * Decides whether row groups from two footer results can share a synthetic Parquet file.
 *
 * <p>The planner invokes this method only on the Spark task thread. The Iceberg implementation
 * uses it for schema, rebase-mode, and synthesized-constant compatibility. A staged reader must
 * reject cross-file combination for format metadata whose semantics depend on file identity or
 * row order, such as Iceberg {@code _file} and {@code _pos}.</p>
 *
 * @param <C> format-specific immutable or task-confined footer context
 */
@FunctionalInterface
public interface FooterCompatibility<C> {
  /**
   * Returns whether {@code candidate} may be appended to a subtask already containing
   * {@code existing}.
   */
  boolean canCombine(FooterResult<C> existing, FooterResult<C> candidate);
}
