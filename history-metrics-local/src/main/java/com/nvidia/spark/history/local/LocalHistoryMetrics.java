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
package com.nvidia.spark.history.local;

import java.nio.file.Path;
import java.time.Duration;

import com.nvidia.spark.history.MetricStore;

/**
 * Explicit owner of one standalone driver-local history metrics store.
 *
 * <p>The owner is not installed in the process locator and is deliberately not
 * {@link AutoCloseable}; callers must choose an explicit shutdown budget.
 */
public interface LocalHistoryMetrics {
  /**
   * Returns the planning-facing store owned by this object.
   *
   * @return owned store facade
   */
  MetricStore store();

  /**
   * Returns companion-only immutable diagnostic snapshots for this owner.
   *
   * @return diagnostic test handle
   */
  LocalHistoryMetricsTestHandle testHandle();

  /**
   * Saves one coherent current-version image within a single monotonic timeout budget.
   *
   * <p>The target and sibling temporary files contain unencrypted sensitive data. The caller owns
   * path protection and residual-file cleanup. A successful return means atomic replacement
   * committed; it does not promise power-loss durability.
   *
   * @param target explicit target path whose parent must already exist
   * @param timeout relative nonnegative end-to-end budget
   * @throws NullPointerException if {@code target} or {@code timeout} is null
   * @throws IllegalArgumentException if {@code timeout} is negative
   * @throws LocalSnapshotException when bounded snapshot publication cannot complete
   * @throws IllegalStateException if shutdown won the lifecycle reservation
   */
  void save(Path target, Duration timeout) throws LocalSnapshotException;

  /**
   * Waits for every observation admitted through the call's captured watermark to become terminal.
   *
   * <p>A zero duration polls. Later admissions are outside this watermark.
   *
   * @param timeout relative nonnegative waiting budget
   * @return true only when the captured watermark completed within this caller's budget
   * @throws NullPointerException if {@code timeout} is null
   * @throws IllegalArgumentException if {@code timeout} is negative
   */
  boolean drain(Duration timeout);

  /**
   * Initiates or joins idempotent owned-resource shutdown.
   *
   * <p>The first call stops new admission and fixes the shared drain/drop deadline. Later calls
   * cannot extend it and only wait within their own budgets. A false return means owned daemon
   * cleanup continues; a later call returns true after cleanup completes. A zero-duration first call
   * still initiates stopping.
   *
   * @param timeout relative nonnegative budget for this caller
   * @return true only when shared cleanup completed within this caller's budget
   * @throws NullPointerException if {@code timeout} is null
   * @throws IllegalArgumentException if {@code timeout} is negative
   */
  boolean shutdown(Duration timeout);
}
