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

import java.util.List;

/**
 * Companion-only defensive point-in-time diagnostics for one explicit local owner.
 *
 * <p>Every returned object is an immutable snapshot. This interface is test/prototype support, not
 * provider-neutral raw read-back or an administrative API.
 */
public interface LocalHistoryMetricsTestHandle {
  /**
   * Returns accepted observations ordered by provider acceptance ordinal.
   *
   * @return immutable observation snapshots
   */
  List<LocalObservationSnapshot> observations();

  /**
   * Returns declarations ordered by packed metric ID and version.
   *
   * @return immutable declaration snapshots
   */
  List<LocalDeclarationSnapshot> declarations();

  /**
   * Returns an immutable counter snapshot with no reset operation.
   *
   * @return point-in-time counters
   */
  LocalHistoryMetricsCounters counters();

  /**
   * Returns the instantaneous planning breaker state.
   *
   * <p>Shutdown does not synthesize a new breaker state; the last normal state remains observable.
   *
   * @return instantaneous breaker state
   */
  LocalCircuitBreakerState breakerState();
}
