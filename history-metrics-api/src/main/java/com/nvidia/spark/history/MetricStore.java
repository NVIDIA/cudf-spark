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
package com.nvidia.spark.history;

import java.time.Duration;
import java.util.List;

/**
 * Deadline-bounded planning-facing history metrics store.
 *
 * <p>Declaration and summary calls are synchronous from the caller's perspective. Their timeouts
 * are relative, nonnegative end-to-end budgets. Implementations start the budget at method entry
 * using monotonic elapsed time, pass only remaining budget to each
 * layer, and return positional fallback results instead of allowing failures to escape into a query.
 * Empty batches do not invoke a provider. Batches larger than 128 use the documented empty-list
 * invalid-batch sentinel.
 *
 * <p>{@link #record(List)} is a total, non-blocking boundary: malformed input and write failures are
 * counted and dropped by an implementation. The planning contract deliberately has no flush, close,
 * update, or destructive operation.
 */
public interface MetricStore {
  /**
   * Synchronously declares schemas within the relative deadline and returns one outcome for every
   * input position.
   *
   * <p>{@code timeout} is a relative, nonnegative budget measured from method entry. Invalid,
   * zero-budget, stopped, unavailable, and malformed-provider outcomes are returned as positional
   * statuses; callers branch on {@link SchemaStatus.Code}, not diagnostic text. A null or oversized
   * batch uses the empty-list invalid-batch sentinel.
   *
   * @param schemas ordered schemas to declare; valid batches contain at most 128 entries
   * @param timeout relative end-to-end budget
   * @return positional immutable declaration outcomes, or the documented empty-list sentinel
   */
  List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout);

  /**
   * Offers raw observations for asynchronous persistence without blocking or throwing into query
   * work.
   *
   * <p>A configured implementation stamps provenance, counts and drops invalid or unavailable
   * evidence, and may drop a suffix when its bounded queue is full. Return does not mean the
   * observations were persisted. The built-in no-op store discards the entire call without
   * inspecting it.
   *
   * @param observations raw observations; configured stores defensively handle null input and
   *     elements
   */
  void record(List<Observation> observations);

  /**
   * Synchronously returns one summary outcome for every request position within the relative
   * deadline.
   *
   * <p>{@code timeout} is a relative, nonnegative budget measured from method entry. Consumers must
   * branch on {@link Status.Code}; {@code OK} with a null summary is normal absence of evidence.
   * Unexpected response cardinality requires whole-batch abstention. A null or oversized batch uses
   * the empty-list invalid-batch sentinel.
   *
   * @param requests ordered requests; valid batches contain at most 128 entries
   * @param timeout relative end-to-end budget
   * @return positional immutable summary outcomes, or the documented empty-list sentinel
   */
  List<SummaryResponse> summarize(List<SummaryRequest> requests, Duration timeout);

  /**
   * Returns immutable compatibility information cached during provider construction.
   *
   * <p>This accessor must not perform provider I/O or throw.
   *
   * @return immutable cached compatibility information
   */
  BackendInfo info();
}
