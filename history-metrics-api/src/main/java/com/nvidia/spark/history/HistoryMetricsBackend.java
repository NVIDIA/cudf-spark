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
 * Synchronous provider boundary owned behind the non-blocking framework store.
 *
 * <p>Future additive methods must be Java 8 default methods with explicit unavailable results.
 */
public interface HistoryMetricsBackend extends AutoCloseable {
  /**
   * Returns the provider's API compatibility and redacted diagnostic description.
   *
   * @return non-null provider information
   */
  BackendInfo info();

  /**
   * Declares metric schemas synchronously within the remaining relative budget.
   *
   * <p>The result must be non-null, preserve input order, and contain exactly one status per input.
   *
   * @param schemas non-null provider-eligible schemas
   * @param timeout positive remaining relative call budget
   * @return positional declaration statuses
   */
  List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout);

  /**
   * Records a framework-stamped batch synchronously.
   *
   * <p>The returned accepted and rejected counts must sum to the submitted batch size.
   *
   * @param observations non-null observations carrying framework-owned provenance
   * @return the non-null counted batch outcome
   */
  WriteResult record(List<StampedObservation> observations);

  /**
   * Summarizes requests synchronously within the remaining relative budget.
   *
   * <p>The result must be non-null, preserve input order, and contain exactly one response per
   * request.
   *
   * @param requests non-null provider-eligible requests
   * @param timeout positive remaining relative call budget
   * @return positional summary responses
   */
  List<SummaryResponse> summarize(List<SummaryRequest> requests, Duration timeout);

  /** Releases provider-owned resources after active backend invocations have finished. */
  @Override
  void close();
}
