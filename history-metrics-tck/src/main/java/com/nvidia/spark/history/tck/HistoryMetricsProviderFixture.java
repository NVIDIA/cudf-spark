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
package com.nvidia.spark.history.tck;

import java.time.Duration;

import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.MetricStore;

/**
 * Provider-neutral lifecycle and deterministic test controls used by the conformance suite.
 *
 * <p>A provider adapter owns every resource behind this fixture. {@link #awaitWrites(Duration)}
 * waits for observations accepted before the call without imposing a production flush API. This
 * core fixture is deliberately black-box and exposes no provider-neutral stored-observation or
 * provenance inspection hook.
 */
public interface HistoryMetricsProviderFixture extends AutoCloseable {
  /** @return the complete catalog supplied when this fixture was opened */
  HistoryMetricCatalog catalog();

  /** @return the provider's framework-owned planning store */
  MetricStore store();

  /**
   * Sets the provider clock used by subsequent retention and coverage decisions.
   *
   * @param timestampMs provider time in epoch milliseconds
   */
  void setProviderTime(long timestampMs);

  /**
   * Waits for observations accepted before this call to become visible to summaries.
   *
   * @param timeout relative wait budget
   * @return {@code true} when all such observations are visible within the budget
   */
  boolean awaitWrites(Duration timeout);

  /**
   * Releases all resources owned by this isolated fixture.
   *
   * @throws Exception if provider cleanup fails
   */
  @Override
  void close() throws Exception;
}
