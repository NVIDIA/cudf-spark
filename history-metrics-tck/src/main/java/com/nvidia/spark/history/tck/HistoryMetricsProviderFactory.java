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

/**
 * Creates one isolated provider fixture for each inherited conformance test.
 *
 * <p>The returned isolation domain starts without declarations or observations, uses the supplied
 * complete catalog and controllable provider time, and treats {@code maximumPlanningAge} as its
 * planning-region envelope. The adapter may provision a process-local store or an isolated remote
 * test deployment; the conformance suite does not depend on that choice.
 */
public interface HistoryMetricsProviderFactory {
  /**
   * Opens a fresh isolation domain for one inherited conformance test.
   *
   * @param catalog complete source-controlled catalog for the fixture
   * @param initialProviderTimeMs initial controllable provider time in epoch milliseconds
   * @param maximumPlanningAge provider planning-region envelope
   * @return a non-null fixture with no declarations or observations
   * @throws Exception if the provider cannot create the isolated fixture
   */
  HistoryMetricsProviderFixture open(
      HistoryMetricCatalog catalog,
      long initialProviderTimeMs,
      Duration maximumPlanningAge) throws Exception;
}
