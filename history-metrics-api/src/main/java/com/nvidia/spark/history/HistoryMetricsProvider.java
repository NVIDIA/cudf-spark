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

import org.apache.spark.SparkContext;

/**
 * Driver-side history metrics provider discovered and owned by the RAPIDS plugin.
 *
 * <p>Implementations are discovered through {@link java.util.ServiceLoader}. A provider reads its
 * own configuration from the supplied Spark context. Merely placing a provider on the classpath
 * does not enable it; the RAPIDS provider configuration must select its {@link #name() name}.
 */
public interface HistoryMetricsProvider {
  /**
   * Returns the stable, case-insensitive configuration name for this provider.
   *
   * @return nonempty provider name; {@code none} is reserved by RAPIDS
   */
  String name();

  /**
   * Opens this provider and returns its planning-facing store.
   *
   * <p>The provider owns all resources behind the returned store. If opening fails, the provider
   * must release any partially constructed resources before throwing.
   *
   * @param sparkContext driver Spark context, including Spark and Hadoop configuration
   * @return non-null store owned by this provider
   * @throws Exception when the provider cannot be opened
   */
  MetricStore open(SparkContext sparkContext) throws Exception;

  /**
   * Releases resources owned by this provider within the supplied budget.
   *
   * <p>Repeated calls must be harmless. A {@code false} result means shutdown did not finish
   * within the budget; callers should report it but must not block indefinitely.
   *
   * @param timeout nonnegative shutdown budget
   * @return whether shutdown completed within the budget
   */
  boolean shutdown(Duration timeout);
}
