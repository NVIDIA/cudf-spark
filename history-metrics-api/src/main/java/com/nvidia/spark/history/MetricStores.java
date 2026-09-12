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

import java.util.Objects;

/** Thread-safe process service locator with a built-in non-null no-op default. */
public final class MetricStores {
  private static final Object LOCK = new Object();

  private static volatile MetricStore current = NoOpMetricStore.INSTANCE;
  private static Registration active;

  private MetricStores() {
  }

  /**
   * Returns the currently installed store, or the built-in no-op store when none is installed.
   *
   * @return the non-null current store
   */
  public static MetricStore current() {
    return current;
  }

  /**
   * Atomically installs one explicitly constructed compatible store.
   *
   * <p>Only one registration may be active. The store's cached compatibility information is
   * validated before publication. The returned handle owns only registration: closing it detaches
   * this exact store and restores the no-op store, but never closes provider resources. Repeated
   * closure is harmless.
   *
   * @param store explicitly constructed store whose resources remain caller-owned
   * @return scoped non-owning registration handle
   * @throws NullPointerException if {@code store} is null
   * @throws IllegalArgumentException if compatibility information is unavailable or unsupported
   * @throws IllegalStateException if another registration is active
   */
  public static AutoCloseable install(MetricStore store) {
    Objects.requireNonNull(store, "store");
    synchronized (LOCK) {
      if (active != null) {
        throw new IllegalStateException("a history metrics store is already installed");
      }
    }

    BackendInfo info;
    try {
      info = store.info();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("store info is unavailable", e);
    }
    if (info == null) {
      throw new IllegalArgumentException("store info must not be null");
    }
    if (info.apiVersion() != HistoryMetricsApi.CURRENT_API_VERSION) {
      throw new IllegalArgumentException(
          "unsupported history metrics API version: " + info.apiVersion());
    }

    synchronized (LOCK) {
      if (active != null) {
        throw new IllegalStateException("a history metrics store is already installed");
      }
      Registration registration = new Registration(store);
      current = store;
      active = registration;
      return registration;
    }
  }

  private static final class Registration implements AutoCloseable {
    private final MetricStore installed;

    private Registration(MetricStore installed) {
      this.installed = installed;
    }

    @Override
    public void close() {
      synchronized (LOCK) {
        if (active == this && current == installed) {
          current = NoOpMetricStore.INSTANCE;
          active = null;
        }
      }
    }
  }
}
