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

/** Explicit immutable planning executor bounds for a local provider. */
public final class LocalExecutionPolicy {
  private final int planningThreads;
  private final int planningQueueCapacity;

  private LocalExecutionPolicy(int planningThreads, int planningQueueCapacity) {
    if (planningThreads <= 0) {
      throw new IllegalArgumentException("planningThreads must be positive");
    }
    if (planningQueueCapacity <= 0) {
      throw new IllegalArgumentException("planningQueueCapacity must be positive");
    }
    this.planningThreads = planningThreads;
    this.planningQueueCapacity = planningQueueCapacity;
  }

  /**
   * Creates explicit planning executor bounds.
   *
   * @param planningThreads positive number of planning worker threads
   * @param planningQueueCapacity positive number of queued planning tasks
   * @return immutable execution policy
   */
  public static LocalExecutionPolicy of(int planningThreads, int planningQueueCapacity) {
    return new LocalExecutionPolicy(planningThreads, planningQueueCapacity);
  }

  /**
   * Returns the positive planning worker-thread count.
   *
   * @return planning thread count
   */
  public int planningThreads() {
    return planningThreads;
  }

  /**
   * Returns the positive planning queue capacity in tasks.
   *
   * @return queued task capacity
   */
  public int planningQueueCapacity() {
    return planningQueueCapacity;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LocalExecutionPolicy)) {
      return false;
    }
    LocalExecutionPolicy that = (LocalExecutionPolicy) other;
    return planningThreads == that.planningThreads &&
        planningQueueCapacity == that.planningQueueCapacity;
  }

  @Override
  public int hashCode() {
    return 31 * planningThreads + planningQueueCapacity;
  }

  @Override
  public String toString() {
    return "LocalExecutionPolicy{" +
        "planningThreads=" + planningThreads +
        ", planningQueueCapacity=" + planningQueueCapacity + '}';
  }
}
