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

/** Immutable API compatibility and redacted diagnostic information for one store. */
public final class BackendInfo {
  private static final int MAX_DESCRIPTION_BYTES = 256;

  private final int apiVersion;
  private final String description;

  /**
   * Creates provider information for compatibility checks and diagnostics.
   *
   * @param apiVersion positive implemented API version
   * @param description nonempty, already-redacted description of at most 256 strict UTF-8 bytes
   * @throws NullPointerException if {@code description} is null
   * @throws IllegalArgumentException if the version or description is invalid
   */
  public BackendInfo(int apiVersion, String description) {
    if (apiVersion <= 0) {
      throw new IllegalArgumentException("apiVersion must be positive");
    }
    Objects.requireNonNull(description, "description");
    if (description.isEmpty()) {
      throw new IllegalArgumentException("description must not be empty");
    }
    if (StrictUtf8.encode(description, "description").length > MAX_DESCRIPTION_BYTES) {
      throw new IllegalArgumentException(
          "description exceeds " + MAX_DESCRIPTION_BYTES + " UTF-8 bytes");
    }
    this.apiVersion = apiVersion;
    this.description = description;
  }

  /** @return the positive API version implemented by the provider */
  public int apiVersion() {
    return apiVersion;
  }

  /** @return the nonempty redacted diagnostic description */
  public String description() {
    return description;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof BackendInfo)) {
      return false;
    }
    BackendInfo that = (BackendInfo) other;
    return apiVersion == that.apiVersion && description.equals(that.description);
  }

  @Override
  public int hashCode() {
    return 31 * apiVersion + description.hashCode();
  }

  @Override
  public String toString() {
    return "BackendInfo{" + "apiVersion=" + apiVersion +
        ", description='" + description + '\'' + '}';
  }
}
