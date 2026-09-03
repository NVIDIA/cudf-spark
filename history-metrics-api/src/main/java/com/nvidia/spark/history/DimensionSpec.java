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

/** The stable name and equality kind of one declared dimension. */
public final class DimensionSpec {
  private static final int MAX_NAME_BYTES = 128;

  private final String name;
  private final DimValue.Kind kind;

  /**
   * Creates one ordered equality dimension declaration.
   *
   * @param name nonempty case-sensitive name of at most 128 strict UTF-8 bytes
   * @param kind exact value kind
   * @throws NullPointerException if either argument is null
   * @throws IllegalArgumentException if the name is empty, malformed, or over its byte bound
   */
  public DimensionSpec(String name, DimValue.Kind kind) {
    this.name = validateName(name);
    this.kind = Objects.requireNonNull(kind, "kind");
  }

  static String validateName(String name) {
    Objects.requireNonNull(name, "name");
    if (name.isEmpty()) {
      throw new IllegalArgumentException("dimension name must not be empty");
    }
    if (StrictUtf8.encode(name, "dimension name").length > MAX_NAME_BYTES) {
      throw new IllegalArgumentException(
          "dimension name exceeds " + MAX_NAME_BYTES + " UTF-8 bytes");
    }
    return name;
  }

  /** @return the stable case-sensitive dimension name */
  public String name() {
    return name;
  }

  /** @return the exact equality-value kind */
  public DimValue.Kind kind() {
    return kind;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DimensionSpec)) {
      return false;
    }
    DimensionSpec that = (DimensionSpec) other;
    return name.equals(that.name) && kind == that.kind;
  }

  @Override
  public int hashCode() {
    return 31 * name.hashCode() + kind.hashCode();
  }

  @Override
  public String toString() {
    return "DimensionSpec{" + "name='" + name + '\'' + ", kind=" + kind + '}';
  }
}
