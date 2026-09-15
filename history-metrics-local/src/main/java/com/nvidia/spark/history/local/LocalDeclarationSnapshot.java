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

import java.util.Objects;

import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Retention;

/** Immutable defensive point-in-time view of one canonical local declaration. */
public final class LocalDeclarationSnapshot {
  private final MetricSchema schema;
  private final Retention effectiveRetention;

  LocalDeclarationSnapshot(MetricSchema schema, Retention effectiveRetention) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.effectiveRetention =
        Objects.requireNonNull(effectiveRetention, "effectiveRetention");
  }

  public MetricSchema schema() {
    return schema;
  }

  public Retention effectiveRetention() {
    return effectiveRetention;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LocalDeclarationSnapshot)) {
      return false;
    }
    LocalDeclarationSnapshot that = (LocalDeclarationSnapshot) other;
    return schema.equals(that.schema) &&
        effectiveRetention.equals(that.effectiveRetention);
  }

  @Override
  public int hashCode() {
    return 31 * schema.hashCode() + effectiveRetention.hashCode();
  }

  @Override
  public String toString() {
    return "LocalDeclarationSnapshot{" +
        "metric=" + schema.metric() +
        ", effectiveRetention=" + effectiveRetention + '}';
  }
}
