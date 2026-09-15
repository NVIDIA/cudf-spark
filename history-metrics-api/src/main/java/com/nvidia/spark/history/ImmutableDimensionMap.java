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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Defensive dimension-map copying shared by raw observations and summary requests. */
final class ImmutableDimensionMap {
  private ImmutableDimensionMap() {
  }

  static Map<String, DimValue> copy(Map<String, DimValue> dimensions, String description) {
    Objects.requireNonNull(dimensions, description);
    if (dimensions.size() > MetricSchema.MAX_DIMENSIONS) {
      throw new IllegalArgumentException(
          description + " may contain at most " + MetricSchema.MAX_DIMENSIONS + " entries");
    }
    Map<String, DimValue> copied = new LinkedHashMap<String, DimValue>(dimensions.size());
    for (Map.Entry<String, DimValue> entry : dimensions.entrySet()) {
      String name = DimensionSpec.validateName(entry.getKey());
      DimValue value = Objects.requireNonNull(entry.getValue(), "dimension value");
      copied.put(name, value);
    }
    return Collections.unmodifiableMap(copied);
  }
}
