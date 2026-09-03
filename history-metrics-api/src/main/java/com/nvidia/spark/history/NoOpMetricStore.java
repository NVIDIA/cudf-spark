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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Built-in dependency-free store used whenever no provider is installed. */
final class NoOpMetricStore implements MetricStore {
  static final NoOpMetricStore INSTANCE = new NoOpMetricStore();

  private static final int MAX_BATCH_SIZE = 128;
  private static final BackendInfo INFO =
      new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "no-op history metrics store");

  private NoOpMetricStore() {
  }

  @Override
  public List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout) {
    if (schemas == null || schemas.size() > MAX_BATCH_SIZE) {
      return Collections.emptyList();
    }
    if (schemas.isEmpty()) {
      return Collections.emptyList();
    }

    Set<MetricVersionId> conflicts = findConflicts(schemas);
    boolean invalidTimeout = timeout == null || timeout.isNegative();
    List<SchemaStatus> results = new ArrayList<SchemaStatus>(schemas.size());
    for (MetricSchema schema : schemas) {
      if (schema == null) {
        results.add(SchemaStatus.of(
            null, SchemaStatus.Code.INVALID_REQUEST, "schema must not be null"));
      } else if (conflicts.contains(schema.metric())) {
        results.add(SchemaStatus.of(
            schema.metric(),
            SchemaStatus.Code.INCOMPATIBLE,
            "conflicting canonical schemas in one declaration batch"));
      } else if (invalidTimeout) {
        results.add(SchemaStatus.of(
            schema.metric(),
            SchemaStatus.Code.INVALID_REQUEST,
            "timeout must be non-null and nonnegative"));
      } else {
        results.add(SchemaStatus.accepted(schema.metric(), null));
      }
    }
    return Collections.unmodifiableList(results);
  }

  @Override
  public void record(List<Observation> observations) {
    // Optional evidence is deliberately discarded without provider, file, or network side effects.
  }

  @Override
  public List<SummaryResponse> summarize(
      List<SummaryRequest> requests, Duration timeout) {
    if (requests == null || requests.size() > MAX_BATCH_SIZE) {
      return Collections.emptyList();
    }
    if (requests.isEmpty()) {
      return Collections.emptyList();
    }
    for (SummaryRequest request : requests) {
      if (request == null) {
        return repeatedSummaryError(
            requests.size(),
            Status.Code.INVALID_REQUEST,
            "summary request batch contains a null element");
      }
    }
    if (timeout == null || timeout.isNegative()) {
      return repeatedSummaryError(
          requests.size(),
          Status.Code.INVALID_REQUEST,
          "timeout must be non-null and nonnegative");
    }
    if (timeout.isZero()) {
      return repeatedSummaryError(
          requests.size(),
          Status.Code.DEADLINE_EXCEEDED,
          "summary timeout expired before provider work");
    }
    return repeatedSummaryError(
        requests.size(), Status.Code.UNAVAILABLE, "no history metrics provider is installed");
  }

  @Override
  public BackendInfo info() {
    return INFO;
  }

  private static Set<MetricVersionId> findConflicts(List<MetricSchema> schemas) {
    Map<MetricVersionId, MetricSchema> firstByMetric = new HashMap<MetricVersionId, MetricSchema>();
    Set<MetricVersionId> conflicts = new HashSet<MetricVersionId>();
    for (MetricSchema schema : schemas) {
      if (schema != null) {
        MetricSchema first = firstByMetric.get(schema.metric());
        if (first == null) {
          firstByMetric.put(schema.metric(), schema);
        } else if (!first.equals(schema)) {
          conflicts.add(schema.metric());
        }
      }
    }
    return conflicts;
  }

  private static List<SummaryResponse> repeatedSummaryError(
      int count, Status.Code code, String reason) {
    SummaryResponse response = SummaryResponse.error(Status.of(code, reason));
    List<SummaryResponse> results = new ArrayList<SummaryResponse>(count);
    for (int index = 0; index < count; index++) {
      results.add(response);
    }
    return Collections.unmodifiableList(results);
  }
}
