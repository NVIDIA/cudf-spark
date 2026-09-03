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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import com.nvidia.spark.history.BackendInfo;
import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsApi;
import com.nvidia.spark.history.HistoryMetricsBackend;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Retention;
import com.nvidia.spark.history.SchemaStatus;
import com.nvidia.spark.history.StampedObservation;
import com.nvidia.spark.history.Status;
import com.nvidia.spark.history.SummaryRequest;
import com.nvidia.spark.history.SummaryResponse;
import com.nvidia.spark.history.TestHistoryMetricCatalog;
import com.nvidia.spark.history.WriteResult;

import org.junit.jupiter.api.Test;

/** Contract tests for bounded status diagnostics and hostile provider results. */
class LocalStatusBoundsTest {
  private static final MetricVersionId FIRST = new MetricVersionId(61, 1);
  private static final MetricVersionId SECOND = new MetricVersionId(62, 1);
  private static final Duration TIMEOUT = Duration.ofSeconds(1);
  private static final String MALFORMED_RESULTS =
      "history metrics backend returned malformed results";
  private static final String PROVIDER_UNAVAILABLE =
      "history metrics backend is unavailable";

  @Test
  void statusReasonRequiresOneThrough256StrictUtf8Bytes() {
    assertNull(Status.ok().reason());
    for (Status.Code code : Status.Code.values()) {
      if (code != Status.Code.OK) {
        assertEquals(ascii(255), Status.of(code, ascii(255)).reason());
        assertEquals(ascii(256), Status.of(code, ascii(256)).reason());
        assertEquals(multibyte(128), Status.of(code, multibyte(128)).reason());
        assertThrows(IllegalArgumentException.class, () -> Status.of(code, ascii(257)));
        assertThrows(IllegalArgumentException.class, () -> Status.of(code, multibyte(129)));
        assertThrows(IllegalArgumentException.class, () -> Status.of(code, "\uD800"));
        assertThrows(IllegalArgumentException.class, () -> Status.of(code, ""));
        assertThrows(NullPointerException.class, () -> Status.of(code, null));
      }
    }
  }

  @Test
  void schemaReasonsAndAcceptedWarningsUseTheSameBound() {
    assertNull(SchemaStatus.accepted(FIRST, null).reason());
    assertEquals(ascii(255), SchemaStatus.accepted(FIRST, ascii(255)).reason());
    assertEquals(ascii(256), SchemaStatus.accepted(FIRST, ascii(256)).reason());
    assertEquals(multibyte(128), SchemaStatus.accepted(FIRST, multibyte(128)).reason());
    assertThrows(
        IllegalArgumentException.class, () -> SchemaStatus.accepted(FIRST, ascii(257)));
    assertThrows(
        IllegalArgumentException.class, () -> SchemaStatus.accepted(FIRST, multibyte(129)));
    assertThrows(
        IllegalArgumentException.class, () -> SchemaStatus.accepted(FIRST, "\uD800"));
    assertThrows(IllegalArgumentException.class, () -> SchemaStatus.accepted(FIRST, ""));

    for (SchemaStatus.Code code : SchemaStatus.Code.values()) {
      if (code != SchemaStatus.Code.ACCEPTED) {
        assertEquals(ascii(255), SchemaStatus.of(FIRST, code, ascii(255)).reason());
        assertEquals(ascii(256), SchemaStatus.of(FIRST, code, ascii(256)).reason());
        assertEquals(multibyte(128), SchemaStatus.of(FIRST, code, multibyte(128)).reason());
        assertThrows(
            IllegalArgumentException.class, () -> SchemaStatus.of(FIRST, code, ascii(257)));
        assertThrows(
            IllegalArgumentException.class, () -> SchemaStatus.of(FIRST, code, multibyte(129)));
        assertThrows(
            IllegalArgumentException.class, () -> SchemaStatus.of(FIRST, code, "\uD800"));
        assertThrows(IllegalArgumentException.class, () -> SchemaStatus.of(FIRST, code, ""));
        assertThrows(NullPointerException.class, () -> SchemaStatus.of(FIRST, code, null));
      }
    }
  }

  @Test
  void malformedProviderDiagnosticsRejectTheWholeSubmittedSubbatch() throws Exception {
    MutableBackend backend = new MutableBackend();
    DirectExecutor executor = new DirectExecutor();
    LocalMetricStorePlanningAdapter adapter = adapter(backend, executor);
    try {
      SchemaStatus malformedWarning = SchemaStatus.accepted(FIRST, "provider data");
      replaceReason(malformedWarning, ascii(257));
      backend.declarations = Arrays.asList(
          malformedWarning, SchemaStatus.accepted(SECOND, null));

      List<SchemaStatus> declarationResults =
          adapter.declare(Arrays.asList(schema(FIRST), schema(SECOND)), TIMEOUT);
      assertEquals(2, declarationResults.size());
      for (SchemaStatus status : declarationResults) {
        assertEquals(SchemaStatus.Code.UNAVAILABLE, status.code());
        assertEquals(MALFORMED_RESULTS, status.reason());
      }

      backend.declarations = Arrays.asList(
          SchemaStatus.accepted(FIRST, null), SchemaStatus.accepted(SECOND, null));
      assertEquals(
          SchemaStatus.Code.ACCEPTED,
          adapter.declare(Arrays.asList(schema(FIRST), schema(SECOND)), TIMEOUT).get(0).code());

      Status malformedReason = Status.of(Status.Code.DENIED, "provider data");
      replaceReason(malformedReason, "\uD800");
      backend.summaries = Arrays.asList(
          SummaryResponse.error(malformedReason),
          SummaryResponse.error(Status.of(Status.Code.DENIED, "valid but discarded")));

      List<SummaryResponse> summaryResults =
          adapter.summarize(Arrays.asList(request(FIRST), request(SECOND)), TIMEOUT);
      assertEquals(2, summaryResults.size());
      for (SummaryResponse response : summaryResults) {
        assertEquals(Status.Code.UNAVAILABLE, response.status().code());
        assertEquals(PROVIDER_UNAVAILABLE, response.status().reason());
      }
    } finally {
      adapter.stopPlanning();
      executor.shutdownNow();
    }
  }

  private static void replaceReason(Object status, String replacement) throws Exception {
    Field reason = status.getClass().getDeclaredField("reason");
    reason.setAccessible(true);
    reason.set(status, replacement);
  }

  private static String ascii(int bytes) {
    char[] value = new char[bytes];
    Arrays.fill(value, 'a');
    return new String(value);
  }

  private static String multibyte(int characters) {
    char[] value = new char[characters];
    Arrays.fill(value, '\u00E9');
    return new String(value);
  }

  private static MetricSchema schema(MetricVersionId metric) {
    return new MetricSchema(
        metric,
        Collections.singletonList(new DimensionSpec("table", DimValue.Kind.STRING)),
        new Retention(Duration.ofDays(1), Duration.ofDays(30)));
  }

  private static SummaryRequest request(MetricVersionId metric) {
    return SummaryRequest.builder(metric)
        .bound(Collections.singletonMap("table", DimValue.of("a")))
        .window(0L, 1L)
        .limit(1)
        .build();
  }

  private static LocalMetricStorePlanningAdapter adapter(
      MutableBackend backend, DirectExecutor executor) {
    HistoryMetricCatalog catalog = TestHistoryMetricCatalog.create(
        TestHistoryMetricCatalog.live(61, "test.first"),
        TestHistoryMetricCatalog.live(62, "test.second"));
    LocalCircuitBreakerPolicy breakerPolicy = LocalCircuitBreakerPolicy.of(
        128, 128, 1.0, Duration.ofDays(1), 1.0, Duration.ofDays(1));
    return LocalMetricStorePlanningAdapter.create(
        backend, catalog, executor, false, System::nanoTime, breakerPolicy);
  }

  private static final class MutableBackend implements HistoryMetricsBackend {
    private List<SchemaStatus> declarations;
    private List<SummaryResponse> summaries;

    @Override
    public BackendInfo info() {
      return new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "test backend");
    }

    @Override
    public List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout) {
      if (declarations != null) {
        return declarations;
      }
      List<SchemaStatus> accepted = new ArrayList<SchemaStatus>();
      for (MetricSchema schema : schemas) {
        accepted.add(SchemaStatus.accepted(schema.metric(), null));
      }
      return accepted;
    }

    @Override
    public WriteResult record(List<StampedObservation> observations) {
      return WriteResult.ok(observations.size());
    }

    @Override
    public List<SummaryResponse> summarize(
        List<SummaryRequest> requests, Duration timeout) {
      return summaries;
    }

    @Override
    public void close() {
    }
  }

  private static final class DirectExecutor extends AbstractExecutorService {
    private boolean shutdown;

    @Override
    public void execute(Runnable command) {
      if (shutdown) {
        throw new RejectedExecutionException("shutdown");
      }
      command.run();
    }

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return shutdown;
    }
  }
}
