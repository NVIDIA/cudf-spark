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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.LocalTestCatalog;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Provenance;
import com.nvidia.spark.history.Retention;

import org.junit.jupiter.api.Test;

/** Behavioral and compiled-shape coverage for the explicit local companion contracts. */
class LocalCompanionContractsTest {
  @Test
  void localOwnerFactoryAndTestHandlePublicSurfacesRemainExact() {
    assertPublicMethods(
        LocalHistoryMetrics.class,
        "store():com.nvidia.spark.history.MetricStore",
        "testHandle():com.nvidia.spark.history.local.LocalHistoryMetricsTestHandle",
        "save(java.nio.file.Path,java.time.Duration):void",
        "drain(java.time.Duration):boolean",
        "shutdown(java.time.Duration):boolean");
    assertEquals(0, LocalHistoryMetricsFactory.class.getConstructors().length);
    assertPublicMethods(
        LocalHistoryMetricsFactory.class,
        "open(com.nvidia.spark.history.HistoryMetricCatalog,java.time.Clock," +
            "com.nvidia.spark.history.local.LocalProvenanceSource,java.time.Duration," +
            "com.nvidia.spark.history.local.LocalQueuePolicy," +
            "com.nvidia.spark.history.local.LocalExecutionPolicy," +
            "com.nvidia.spark.history.local.LocalCircuitBreakerPolicy):" +
            "com.nvidia.spark.history.local.LocalHistoryMetrics",
        "openSnapshot(java.nio.file.Path," +
            "com.nvidia.spark.history.HistoryMetricCatalog,java.time.Clock," +
            "com.nvidia.spark.history.local.LocalProvenanceSource,java.time.Duration," +
            "com.nvidia.spark.history.local.LocalQueuePolicy," +
            "com.nvidia.spark.history.local.LocalExecutionPolicy," +
            "com.nvidia.spark.history.local.LocalCircuitBreakerPolicy,java.time.Duration):" +
            "com.nvidia.spark.history.local.LocalHistoryMetrics");
    assertPublicMethods(
        LocalHistoryMetricsTestHandle.class,
        "observations():java.util.List",
        "declarations():java.util.List",
        "counters():com.nvidia.spark.history.local.LocalHistoryMetricsCounters",
        "breakerState():com.nvidia.spark.history.local.LocalCircuitBreakerState");
    assertEquals(0, LocalObservationSnapshot.class.getConstructors().length);
    assertPublicMethods(
        LocalObservationSnapshot.class,
        "acceptanceOrdinal():long",
        "dimensions():java.util.Map",
        "equals(java.lang.Object):boolean",
        "hashCode():int",
        "metric():com.nvidia.spark.history.MetricVersionId",
        "provenance():com.nvidia.spark.history.Provenance",
        "timestampMs():long",
        "toString():java.lang.String",
        "value():double");
  }

  @Test
  void provenanceIdentityIsStrictBoundedImmutableAndRedacted() {
    LocalProvenanceIdentity identity =
        LocalProvenanceIdentity.of("secret-app-value", null, "secret-plugin-value");
    LocalProvenanceIdentity same =
        LocalProvenanceIdentity.of("secret-app-value", null, "secret-plugin-value");
    LocalProvenanceIdentity attempted =
        LocalProvenanceIdentity.of("secret-app-value", "attempt", "secret-plugin-value");

    assertEquals("secret-app-value", identity.applicationId());
    assertEquals(null, identity.attemptId());
    assertEquals("secret-plugin-value", identity.pluginVersion());
    assertEquals(identity, same);
    assertEquals(identity.hashCode(), same.hashCode());
    assertNotEquals(identity, attempted);
    assertFalse(identity.toString().contains("secret-app-value"));
    assertFalse(identity.toString().contains("secret-plugin-value"));
    assertTrue(attempted.toString().contains("attemptIdPresent=true"));

    LocalProvenanceIdentity.of(repeat('a', 255), repeat('b', 64), repeat('c', 64));
    assertThrows(IllegalArgumentException.class,
        () -> LocalProvenanceIdentity.of(repeat('a', 256), null, "v"));
    assertThrows(IllegalArgumentException.class,
        () -> LocalProvenanceIdentity.of("app", repeat('b', 65), "v"));
    assertThrows(IllegalArgumentException.class,
        () -> LocalProvenanceIdentity.of("app", null, repeat('c', 65)));
    assertThrows(IllegalArgumentException.class,
        () -> LocalProvenanceIdentity.of("\uD83Dprobe", null, "v"));
    assertThrows(IllegalArgumentException.class,
        () -> LocalProvenanceIdentity.of("app", null, "probe\uDE00"));
    assertThrows(NullPointerException.class,
        () -> LocalProvenanceIdentity.of(null, null, "v"));
    assertThrows(IllegalArgumentException.class,
        () -> LocalProvenanceIdentity.of("", null, "v"));
  }

  @Test
  void queueAndExecutionPoliciesRequireExplicitPositiveBounds() {
    LocalQueuePolicy queue = LocalQueuePolicy.of(17, 5);
    LocalQueuePolicy sameQueue = LocalQueuePolicy.of(17, 5);
    assertEquals(17, queue.capacityObservations());
    assertEquals(5, queue.maxBackendBatchSize());
    assertEquals(queue, sameQueue);
    assertEquals(queue.hashCode(), sameQueue.hashCode());
    assertTrue(queue.toString().contains("capacityObservations=17"));
    assertThrows(IllegalArgumentException.class, () -> LocalQueuePolicy.of(0, 1));
    assertThrows(IllegalArgumentException.class, () -> LocalQueuePolicy.of(1, 0));

    LocalExecutionPolicy execution = LocalExecutionPolicy.of(3, 29);
    LocalExecutionPolicy sameExecution = LocalExecutionPolicy.of(3, 29);
    assertEquals(3, execution.planningThreads());
    assertEquals(29, execution.planningQueueCapacity());
    assertEquals(execution, sameExecution);
    assertEquals(execution.hashCode(), sameExecution.hashCode());
    assertTrue(execution.toString().contains("planningThreads=3"));
    assertThrows(IllegalArgumentException.class, () -> LocalExecutionPolicy.of(0, 1));
    assertThrows(IllegalArgumentException.class, () -> LocalExecutionPolicy.of(1, 0));
  }

  @Test
  void circuitBreakerPolicyEnforcesEveryFrozenBoundary() {
    LocalCircuitBreakerPolicy policy = LocalCircuitBreakerPolicy.of(
        10,
        4,
        0.5,
        Duration.ofMillis(25),
        1.0,
        Duration.ofSeconds(3));
    LocalCircuitBreakerPolicy same = LocalCircuitBreakerPolicy.of(
        10,
        4,
        0.5,
        Duration.ofMillis(25),
        1.0,
        Duration.ofSeconds(3));

    assertEquals(10, policy.windowSize());
    assertEquals(4, policy.minSamples());
    assertEquals(0.5, policy.failureRateThreshold());
    assertEquals(Duration.ofMillis(25), policy.slowCallThreshold());
    assertEquals(1.0, policy.slowRateThreshold());
    assertEquals(Duration.ofSeconds(3), policy.openDuration());
    assertEquals(policy, same);
    assertEquals(policy.hashCode(), same.hashCode());
    assertTrue(policy.toString().contains("windowSize=10"));

    assertThrows(IllegalArgumentException.class, () -> breaker(0, 1, 0.5, 0.5));
    assertThrows(IllegalArgumentException.class, () -> breaker(2, 0, 0.5, 0.5));
    assertThrows(IllegalArgumentException.class, () -> breaker(2, 3, 0.5, 0.5));
    for (double invalid : new double[] {0.0, -0.1, 1.01, Double.NaN,
        Double.POSITIVE_INFINITY}) {
      assertThrows(IllegalArgumentException.class, () -> breaker(2, 1, invalid, 0.5));
      assertThrows(IllegalArgumentException.class, () -> breaker(2, 1, 0.5, invalid));
    }
    assertThrows(NullPointerException.class, () -> LocalCircuitBreakerPolicy.of(
        2, 1, 0.5, null, 0.5, Duration.ofSeconds(1)));
    assertThrows(NullPointerException.class, () -> LocalCircuitBreakerPolicy.of(
        2, 1, 0.5, Duration.ofSeconds(1), 0.5, null));
    assertThrows(IllegalArgumentException.class, () -> LocalCircuitBreakerPolicy.of(
        2, 1, 0.5, Duration.ZERO, 0.5, Duration.ofSeconds(1)));
    assertThrows(IllegalArgumentException.class, () -> LocalCircuitBreakerPolicy.of(
        2, 1, 0.5, Duration.ofSeconds(1), 0.5, Duration.ZERO));
  }

  @Test
  void diagnosticsExposeOnlyTheFrozenClosedEnumsAndImmutableCounts() {
    assertArrayEquals(new LocalCircuitBreakerState[] {
        LocalCircuitBreakerState.CLOSED,
        LocalCircuitBreakerState.OPEN,
        LocalCircuitBreakerState.HALF_OPEN
    }, LocalCircuitBreakerState.values());
    assertArrayEquals(new LocalMetricCounter[] {
        LocalMetricCounter.DECLARATION_BATCH,
        LocalMetricCounter.DECLARATION_STATUS_ACCEPTED,
        LocalMetricCounter.DECLARATION_STATUS_INCOMPATIBLE,
        LocalMetricCounter.DECLARATION_STATUS_INVALID_REQUEST,
        LocalMetricCounter.DECLARATION_STATUS_UNAVAILABLE,
        LocalMetricCounter.DECLARATION_STATUS_DENIED,
        LocalMetricCounter.SUMMARY_BATCH,
        LocalMetricCounter.SUMMARY_STATUS_OK,
        LocalMetricCounter.SUMMARY_STATUS_NOT_DECLARED,
        LocalMetricCounter.SUMMARY_STATUS_INVALID_REQUEST,
        LocalMetricCounter.SUMMARY_STATUS_DEADLINE_EXCEEDED,
        LocalMetricCounter.SUMMARY_STATUS_UNAVAILABLE,
        LocalMetricCounter.SUMMARY_STATUS_DENIED,
        LocalMetricCounter.SUMMARY_WINDOW_CLIPPED,
        LocalMetricCounter.SUMMARY_ROWS,
        LocalMetricCounter.RECORD_INVALID,
        LocalMetricCounter.RECORD_NOT_DECLARED,
        LocalMetricCounter.RECORD_FUTURE_TIMESTAMP,
        LocalMetricCounter.RECORD_CLOCK_FAILURE,
        LocalMetricCounter.RECORD_PROVENANCE_FAILURE,
        LocalMetricCounter.RECORD_OVERFLOW,
        LocalMetricCounter.RECORD_POST_STOP,
        LocalMetricCounter.RECORD_ENQUEUED,
        LocalMetricCounter.BACKEND_ACCEPTED,
        LocalMetricCounter.BACKEND_REJECTED,
        LocalMetricCounter.BACKEND_AMBIGUOUS,
        LocalMetricCounter.SNAPSHOT_CLEANUP_FAILURE,
        LocalMetricCounter.QUEUE_CURRENT,
        LocalMetricCounter.QUEUE_HIGH_WATER,
        LocalMetricCounter.DRAIN_SUCCESS,
        LocalMetricCounter.DRAIN_TIMEOUT,
        LocalMetricCounter.BREAKER_SAMPLE,
        LocalMetricCounter.BREAKER_FAILURE,
        LocalMetricCounter.BREAKER_SLOW,
        LocalMetricCounter.BREAKER_OPEN,
        LocalMetricCounter.BREAKER_SUPPRESSED,
        LocalMetricCounter.BREAKER_HALF_OPEN,
        LocalMetricCounter.BREAKER_CLOSE,
        LocalMetricCounter.SHUTDOWN_DROPPED,
        LocalMetricCounter.SHUTDOWN_TIMEOUT,
        LocalMetricCounter.SHUTDOWN_COMPLETE
    }, LocalMetricCounter.values());

    EnumMap<LocalMetricCounter, Long> values =
        new EnumMap<LocalMetricCounter, Long>(LocalMetricCounter.class);
    values.put(LocalMetricCounter.RECORD_ENQUEUED, 7L);
    LocalHistoryMetricsCounters counters = new ImmutableLocalHistoryMetricsCounters(values);
    values.put(LocalMetricCounter.RECORD_ENQUEUED, 99L);
    assertEquals(7L, counters.value(LocalMetricCounter.RECORD_ENQUEUED));
    assertEquals(0L, counters.value(LocalMetricCounter.DRAIN_TIMEOUT));
    assertThrows(NullPointerException.class, () -> counters.value(null));
    assertThrows(IllegalArgumentException.class, () ->
        new ImmutableLocalHistoryMetricsCounters(Collections.singletonMap(
            LocalMetricCounter.RECORD_INVALID, -1L)));
  }

  @Test
  void localTestCatalogBuildsIsolatedGovernedSnapshots() {
    LocalTestCatalog.Builder builder = LocalTestCatalog.builder()
        .addLive(31, "test.live")
        .addRetired(32, "test.retired");
    HistoryMetricCatalog first = builder.build();
    builder.addLive(33, "test.later");
    HistoryMetricCatalog second = builder.build();

    assertEquals("test.live", first.find(31).get().name());
    assertTrue(first.find(32).get().retired());
    assertFalse(first.find(33).isPresent());
    assertTrue(second.find(33).isPresent());
    assertFalse(HistoryMetricCatalog.production().find(31).isPresent());

    assertThrows(IllegalArgumentException.class, () -> LocalTestCatalog.builder()
        .addLive(1, "same")
        .addRetired(1, "other")
        .build());
    assertThrows(IllegalArgumentException.class, () -> LocalTestCatalog.builder()
        .addLive(1, "same")
        .addRetired(2, "same")
        .build());
    assertThrows(IllegalArgumentException.class,
        () -> LocalTestCatalog.builder().addLive(0, "zero").build());
    assertThrows(IllegalArgumentException.class,
        () -> LocalTestCatalog.builder().addLive(1, null).build());
    assertThrows(IllegalArgumentException.class,
        () -> LocalTestCatalog.builder().addLive(1, "").build());
    assertThrows(IllegalArgumentException.class,
        () -> LocalTestCatalog.builder().addLive(1, repeat('n', 129)).build());
    assertThrows(IllegalArgumentException.class,
        () -> LocalTestCatalog.builder().addLive(1, "bad\uD83D").build());
  }

  @Test
  void localTestCatalogCountsMultibyteUtf8BytesNotJavaCharacters() {
    String bytes127 = multibyteName(127);
    String bytes128 = multibyteName(128);
    String bytes129 = multibyteName(129);
    assertEquals(127, bytes127.getBytes(StandardCharsets.UTF_8).length);
    assertEquals(128, bytes128.getBytes(StandardCharsets.UTF_8).length);
    assertEquals(129, bytes129.getBytes(StandardCharsets.UTF_8).length);
    assertEquals(65, bytes129.length());

    HistoryMetricCatalog accepted = LocalTestCatalog.builder()
        .addLive(41, bytes127)
        .addRetired(42, bytes128)
        .build();
    assertEquals(bytes127, accepted.find(41).get().name());
    assertEquals(bytes128, accepted.find(42).get().name());
    assertThrows(IllegalArgumentException.class, () -> LocalTestCatalog.builder()
        .addLive(43, bytes129)
        .build());
  }

  @Test
  void observationAndDeclarationSnapshotsAreDefensiveAndValueBased() {
    byte[] bytes = new byte[] {1, 2, 3};
    Map<String, DimValue> dimensions = new LinkedHashMap<String, DimValue>();
    dimensions.put("key", DimValue.of(bytes));
    Provenance provenance = new Provenance("app", "attempt", "version", 44L);
    LocalObservationSnapshot snapshot = new LocalObservationSnapshot(
        new MetricVersionId(31, 2), dimensions, 4.5, 33L, provenance, 9L);
    LocalObservationSnapshot same = new LocalObservationSnapshot(
        new MetricVersionId(31, 2),
        Collections.singletonMap("key", DimValue.of(new byte[] {1, 2, 3})),
        4.5,
        33L,
        new Provenance("app", "attempt", "version", 44L),
        9L);
    bytes[0] = 99;
    dimensions.clear();

    assertEquals(new MetricVersionId(31, 2), snapshot.metric());
    assertEquals(4.5, snapshot.value());
    assertEquals(33L, snapshot.timestampMs());
    assertEquals(9L, snapshot.acceptanceOrdinal());
    assertEquals(1, snapshot.dimensions().size());
    assertEquals(1, snapshot.dimensions().get("key").bytesValue()[0]);
    assertThrows(UnsupportedOperationException.class, () ->
        snapshot.dimensions().put("other", DimValue.of(1L)));
    assertEquals(provenance, snapshot.provenance());
    assertNotSame(provenance, snapshot.provenance());
    assertEquals(snapshot, same);
    assertEquals(snapshot.hashCode(), same.hashCode());
    assertFalse(snapshot.toString().contains("attempt"));
    assertFalse(snapshot.toString().contains("value=4.5"));

    Retention recommended =
        new Retention(Duration.ofDays(1), Duration.ofDays(7));
    Retention effective =
        new Retention(Duration.ofHours(12), Duration.ofDays(7));
    MetricSchema schema = new MetricSchema(
        new MetricVersionId(31, 2),
        Arrays.asList(new DimensionSpec("key", DimValue.Kind.BYTES)),
        recommended);
    LocalDeclarationSnapshot declaration =
        new LocalDeclarationSnapshot(schema, effective);
    LocalDeclarationSnapshot sameDeclaration =
        new LocalDeclarationSnapshot(schema, effective);
    assertEquals(schema, declaration.schema());
    assertEquals(effective, declaration.effectiveRetention());
    assertEquals(declaration, sameDeclaration);
    assertEquals(declaration.hashCode(), sameDeclaration.hashCode());
  }

  private static LocalCircuitBreakerPolicy breaker(
      int windowSize, int minSamples, double failureRate, double slowRate) {
    return LocalCircuitBreakerPolicy.of(
        windowSize,
        minSamples,
        failureRate,
        Duration.ofSeconds(1),
        slowRate,
        Duration.ofSeconds(1));
  }

  private static String multibyteName(int utf8Bytes) {
    StringBuilder builder = new StringBuilder((utf8Bytes + 1) / 2);
    for (int index = 0; index < utf8Bytes / 2; index++) {
      builder.append('\u00E9');
    }
    if ((utf8Bytes & 1) != 0) {
      builder.append('a');
    }
    return builder.toString();
  }

  private static void assertPublicMethods(Class<?> type, String... expected) {
    Set<String> actual = new HashSet<String>();
    for (Method method : type.getDeclaredMethods()) {
      if (Modifier.isPublic(method.getModifiers()) && !method.isSynthetic()) {
        actual.add(signature(method.getName(), method.getParameterTypes()) +
            ":" + method.getReturnType().getName());
      }
    }
    assertEquals(new HashSet<String>(Arrays.asList(expected)), actual);
  }

  private static String signature(String name, Class<?>[] parameters) {
    StringBuilder value = new StringBuilder(name).append('(');
    for (int index = 0; index < parameters.length; index++) {
      if (index != 0) {
        value.append(',');
      }
      value.append(parameters[index].getName());
    }
    return value.append(')').toString();
  }

  private static String repeat(char value, int count) {
    char[] values = new char[count];
    Arrays.fill(values, value);
    return new String(values);
  }
}
