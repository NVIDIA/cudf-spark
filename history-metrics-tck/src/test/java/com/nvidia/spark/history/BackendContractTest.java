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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import com.nvidia.spark.history.tck.HistoryMetricsProviderFactory;
import com.nvidia.spark.history.tck.HistoryMetricsProviderFixture;

import org.junit.jupiter.api.Test;

/** Behavioral and compiled-contract coverage for FR-08, FR-14, and FR-17 backend SPI shapes. */
class BackendContractTest {
  @Test
  void fr08WriteResultEnforcesBatchStatusRelationships() {
    assertEquals(0, WriteResult.ok(0).accepted());
    assertEquals(0, WriteResult.ok(0).rejected());
    assertEquals(Status.Code.OK, WriteResult.ok(3).status().code());

    WriteResult rejected =
        WriteResult.rejected(2, Status.of(Status.Code.NOT_DECLARED, "not declared"));
    WriteResult partial =
        WriteResult.unavailable(1, 1, "partial backend write");
    assertEquals(0, rejected.accepted());
    assertEquals(2, rejected.rejected());
    assertEquals(1, partial.accepted());
    assertEquals(1, partial.rejected());
    assertEquals(Status.Code.UNAVAILABLE, partial.status().code());

    assertThrows(IllegalArgumentException.class, () ->
        WriteResult.of(-1, 1, Status.ok()));
    assertThrows(IllegalArgumentException.class, () ->
        WriteResult.of(1, 1, Status.ok()));
    assertThrows(IllegalArgumentException.class, () ->
        WriteResult.of(1, 0, Status.of(Status.Code.DENIED, "denied")));
    assertThrows(IllegalArgumentException.class, () ->
        WriteResult.of(1, 0, Status.of(Status.Code.UNAVAILABLE, "failure")));
    assertThrows(IllegalArgumentException.class, () ->
        WriteResult.of(0, 1, Status.of(Status.Code.DEADLINE_EXCEEDED, "late")));
  }

  @Test
  void fr14CurrentBackendMethodsRemainAbstractAndCloseHasNoCheckedException()
      throws Exception {
    assertAbstract("info");
    assertAbstract("declare", List.class, Duration.class);
    assertAbstract("record", List.class);
    assertAbstract("summarize", List.class, Duration.class);

    Method close = HistoryMetricsBackend.class.getMethod("close");
    assertTrue(Modifier.isAbstract(close.getModifiers()));
    assertEquals(0, close.getExceptionTypes().length);
  }

  @Test
  void fr17BackendExposesOnlyTheFrozenPublicSpiSurface() {
    PublicApiSurface.assertMethods(
        HistoryMetricsBackend.class,
        "info():com.nvidia.spark.history.BackendInfo",
        "declare(java.util.List,java.time.Duration):java.util.List",
        "record(java.util.List):com.nvidia.spark.history.WriteResult",
        "summarize(java.util.List,java.time.Duration):java.util.List",
        "close():void");

    HistoryMetricsBackend backend = new StubBackend();
    assertEquals(HistoryMetricsApi.CURRENT_API_VERSION, backend.info().apiVersion());
    assertEquals(0, backend.record(Collections.<StampedObservation>emptyList()).accepted());
  }

  @Test
  void publicApiUsesSparkHistoryNamespaceOnly() throws Exception {
    assertEquals(
        "com.nvidia.spark.history.MetricStore",
        Class.forName("com.nvidia.spark.history.MetricStore").getName());
    assertThrows(
        ClassNotFoundException.class,
        () -> Class.forName("com.nvidia.spark.rapids.history.MetricStore"));
  }

  @Test
  void nfr16HighRiskApiAndBlackBoxTckSurfacesRemainExact() {
    PublicApiSurface.assertConstructors(
        MetricVersionId.class, "MetricVersionId(int,int)");
    PublicApiSurface.assertMethods(
        MetricVersionId.class,
        "metricId():int",
        "version():int",
        "packedKey():long",
        "fromPackedKey(long):com.nvidia.spark.history.MetricVersionId",
        "equals(java.lang.Object):boolean",
        "hashCode():int",
        "toString():java.lang.String");
    assertThrows(ClassNotFoundException.class,
        () -> Class.forName("com.nvidia.spark.history.MetricId"));
    PublicApiSurface.assertConstructors(
        MetricSchema.class,
        "MetricSchema(com.nvidia.spark.history.MetricVersionId,java.util.List," +
            "com.nvidia.spark.history.Retention)");
    PublicApiSurface.assertMethods(
        MetricSchema.class,
        "metric():com.nvidia.spark.history.MetricVersionId",
        "dimensions():java.util.List",
        "recommendedRetention():com.nvidia.spark.history.Retention",
        "equals(java.lang.Object):boolean",
        "hashCode():int",
        "toString():java.lang.String");
    PublicApiSurface.assertConstructors(
        Retention.class, "Retention(java.time.Duration,java.time.Duration)");
    PublicApiSurface.assertMethods(
        Retention.class,
        "planningMaxAge():java.time.Duration",
        "storageRetention():java.time.Duration",
        "equals(java.lang.Object):boolean",
        "hashCode():int",
        "toString():java.lang.String");
    PublicApiSurface.assertConstructors(WriteResult.class);
    PublicApiSurface.assertMethods(
        WriteResult.class,
        "ok(int):com.nvidia.spark.history.WriteResult",
        "rejected(int,com.nvidia.spark.history.Status):" +
            "com.nvidia.spark.history.WriteResult",
        "unavailable(int,int,java.lang.String):com.nvidia.spark.history.WriteResult",
        "of(int,int,com.nvidia.spark.history.Status):" +
            "com.nvidia.spark.history.WriteResult",
        "accepted():int",
        "rejected():int",
        "status():com.nvidia.spark.history.Status",
        "equals(java.lang.Object):boolean",
        "hashCode():int",
        "toString():java.lang.String");
    PublicApiSurface.assertConstructors(Summary.class);
    PublicApiSurface.assertMethods(
        Summary.class,
        "of(long,double,double,double,long,long):com.nvidia.spark.history.Summary",
        "count():long",
        "mean():double",
        "min():double",
        "max():double",
        "firstObservedMs():long",
        "lastObservedMs():long",
        "equals(java.lang.Object):boolean",
        "hashCode():int",
        "toString():java.lang.String");

    PublicApiSurface.assertMethods(
        HistoryMetricsProviderFactory.class,
        "open(com.nvidia.spark.history.HistoryMetricCatalog,long,java.time.Duration):" +
            "com.nvidia.spark.history.tck.HistoryMetricsProviderFixture");
    PublicApiSurface.assertMethods(
        HistoryMetricsProviderFixture.class,
        "catalog():com.nvidia.spark.history.HistoryMetricCatalog",
        "store():com.nvidia.spark.history.MetricStore",
        "setProviderTime(long):void",
        "awaitWrites(java.time.Duration):boolean",
        "close():void");
  }

  private static void assertAbstract(String name, Class<?>... parameterTypes) throws Exception {
    Method method = HistoryMetricsBackend.class.getMethod(name, parameterTypes);
    assertTrue(Modifier.isAbstract(method.getModifiers()));
    assertFalse(method.isDefault());
  }

  private static final class StubBackend implements HistoryMetricsBackend {
    @Override
    public BackendInfo info() {
      return new BackendInfo(HistoryMetricsApi.CURRENT_API_VERSION, "stub");
    }

    @Override
    public List<SchemaStatus> declare(List<MetricSchema> schemas, Duration timeout) {
      return Collections.emptyList();
    }

    @Override
    public WriteResult record(List<StampedObservation> observations) {
      return WriteResult.ok(observations.size());
    }

    @Override
    public List<SummaryResponse> summarize(
        List<SummaryRequest> requests, Duration timeout) {
      return Collections.emptyList();
    }

    @Override
    public void close() {
    }
  }
}
