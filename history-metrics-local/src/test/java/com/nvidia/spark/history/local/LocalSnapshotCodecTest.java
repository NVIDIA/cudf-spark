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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;

import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsApi;
import com.nvidia.spark.history.LocalTestCatalog;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Observation;
import com.nvidia.spark.history.Provenance;
import com.nvidia.spark.history.StampedObservation;
import com.nvidia.spark.history.Retention;

class LocalSnapshotCodecTest {
  private static final MetricVersionId METRIC = new MetricVersionId(1, 1);
  private static final HistoryMetricCatalog CATALOG =
      LocalTestCatalog.builder().addLive(1, "metric-one").build();
  private static final Duration MAXIMUM_AGE = Duration.ofDays(30);

  @Test
  void deterministicRoundTripPreservesDurableStateAndExactDurations() throws Exception {
    LocalSnapshotState first = richState(false);
    LocalSnapshotState reordered = richState(true);

    byte[] firstBytes = LocalSnapshotCodec.encode(first);
    byte[] secondBytes = LocalSnapshotCodec.encode(reordered);
    assertArrayEquals(firstBytes, secondBytes);

    LocalSnapshotState decoded =
        LocalSnapshotCodec.decode(firstBytes, richCatalog(), Duration.ofDays(365));
    assertEquals(first, decoded);
    assertEquals(Duration.ofSeconds(12, 345_678_901),
        decoded.declarations().get(0).schema().recommendedRetention().planningMaxAge());
    assertEquals(Duration.ofSeconds(50, 987_654_321),
        decoded.declarations().get(0).schema().recommendedRetention().storageRetention());
    assertEquals(Duration.ofSeconds(10, 123_456_789),
        decoded.declarations().get(0).effectiveRetention().planningMaxAge());
    assertEquals(Duration.ofSeconds(40, 222_333_444),
        decoded.declarations().get(0).effectiveRetention().storageRetention());
    assertTrue(decoded.catalog().get(0).retired());
    assertFalse(decoded.catalog().get(1).retired());
    assertEquals(10L, decoded.nextAcceptanceOrdinal());
    assertEquals(4L, decoded.observations().get(0).acceptanceOrdinal());
    assertEquals(9L, decoded.observations().get(1).acceptanceOrdinal());
  }

  @Test
  void stateDefensivelyCopiesAndExposesImmutableOrderedLists() {
    MetricSchema schema = new MetricSchema(
        METRIC,
        Collections.<DimensionSpec>emptyList(),
        new Retention(Duration.ofSeconds(1), Duration.ofSeconds(2)));
    List<LocalDeclarationSnapshot> declarations =
        new ArrayList<LocalDeclarationSnapshot>();
    declarations.add(new LocalDeclarationSnapshot(
        schema, schema.recommendedRetention()));
    List<LocalObservationSnapshot> observations =
        new ArrayList<LocalObservationSnapshot>();
    observations.add(new LocalObservationSnapshot(
        METRIC,
        Collections.<String, DimValue>emptyMap(),
        1.0,
        1L,
        new Provenance("app", null, "plugin", 2L),
        1L));

    LocalSnapshotState state =
        LocalSnapshotState.capture(CATALOG, declarations, observations, 2L);
    declarations.clear();
    observations.clear();

    assertEquals(1, state.declarations().size());
    assertEquals(1, state.observations().size());
    assertThrows(UnsupportedOperationException.class,
        () -> state.catalog().clear());
    assertThrows(UnsupportedOperationException.class,
        () -> state.declarations().clear());
    assertThrows(UnsupportedOperationException.class,
        () -> state.observations().clear());
  }

  @Test
  void headerCarriesCurrentFormatApiZeroFlagsAndPayloadLength() throws Exception {
    byte[] image = LocalSnapshotCodec.encode(emptyState());
    ByteBuffer header = ByteBuffer.wrap(image);

    assertEquals(LocalSnapshotCodec.MAGIC, header.getInt());
    assertEquals(LocalSnapshotCodec.FORMAT_MAJOR, header.getShort());
    assertEquals(LocalSnapshotCodec.FORMAT_MINOR, header.getShort());
    assertEquals(HistoryMetricsApi.CURRENT_API_VERSION, header.getInt());
    assertEquals(0, header.getInt());
    assertEquals(image.length - LocalSnapshotCodec.HEADER_BYTES
        - LocalSnapshotCodec.TRAILER_BYTES, header.getInt());
  }

  @Test
  void usesStandardStrictUtf8RatherThanModifiedUtf8() throws Exception {
    String value = "nul-\u0000-emoji-\uD83D\uDE00";
    LocalSnapshotState state = singleStringObservation(value);
    byte[] image = LocalSnapshotCodec.encode(state);
    assertTrue(contains(image, value.getBytes(StandardCharsets.UTF_8)));
    LocalSnapshotState decoded =
        LocalSnapshotCodec.decode(image, CATALOG, MAXIMUM_AGE);

    assertEquals(value,
        decoded.observations().get(0).dimensions().get("key").stringValue());
  }

  @Test
  void backendCaptureIsCoherentAndExcludesRuntimeCounters() throws Exception {
    LocalHistoryMetricsBackend backend = LocalHistoryMetricsBackend.create(
        CATALOG, Clock.systemUTC(), MAXIMUM_AGE);
    MetricSchema schema = new MetricSchema(
        METRIC,
        Collections.singletonList(new DimensionSpec("key", DimValue.Kind.STRING)),
        new Retention(Duration.ofSeconds(5), Duration.ofSeconds(10)));
    assertEquals(1, backend.declare(
        Collections.singletonList(schema), Duration.ofSeconds(1)).size());
    Observation observation = new Observation(
        METRIC,
        Collections.singletonMap("key", DimValue.of("value")),
        3.5,
        11L);
    backend.record(Collections.singletonList(new StampedObservation(
        observation, new Provenance("app", null, "plugin", 12L))));

    byte[] beforeCounterOnlyCall =
        LocalSnapshotCodec.encode(backend.captureSnapshotState());
    backend.declare(Collections.singletonList(schema), Duration.ofSeconds(1));
    byte[] afterCounterOnlyCall =
        LocalSnapshotCodec.encode(backend.captureSnapshotState());

    assertArrayEquals(beforeCounterOnlyCall, afterCounterOnlyCall);
    LocalSnapshotState restored = LocalSnapshotCodec.decode(
        beforeCounterOnlyCall, CATALOG, MAXIMUM_AGE);
    assertEquals(1, restored.declarations().size());
    assertEquals(1, restored.observations().size());
    assertEquals(2L, restored.nextAcceptanceOrdinal());
  }

  @Test
  void crcCorruptionIsIntegrityAfterSafeFraming() throws Exception {
    byte[] image = LocalSnapshotCodec.encode(singleStringObservation("value"));
    image[LocalSnapshotCodec.HEADER_BYTES + 16] ^= 1;

    assertReason(LocalSnapshotException.Reason.INTEGRITY,
        () -> LocalSnapshotCodec.decode(image, CATALOG, MAXIMUM_AGE));
  }

  @Test
  void truncationTrailingBytesUnknownVersionAndFlagsHaveExactReasons() throws Exception {
    byte[] image = LocalSnapshotCodec.encode(emptyState());

    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotCodec.decode(Arrays.copyOf(image, image.length - 1),
            CATALOG, MAXIMUM_AGE));

    byte[] trailing = Arrays.copyOf(image, image.length + 1);
    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotCodec.decode(trailing, CATALOG, MAXIMUM_AGE));

    byte[] version = image.clone();
    ByteBuffer.wrap(version).putShort(4, (short) (LocalSnapshotCodec.FORMAT_MAJOR + 1));
    assertReason(LocalSnapshotException.Reason.VERSION,
        () -> LocalSnapshotCodec.decode(version, CATALOG, MAXIMUM_AGE));

    byte[] api = image.clone();
    ByteBuffer.wrap(api).putInt(8, HistoryMetricsApi.CURRENT_API_VERSION + 1);
    assertReason(LocalSnapshotException.Reason.VERSION,
        () -> LocalSnapshotCodec.decode(api, CATALOG, MAXIMUM_AGE));

    byte[] flags = image.clone();
    ByteBuffer.wrap(flags).putInt(12, 1);
    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotCodec.decode(flags, CATALOG, MAXIMUM_AGE));
  }

  @Test
  void wholeFileAndDeclaredLengthsAreBoundedBeforeReadingOrChecksum() throws Exception {
    CountingInputStream unread = new CountingInputStream();
    assertReason(LocalSnapshotException.Reason.BOUNDS,
        () -> LocalSnapshotCodec.decode(
            unread, (long) LocalSnapshotCodec.MAX_FILE_BYTES + 1, CATALOG, MAXIMUM_AGE));
    assertEquals(0, unread.reads);

    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(payload);
    out.writeInt(100);
    out.writeInt(0);
    out.writeInt(0);
    out.writeInt(0);
    byte[] badLength = framedImage(
        payload.toByteArray(), HistoryMetricsApi.CURRENT_API_VERSION, 0);
    assertReason(LocalSnapshotException.Reason.BOUNDS,
        () -> LocalSnapshotCodec.decode(badLength, CATALOG, MAXIMUM_AGE));
  }

  @Test
  void inBoundPhysicalStringAndDimensionTruncationAreFormatErrors() throws Exception {
    byte[] truncatedCatalogString = bytes(out -> {
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(4);
      out.writeByte('a');
      out.writeByte('b');
    });
    byte[] truncatedStringImage = image(
        truncatedCatalogString,
        countSection(0),
        countSection(0),
        allocatorSection(1L));
    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotCodec.decode(
            truncatedStringImage, CATALOG, MAXIMUM_AGE));

    byte[] declaration = declarationSection(
        Collections.singletonList(declarationRecord(
            METRIC,
            DimValue.Kind.BYTES,
            seconds(5),
            seconds(10),
            seconds(5),
            seconds(10))));
    byte[] truncatedDimension = bytes(out -> {
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(1);
      out.writeByte(3);
      out.writeShort(8);
      out.write(new byte[4]);
    });
    byte[] truncatedDimensionImage = image(
        catalogSection(1, "metric-one", false),
        declaration,
        truncatedDimension,
        allocatorSection(1L));
    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotCodec.decode(
            truncatedDimensionImage, CATALOG, MAXIMUM_AGE));
  }

  @Test
  void countAndExistingStringBoundsAreRejectedWithoutLargeAllocations() throws Exception {
    byte[] excessiveCount = image(
        bytes(out -> out.writeInt(LocalSnapshotCodec.MAX_CATALOG_ENTRIES + 1)),
        countSection(0), countSection(0), allocatorSection(1L));
    excessiveCount[excessiveCount.length - 1] ^= 1;
    assertReason(LocalSnapshotException.Reason.BOUNDS,
        () -> LocalSnapshotCodec.decode(excessiveCount, CATALOG, MAXIMUM_AGE));

    byte[] excessiveDeclarations = image(
        catalogSection(1, "metric-one", false),
        countSection(LocalSnapshotCodec.MAX_DECLARATIONS + 1),
        countSection(0), allocatorSection(1L));
    assertReason(LocalSnapshotException.Reason.BOUNDS,
        () -> LocalSnapshotCodec.decode(excessiveDeclarations, CATALOG, MAXIMUM_AGE));

    byte[] excessiveObservations = image(
        catalogSection(1, "metric-one", false),
        countSection(0),
        countSection(LocalSnapshotCodec.MAX_OBSERVATIONS + 1),
        allocatorSection(1L));
    assertReason(LocalSnapshotException.Reason.BOUNDS,
        () -> LocalSnapshotCodec.decode(excessiveObservations, CATALOG, MAXIMUM_AGE));

    String tooLong = repeat("é", 65);
    byte[] excessiveName = image(
        catalogSection(1, tooLong, false),
        countSection(0), countSection(0), allocatorSection(1L));
    assertReason(LocalSnapshotException.Reason.BOUNDS,
        () -> LocalSnapshotCodec.decode(excessiveName, CATALOG, MAXIMUM_AGE));
  }

  @Test
  void exactExistingStringAndCanonicalDimensionBoundsRoundTrip() throws Exception {
    String maximumName = repeat("x", 128);
    HistoryMetricCatalog catalog =
        LocalTestCatalog.builder().addLive(1, maximumName).build();
    MetricSchema schema = new MetricSchema(
        METRIC,
        Collections.singletonList(new DimensionSpec("key", DimValue.Kind.BYTES)),
        new Retention(Duration.ofSeconds(1), Duration.ofSeconds(2)));
    byte[] maximumPayload = new byte[DimValue.MAX_CANONICAL_BYTES - 3];
    Arrays.fill(maximumPayload, (byte) 7);
    LocalSnapshotState state = LocalSnapshotState.capture(
        catalog,
        Collections.singletonList(
            new LocalDeclarationSnapshot(schema, schema.recommendedRetention())),
        Collections.singletonList(new LocalObservationSnapshot(
            METRIC,
            Collections.singletonMap("key", DimValue.of(maximumPayload)),
            1.0,
            1L,
            new Provenance("app", null, "plugin", 2L),
            1L)),
        2L);

    LocalSnapshotState decoded = LocalSnapshotCodec.decode(
        LocalSnapshotCodec.encode(state), catalog, MAXIMUM_AGE);
    assertEquals(maximumName, decoded.catalog().get(0).name());
    assertArrayEquals(maximumPayload,
        decoded.observations().get(0).dimensions().get("key").bytesValue());
  }

  @Test
  void canonicalDimensionBoundIsCheckedBeforePayloadAllocation() throws Exception {
    byte[] declaration = declarationSection(
        Collections.singletonList(declarationRecord(
            METRIC, DimValue.Kind.BYTES, seconds(5), seconds(5), seconds(5), seconds(5))));
    byte[] oversizedCanonical = bytes(out -> {
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(1);
      out.writeByte(3);
      out.writeShort(254);
      out.write(new byte[254]);
      out.writeDouble(1.0);
      out.writeLong(1L);
      writeString(out, "app");
      out.writeByte(0);
      writeString(out, "plugin");
      out.writeLong(2L);
      out.writeLong(1L);
    });
    byte[] image = image(
        catalogSection(1, "metric-one", false),
        declaration,
        oversizedCanonical,
        allocatorSection(2L));

    assertReason(LocalSnapshotException.Reason.BOUNDS,
        () -> LocalSnapshotCodec.decode(image, CATALOG, MAXIMUM_AGE));
  }

  @Test
  void malformedUtf8AndInvalidDurationNanosAreFormatErrors() throws Exception {
    byte[] badUtf8Catalog = bytes(out -> {
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(2);
      out.writeByte(0xC3);
      out.writeByte(0x28);
      out.writeByte(0);
    });
    byte[] badUtf8 = image(
        badUtf8Catalog, countSection(0), countSection(0), allocatorSection(1L));
    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotCodec.decode(badUtf8, CATALOG, MAXIMUM_AGE));

    byte[] invalidDuration = image(
        catalogSection(1, "metric-one", false),
        declarationSection(Collections.singletonList(declarationRecord(
            METRIC, DimValue.Kind.STRING,
            duration(1, 1_000_000_000), seconds(2), seconds(1), seconds(2)))),
        countSection(0),
        allocatorSection(1L));
    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotCodec.decode(invalidDuration, CATALOG, MAXIMUM_AGE));
  }

  @Test
  void validDurationMaximumNanosRoundTrips() throws Exception {
    Duration endpoint = Duration.ofSeconds(1, 999_999_999);
    byte[] image = image(
        catalogSection(1, "metric-one", false),
        declarationSection(Collections.singletonList(declarationRecord(
            METRIC,
            DimValue.Kind.STRING,
            duration(endpoint.getSeconds(), endpoint.getNano()),
            seconds(2),
            duration(endpoint.getSeconds(), endpoint.getNano()),
            seconds(2)))),
        countSection(0),
        allocatorSection(1L));

    LocalSnapshotState decoded =
        LocalSnapshotCodec.decode(image, CATALOG, MAXIMUM_AGE);
    assertEquals(endpoint,
        decoded.declarations().get(0).schema()
            .recommendedRetention().planningMaxAge());
    assertEquals(endpoint,
        decoded.declarations().get(0).effectiveRetention().planningMaxAge());
  }

  @Test
  void exactCatalogAssociationAndRetiredStateAreRequired() throws Exception {
    byte[] image = LocalSnapshotCodec.encode(emptyState());
    HistoryMetricCatalog wrongName =
        LocalTestCatalog.builder().addLive(1, "different").build();
    HistoryMetricCatalog retired =
        LocalTestCatalog.builder().addRetired(1, "metric-one").build();

    assertReason(LocalSnapshotException.Reason.CATALOG_CONFLICT,
        () -> LocalSnapshotCodec.decode(image, wrongName, MAXIMUM_AGE));
    assertReason(LocalSnapshotException.Reason.CATALOG_CONFLICT,
        () -> LocalSnapshotCodec.decode(image, retired, MAXIMUM_AGE));
  }

  @Test
  void duplicateDimensionNameInOneSchemaIsStructuralFormatError() throws Exception {
    byte[] invalidSchema = bytes(out -> {
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(2);
      writeString(out, "key");
      out.writeByte(1);
      writeString(out, "key");
      out.writeByte(1);
      out.write(seconds(5));
      out.write(seconds(10));
      out.write(seconds(5));
      out.write(seconds(10));
    });
    byte[] image = image(
        catalogSection(1, "metric-one", false),
        invalidSchema,
        countSection(0),
        allocatorSection(1L));

    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotCodec.decode(image, CATALOG, MAXIMUM_AGE));
  }

  @Test
  void duplicateDeclarationContradictionsUseSchemaAndPolicyReasons() throws Exception {
    byte[] first = declarationRecord(
        METRIC, DimValue.Kind.STRING, seconds(5), seconds(10), seconds(5), seconds(10));
    byte[] differentSchema = declarationRecord(
        METRIC, DimValue.Kind.LONG, seconds(5), seconds(10), seconds(5), seconds(10));
    assertReason(LocalSnapshotException.Reason.SCHEMA_CONFLICT,
        () -> decodeDeclarations(first, differentSchema));

    byte[] differentRecommended = declarationRecord(
        METRIC, DimValue.Kind.STRING, seconds(6), seconds(10), seconds(5), seconds(10));
    assertReason(LocalSnapshotException.Reason.SCHEMA_CONFLICT,
        () -> decodeDeclarations(first, differentRecommended));

    byte[] differentPolicy = declarationRecord(
        METRIC, DimValue.Kind.STRING, seconds(5), seconds(10), seconds(4), seconds(10));
    assertReason(LocalSnapshotException.Reason.POLICY_CONFLICT,
        () -> decodeDeclarations(first, differentPolicy));

    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> decodeDeclarations(first, first));
  }

  @Test
  void observationSchemaMismatchIsReachableSchemaConflict() throws Exception {
    byte[] declaration = declarationSection(Collections.singletonList(declarationRecord(
        METRIC, DimValue.Kind.STRING, seconds(5), seconds(10), seconds(5), seconds(10))));
    byte[] observation = observationSection(DimValue.of(7L), 1L);
    byte[] image = image(
        catalogSection(1, "metric-one", false),
        declaration,
        observation,
        allocatorSection(2L));

    assertReason(LocalSnapshotException.Reason.SCHEMA_CONFLICT,
        () -> LocalSnapshotCodec.decode(image, CATALOG, MAXIMUM_AGE));

    byte[] missingComponent = bytes(out -> {
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(0);
      out.writeDouble(1.0);
      out.writeLong(1L);
      writeString(out, "app");
      out.writeByte(0);
      writeString(out, "plugin");
      out.writeLong(2L);
      out.writeLong(1L);
    });
    byte[] missingComponentImage = image(
        catalogSection(1, "metric-one", false),
        declaration,
        missingComponent,
        allocatorSection(2L));
    assertReason(LocalSnapshotException.Reason.SCHEMA_CONFLICT,
        () -> LocalSnapshotCodec.decode(
            missingComponentImage, CATALOG, MAXIMUM_AGE));
  }

  @Test
  void policyEnvelopeAndOrderingViolationsHaveStableReasons() throws Exception {
    byte[] overEnvelope = image(
        catalogSection(1, "metric-one", false),
        declarationSection(Collections.singletonList(declarationRecord(
            METRIC, DimValue.Kind.STRING, seconds(5), seconds(10),
            seconds(6), seconds(10)))),
        countSection(0),
        allocatorSection(1L));
    assertReason(LocalSnapshotException.Reason.POLICY_CONFLICT,
        () -> LocalSnapshotCodec.decode(overEnvelope, CATALOG, Duration.ofSeconds(5)));

    byte[] observation = observationSection(DimValue.of("v"), 2L);
    byte[] invalidNext = image(
        catalogSection(1, "metric-one", false),
        declarationSection(Collections.singletonList(declarationRecord(
            METRIC, DimValue.Kind.STRING, seconds(5), seconds(10),
            seconds(5), seconds(10)))),
        observation,
        allocatorSection(2L));
    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotCodec.decode(invalidNext, CATALOG, MAXIMUM_AGE));
  }

  @Test
  void checkedExceptionHasClosedReasonAndRedactedDiagnosticShape() {
    LocalSnapshotException failure = new LocalSnapshotException(
        LocalSnapshotException.Reason.FORMAT, "snapshot framing is invalid");

    assertTrue(failure instanceof IOException);
    assertEquals(LocalSnapshotException.Reason.FORMAT, failure.reason());
    assertFalse(failure.getMessage().isEmpty());
    assertArrayEquals(new LocalSnapshotException.Reason[] {
        LocalSnapshotException.Reason.TIMEOUT,
        LocalSnapshotException.Reason.BUSY,
        LocalSnapshotException.Reason.IO,
        LocalSnapshotException.Reason.ATOMIC_MOVE_UNSUPPORTED,
        LocalSnapshotException.Reason.FORMAT,
        LocalSnapshotException.Reason.VERSION,
        LocalSnapshotException.Reason.INTEGRITY,
        LocalSnapshotException.Reason.BOUNDS,
        LocalSnapshotException.Reason.CATALOG_CONFLICT,
        LocalSnapshotException.Reason.SCHEMA_CONFLICT,
        LocalSnapshotException.Reason.POLICY_CONFLICT
    }, LocalSnapshotException.Reason.values());
  }

  private static LocalSnapshotState richState(boolean reverse) {
    HistoryMetricCatalog catalog = reverse ? richCatalogReversed() : richCatalog();
    MetricSchema firstSchema = new MetricSchema(
        new MetricVersionId(2, 1),
        Collections.<DimensionSpec>emptyList(),
        new Retention(Duration.ofSeconds(2, 3), Duration.ofSeconds(20, 4)));
    List<DimensionSpec> dimensions = Arrays.asList(
        new DimensionSpec("text", DimValue.Kind.STRING),
        new DimensionSpec("number", DimValue.Kind.LONG),
        new DimensionSpec("blob", DimValue.Kind.BYTES));
    MetricSchema secondSchema = new MetricSchema(
        new MetricVersionId(1, 2),
        dimensions,
        new Retention(
            Duration.ofSeconds(12, 345_678_901),
            Duration.ofSeconds(50, 987_654_321)));
    List<LocalDeclarationSnapshot> declarations = new ArrayList<LocalDeclarationSnapshot>();
    declarations.add(new LocalDeclarationSnapshot(
        firstSchema, new Retention(Duration.ofSeconds(2, 3), Duration.ofSeconds(20, 4))));
    declarations.add(new LocalDeclarationSnapshot(
        secondSchema,
        new Retention(
            Duration.ofSeconds(10, 123_456_789),
            Duration.ofSeconds(40, 222_333_444))));

    Map<String, DimValue> values = new LinkedHashMap<String, DimValue>();
    values.put("blob", DimValue.of(new byte[] {0, 1, (byte) 255}));
    values.put("number", DimValue.of(Long.MIN_VALUE));
    values.put("text", DimValue.of("nul-\u0000-emoji-\uD83D\uDE00"));
    List<LocalObservationSnapshot> observations = new ArrayList<LocalObservationSnapshot>();
    observations.add(new LocalObservationSnapshot(
        secondSchema.metric(), values, -Double.MAX_VALUE, Long.MIN_VALUE,
        new Provenance("app-\u03B1", null, "plugin-1", Long.MAX_VALUE), 9L));
    observations.add(new LocalObservationSnapshot(
        firstSchema.metric(), Collections.<String, DimValue>emptyMap(), Double.MAX_VALUE, 7L,
        new Provenance("app-2", "attempt-1", "plugin-2", 8L), 4L));

    if (reverse) {
      Collections.reverse(declarations);
      Collections.reverse(observations);
    }
    return LocalSnapshotState.capture(catalog, declarations, observations, 10L);
  }

  private static HistoryMetricCatalog richCatalog() {
    return LocalTestCatalog.builder()
        .addLive(2, "metric-two")
        .addRetired(1, "metric-one-retired")
        .build();
  }

  private static HistoryMetricCatalog richCatalogReversed() {
    return LocalTestCatalog.builder()
        .addRetired(1, "metric-one-retired")
        .addLive(2, "metric-two")
        .build();
  }

  private static LocalSnapshotState emptyState() {
    return LocalSnapshotState.capture(
        CATALOG,
        Collections.<LocalDeclarationSnapshot>emptyList(),
        Collections.<LocalObservationSnapshot>emptyList(),
        1L);
  }

  private static LocalSnapshotState singleStringObservation(String value) {
    MetricSchema schema = new MetricSchema(
        METRIC,
        Collections.singletonList(new DimensionSpec("key", DimValue.Kind.STRING)),
        new Retention(Duration.ofSeconds(5), Duration.ofSeconds(10)));
    LocalDeclarationSnapshot declaration =
        new LocalDeclarationSnapshot(schema, schema.recommendedRetention());
    LocalObservationSnapshot observation = new LocalObservationSnapshot(
        METRIC,
        Collections.singletonMap("key", DimValue.of(value)),
        1.25,
        123L,
        new Provenance("app", "attempt", "plugin", 456L),
        1L);
    return LocalSnapshotState.capture(
        CATALOG,
        Collections.singletonList(declaration),
        Collections.singletonList(observation),
        2L);
  }

  private static void decodeDeclarations(byte[]... records) throws Exception {
    byte[] declarations = declarationSection(Arrays.asList(records));
    byte[] image = image(
        catalogSection(1, "metric-one", false),
        declarations,
        countSection(0),
        allocatorSection(1L));
    LocalSnapshotCodec.decode(image, CATALOG, MAXIMUM_AGE);
  }

  private static byte[] declarationRecord(
      MetricVersionId metric,
      DimValue.Kind kind,
      byte[] recommendedPlanning,
      byte[] recommendedStorage,
      byte[] effectivePlanning,
      byte[] effectiveStorage) throws IOException {
    return bytes(out -> {
      out.writeInt(metric.metricId());
      out.writeInt(metric.version());
      out.writeInt(1);
      writeString(out, "key");
      out.writeByte(kind.ordinal() + 1);
      out.write(recommendedPlanning);
      out.write(recommendedStorage);
      out.write(effectivePlanning);
      out.write(effectiveStorage);
    });
  }

  private static byte[] declarationSection(List<byte[]> records) throws IOException {
    return bytes(out -> {
      out.writeInt(records.size());
      for (byte[] record : records) {
        out.write(record);
      }
    });
  }

  private static byte[] observationSection(DimValue value, long ordinal) throws IOException {
    return bytes(out -> {
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(1);
      out.write(value.canonicalBytes());
      out.writeDouble(1.0);
      out.writeLong(1L);
      writeString(out, "app");
      out.writeByte(0);
      writeString(out, "plugin");
      out.writeLong(2L);
      out.writeLong(ordinal);
    });
  }

  private static byte[] catalogSection(int id, String name, boolean retired) throws IOException {
    return bytes(out -> {
      out.writeInt(1);
      out.writeInt(id);
      writeString(out, name);
      out.writeByte(retired ? 1 : 0);
    });
  }

  private static byte[] countSection(int count) throws IOException {
    return bytes(out -> out.writeInt(count));
  }

  private static byte[] allocatorSection(long next) throws IOException {
    return bytes(out -> out.writeLong(next));
  }

  private static byte[] seconds(long value) throws IOException {
    return duration(value, 0);
  }

  private static byte[] duration(long seconds, int nanos) throws IOException {
    return bytes(out -> {
      out.writeLong(seconds);
      out.writeInt(nanos);
    });
  }

  private static byte[] image(
      byte[] catalog, byte[] declarations, byte[] observations, byte[] allocator)
      throws IOException {
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(payload);
    writeSection(out, catalog);
    writeSection(out, declarations);
    writeSection(out, observations);
    writeSection(out, allocator);
    return framedImage(payload.toByteArray(), HistoryMetricsApi.CURRENT_API_VERSION, 0);
  }

  private static byte[] framedImage(
      byte[] payload, int apiVersion, int flags) throws IOException {
    ByteArrayOutputStream image = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(image);
    out.writeInt(LocalSnapshotCodec.MAGIC);
    out.writeShort(LocalSnapshotCodec.FORMAT_MAJOR);
    out.writeShort(LocalSnapshotCodec.FORMAT_MINOR);
    out.writeInt(apiVersion);
    out.writeInt(flags);
    out.writeInt(payload.length);
    out.write(payload);
    CRC32 crc = new CRC32();
    crc.update(payload);
    out.writeInt((int) crc.getValue());
    return image.toByteArray();
  }

  private static void writeSection(DataOutputStream out, byte[] section) throws IOException {
    out.writeInt(section.length);
    out.write(section);
  }

  private static void writeString(DataOutputStream out, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(encoded.length);
    out.write(encoded);
  }

  private static boolean contains(byte[] bytes, byte[] expected) {
    for (int offset = 0; offset <= bytes.length - expected.length; offset++) {
      boolean matches = true;
      for (int index = 0; index < expected.length; index++) {
        if (bytes[offset + index] != expected[index]) {
          matches = false;
          break;
        }
      }
      if (matches) {
        return true;
      }
    }
    return false;
  }

  private static String repeat(String value, int count) {
    StringBuilder result = new StringBuilder(value.length() * count);
    for (int index = 0; index < count; index++) {
      result.append(value);
    }
    return result.toString();
  }

  private static byte[] bytes(IoWriter writer) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(output);
    writer.write(out);
    return output.toByteArray();
  }

  private static void assertReason(
      LocalSnapshotException.Reason expected, ThrowingCall call) {
    LocalSnapshotException failure = assertThrows(LocalSnapshotException.class, call::run);
    assertEquals(expected, failure.reason());
    assertFalse(failure.getMessage().isEmpty());
  }

  private interface IoWriter {
    void write(DataOutputStream out) throws IOException;
  }

  private interface ThrowingCall {
    void run() throws Exception;
  }

  private static final class CountingInputStream extends InputStream {
    private int reads;

    @Override
    public int read() {
      reads++;
      return -1;
    }
  }
}
