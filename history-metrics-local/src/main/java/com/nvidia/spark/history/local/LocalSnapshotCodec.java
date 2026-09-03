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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.CRC32;

import com.nvidia.spark.history.DimValue;
import com.nvidia.spark.history.DimensionSpec;
import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.HistoryMetricsApi;
import com.nvidia.spark.history.MetricVersionId;
import com.nvidia.spark.history.MetricSchema;
import com.nvidia.spark.history.Provenance;
import com.nvidia.spark.history.Retention;

/** Deterministic, bounded, JDK-only binary codec for unpublished local snapshot state. */
final class LocalSnapshotCodec {
  static final int MAGIC = 0x484D5331;
  static final short FORMAT_MAJOR = 1;
  static final short FORMAT_MINOR = 0;
  static final int HEADER_BYTES = 20;
  static final int TRAILER_BYTES = 4;
  static final int MAX_FILE_BYTES = 256 * 1024 * 1024;
  static final int MAX_CATALOG_ENTRIES = 65_535;
  static final int MAX_DECLARATIONS = 65_535;
  static final int MAX_OBSERVATIONS = 1_000_000;
  static final int MAX_STRING_BYTES = 4_096;

  private static final int MAX_CATALOG_NAME_BYTES = 128;
  private static final int MAX_DIMENSION_NAME_BYTES = 128;
  private static final int MAX_APP_BYTES = 255;
  private static final int MAX_ATTEMPT_BYTES = 64;
  private static final int MAX_PLUGIN_VERSION_BYTES = 64;
  private static final int SECTION_COUNT = 4;
  private static final int MAX_PAYLOAD_BYTES =
      MAX_FILE_BYTES - HEADER_BYTES - TRAILER_BYTES;

  private LocalSnapshotCodec() {
  }

  static byte[] encode(LocalSnapshotState state) throws LocalSnapshotException {
    Objects.requireNonNull(state, "state");
    validateCounts(state);
    Map<MetricVersionId, LocalDeclarationSnapshot> declarations = validateState(state);

    try {
      int remaining = MAX_PAYLOAD_BYTES - SECTION_COUNT * Integer.BYTES;
      byte[] catalog = encodeCatalog(state.catalog(), remaining);
      remaining -= catalog.length;
      byte[] declarationBytes =
          encodeDeclarations(state.declarations(), remaining);
      remaining -= declarationBytes.length;
      byte[] observations =
          encodeObservations(state.observations(), declarations, remaining);
      remaining -= observations.length;
      byte[] allocator =
          encodeAllocator(state.nextAcceptanceOrdinal(), remaining);
      byte[][] sections =
          new byte[][] {catalog, declarationBytes, observations, allocator};

      long payloadSize = 0L;
      for (byte[] section : sections) {
        payloadSize += Integer.BYTES + (long) section.length;
      }
      if (payloadSize > MAX_PAYLOAD_BYTES) {
        throw failure(
            LocalSnapshotException.Reason.BOUNDS,
            "snapshot image exceeds the current file bound");
      }

      BoundedByteArrayOutputStream image =
          new BoundedByteArrayOutputStream(MAX_FILE_BYTES);
      DataOutputStream out = new DataOutputStream(image);
      out.writeInt(MAGIC);
      out.writeShort(FORMAT_MAJOR);
      out.writeShort(FORMAT_MINOR);
      out.writeInt(HistoryMetricsApi.CURRENT_API_VERSION);
      out.writeInt(0);
      out.writeInt((int) payloadSize);

      CRC32 crc = new CRC32();
      for (byte[] section : sections) {
        writeIntAndUpdate(out, crc, section.length);
        out.write(section);
        crc.update(section);
      }
      out.writeInt((int) crc.getValue());
      out.flush();
      return image.toByteArray();
    } catch (SizeLimitException exceeded) {
      throw failure(
          LocalSnapshotException.Reason.BOUNDS,
          "snapshot image exceeds the current file bound");
    } catch (IOException impossible) {
      throw failure(
          LocalSnapshotException.Reason.IO,
          "snapshot encoding failed",
          impossible);
    }
  }

  static LocalSnapshotState decode(
      byte[] image,
      HistoryMetricCatalog expectedCatalog,
      Duration maximumPlanningAge) throws LocalSnapshotException {
    Objects.requireNonNull(image, "image");
    return decode(
        new ByteArrayInputStream(image),
        image.length,
        expectedCatalog,
        maximumPlanningAge);
  }

  static LocalSnapshotState decode(
      InputStream source,
      long totalSize,
      HistoryMetricCatalog expectedCatalog,
      Duration maximumPlanningAge) throws LocalSnapshotException {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(expectedCatalog, "expectedCatalog");
    Objects.requireNonNull(maximumPlanningAge, "maximumPlanningAge");
    if (maximumPlanningAge.isNegative()) {
      throw new IllegalArgumentException("maximumPlanningAge must not be negative");
    }
    if (totalSize < 0 || totalSize > MAX_FILE_BYTES) {
      throw failure(
          LocalSnapshotException.Reason.BOUNDS,
          "snapshot file size exceeds the current bound");
    }
    if (totalSize < HEADER_BYTES + TRAILER_BYTES) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot header or trailer is truncated");
    }

    try {
      DataInputStream in = new DataInputStream(source);
      int magic = readInt(in, "snapshot header is truncated");
      short major = readShort(in, "snapshot header is truncated");
      short minor = readShort(in, "snapshot header is truncated");
      int apiVersion = readInt(in, "snapshot header is truncated");
      int flags = readInt(in, "snapshot header is truncated");
      int payloadLength = readInt(in, "snapshot header is truncated");

      if (magic != MAGIC) {
        throw failure(
            LocalSnapshotException.Reason.FORMAT,
            "snapshot magic is invalid");
      }
      if (major != FORMAT_MAJOR || minor != FORMAT_MINOR ||
          apiVersion != HistoryMetricsApi.CURRENT_API_VERSION) {
        throw failure(
            LocalSnapshotException.Reason.VERSION,
            "snapshot format or API version is incompatible");
      }
      if (flags != 0) {
        throw failure(
            LocalSnapshotException.Reason.FORMAT,
            "snapshot flags are not recognized");
      }
      if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES) {
        throw failure(
            LocalSnapshotException.Reason.BOUNDS,
            "snapshot payload length exceeds the current bound");
      }

      long expectedSize = HEADER_BYTES + (long) payloadLength + TRAILER_BYTES;
      if (expectedSize > totalSize) {
        throw failure(
            LocalSnapshotException.Reason.FORMAT,
            "snapshot payload or trailer is truncated");
      }
      if (expectedSize < totalSize) {
        throw failure(
            LocalSnapshotException.Reason.FORMAT,
            "snapshot contains trailing bytes");
      }

      byte[] payload = new byte[payloadLength];
      readFully(in, payload, "snapshot payload is truncated");
      int expectedCrc = readInt(in, "snapshot checksum is truncated");

      Section[] sections = frameSections(payload);
      preflightBounds(payload, sections);

      CRC32 crc = new CRC32();
      crc.update(payload);
      if ((int) crc.getValue() != expectedCrc) {
        throw failure(
            LocalSnapshotException.Reason.INTEGRITY,
            "snapshot checksum does not match the framed payload");
      }

      List<LocalSnapshotState.CatalogEntry> catalog =
          decodeCatalog(payload, sections[0]);
      validateCatalogMatch(catalog, expectedCatalog);
      List<LocalDeclarationSnapshot> declarations =
          decodeDeclarations(
              payload, sections[1], catalog, maximumPlanningAge);
      List<LocalObservationSnapshot> observations =
          decodeObservations(payload, sections[2], declarations);
      long nextOrdinal =
          decodeAllocator(payload, sections[3], observations);

      try {
        return LocalSnapshotState.decoded(
            catalog, declarations, observations, nextOrdinal);
      } catch (RuntimeException invalid) {
        throw failure(
            LocalSnapshotException.Reason.FORMAT,
            "snapshot state ordering is invalid",
            invalid);
      }
    } catch (LocalSnapshotException expected) {
      throw expected;
    } catch (EOFException truncated) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot framing is truncated",
          truncated);
    } catch (IOException io) {
      throw failure(
          LocalSnapshotException.Reason.IO,
          "snapshot decoding failed",
          io);
    }
  }

  private static void validateCounts(LocalSnapshotState state)
      throws LocalSnapshotException {
    if (state.catalog().size() > MAX_CATALOG_ENTRIES ||
        state.declarations().size() > MAX_DECLARATIONS ||
        state.observations().size() > MAX_OBSERVATIONS) {
      throw failure(
          LocalSnapshotException.Reason.BOUNDS,
          "snapshot section count exceeds the current bound");
    }
  }

  private static Map<MetricVersionId, LocalDeclarationSnapshot> validateState(
      LocalSnapshotState state) throws LocalSnapshotException {
    Map<Integer, LocalSnapshotState.CatalogEntry> catalog =
        new HashMap<Integer, LocalSnapshotState.CatalogEntry>();
    for (LocalSnapshotState.CatalogEntry entry : state.catalog()) {
      validateMetricId(entry.metricId(), "snapshot catalog metric ID is invalid");
      encodeString(entry.name(), MAX_CATALOG_NAME_BYTES, "catalog name");
      catalog.put(entry.metricId(), entry);
    }

    Map<MetricVersionId, LocalDeclarationSnapshot> declarations =
        new HashMap<MetricVersionId, LocalDeclarationSnapshot>();
    for (LocalDeclarationSnapshot declaration : state.declarations()) {
      MetricSchema schema = declaration.schema();
      if (!catalog.containsKey(schema.metric().metricId())) {
        throw failure(
            LocalSnapshotException.Reason.SCHEMA_CONFLICT,
            "snapshot declaration is absent from the catalog");
      }
      validateRetention(
          schema.recommendedRetention(),
          LocalSnapshotException.Reason.SCHEMA_CONFLICT,
          "schema-recommended retention is invalid");
      validateRetention(
          declaration.effectiveRetention(),
          LocalSnapshotException.Reason.POLICY_CONFLICT,
          "effective retention is invalid");
      declarations.put(schema.metric(), declaration);
    }

    long previousOrdinal = 0L;
    for (LocalObservationSnapshot observation : state.observations()) {
      LocalDeclarationSnapshot declaration = declarations.get(observation.metric());
      if (declaration == null) {
        throw failure(
            LocalSnapshotException.Reason.SCHEMA_CONFLICT,
            "snapshot observation has no declaration");
      }
      validateObservation(observation, declaration.schema());
      if (observation.acceptanceOrdinal() <= previousOrdinal) {
        throw failure(
            LocalSnapshotException.Reason.FORMAT,
            "snapshot observation ordinals are not strictly ordered");
      }
      previousOrdinal = observation.acceptanceOrdinal();
    }
    if (state.nextAcceptanceOrdinal() <= previousOrdinal) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot allocator would reuse an acceptance ordinal");
    }
    return declarations;
  }

  private static byte[] encodeCatalog(
      List<LocalSnapshotState.CatalogEntry> catalog, int maximumBytes)
      throws IOException, LocalSnapshotException {
    BoundedByteArrayOutputStream bytes =
        new BoundedByteArrayOutputStream(maximumBytes);
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeInt(catalog.size());
    for (LocalSnapshotState.CatalogEntry entry : catalog) {
      out.writeInt(entry.metricId());
      writeString(out, entry.name(), MAX_CATALOG_NAME_BYTES, "catalog name");
      out.writeByte(entry.retired() ? 1 : 0);
    }
    return bytes.toByteArray();
  }

  private static byte[] encodeDeclarations(
      List<LocalDeclarationSnapshot> declarations, int maximumBytes)
      throws IOException, LocalSnapshotException {
    BoundedByteArrayOutputStream bytes =
        new BoundedByteArrayOutputStream(maximumBytes);
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeInt(declarations.size());
    for (LocalDeclarationSnapshot declaration : declarations) {
      MetricSchema schema = declaration.schema();
      out.writeInt(schema.metric().metricId());
      out.writeInt(schema.metric().version());
      out.writeInt(schema.dimensions().size());
      for (DimensionSpec dimension : schema.dimensions()) {
        writeString(
            out, dimension.name(), MAX_DIMENSION_NAME_BYTES, "dimension name");
        out.writeByte(kindTag(dimension.kind()));
      }
      writeRetention(out, schema.recommendedRetention());
      writeRetention(out, declaration.effectiveRetention());
    }
    return bytes.toByteArray();
  }

  private static byte[] encodeObservations(
      List<LocalObservationSnapshot> observations,
      Map<MetricVersionId, LocalDeclarationSnapshot> declarations,
      int maximumBytes)
      throws IOException, LocalSnapshotException {
    BoundedByteArrayOutputStream bytes =
        new BoundedByteArrayOutputStream(maximumBytes);
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeInt(observations.size());
    for (LocalObservationSnapshot observation : observations) {
      MetricSchema schema = declarations.get(observation.metric()).schema();
      out.writeInt(observation.metric().metricId());
      out.writeInt(observation.metric().version());
      out.writeInt(schema.dimensions().size());
      for (DimensionSpec dimension : schema.dimensions()) {
        byte[] canonical =
            observation.dimensions().get(dimension.name()).canonicalBytes();
        if (canonical.length > DimValue.MAX_CANONICAL_BYTES) {
          throw failure(
              LocalSnapshotException.Reason.BOUNDS,
              "canonical dimension exceeds the current bound");
        }
        out.write(canonical);
      }
      out.writeDouble(observation.value());
      out.writeLong(observation.timestampMs());
      Provenance provenance = observation.provenance();
      writeString(out, provenance.app(), MAX_APP_BYTES, "provenance app");
      if (provenance.attempt() == null) {
        out.writeByte(0);
      } else {
        out.writeByte(1);
        writeString(
            out,
            provenance.attempt(),
            MAX_ATTEMPT_BYTES,
            "provenance attempt");
      }
      writeString(
          out,
          provenance.pluginVersion(),
          MAX_PLUGIN_VERSION_BYTES,
          "provenance plugin version");
      out.writeLong(provenance.writtenAtMs());
      out.writeLong(observation.acceptanceOrdinal());
    }
    return bytes.toByteArray();
  }

  private static byte[] encodeAllocator(long nextOrdinal, int maximumBytes) {
    if (maximumBytes < Long.BYTES) {
      throw new SizeLimitException();
    }
    ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES);
    bytes.putLong(nextOrdinal);
    return bytes.array();
  }

  private static List<LocalSnapshotState.CatalogEntry> decodeCatalog(
      byte[] payload, Section section) throws LocalSnapshotException {
    DataInputStream in = sectionInput(payload, section);
    try {
      int count = in.readInt();
      List<LocalSnapshotState.CatalogEntry> catalog =
          new ArrayList<LocalSnapshotState.CatalogEntry>(count);
      int previousId = 0;
      Set<String> names = new HashSet<String>();
      for (int index = 0; index < count; index++) {
        int id = in.readInt();
        validateMetricId(id, "snapshot catalog metric ID is invalid");
        String name =
            readString(in, MAX_CATALOG_NAME_BYTES, "catalog name");
        int state = in.readUnsignedByte();
        if (state != 0 && state != 1) {
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "snapshot catalog state is invalid");
        }
        if (name.isEmpty() || !names.add(name)) {
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "snapshot catalog name is empty or duplicated");
        }
        if (id <= previousId) {
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "snapshot catalog is not strictly metric-ID ordered");
        }
        previousId = id;
        catalog.add(new LocalSnapshotState.CatalogEntry(id, name, state == 1));
      }
      requireConsumed(in, "snapshot catalog section has trailing bytes");
      return Collections.unmodifiableList(catalog);
    } catch (LocalSnapshotException expected) {
      throw expected;
    } catch (IOException truncated) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot catalog section is truncated",
          truncated);
    }
  }

  private static void validateCatalogMatch(
      List<LocalSnapshotState.CatalogEntry> decoded,
      HistoryMetricCatalog expectedCatalog) throws LocalSnapshotException {
    List<HistoryMetricCatalog.MetricDefinition> expected =
        new ArrayList<HistoryMetricCatalog.MetricDefinition>(
            expectedCatalog.entries());
    Collections.sort(
        expected,
        new Comparator<HistoryMetricCatalog.MetricDefinition>() {
          @Override
          public int compare(
              HistoryMetricCatalog.MetricDefinition left,
              HistoryMetricCatalog.MetricDefinition right) {
            return Integer.compare(left.metricId(), right.metricId());
          }
        });
    if (decoded.size() != expected.size()) {
      throw failure(
          LocalSnapshotException.Reason.CATALOG_CONFLICT,
          "snapshot catalog does not match the supplied catalog");
    }
    for (int index = 0; index < decoded.size(); index++) {
      LocalSnapshotState.CatalogEntry actual = decoded.get(index);
      HistoryMetricCatalog.MetricDefinition wanted = expected.get(index);
      if (actual.metricId() != wanted.metricId() ||
          actual.retired() != wanted.retired() ||
          !actual.name().equals(wanted.name())) {
        throw failure(
            LocalSnapshotException.Reason.CATALOG_CONFLICT,
            "snapshot catalog does not match the supplied catalog");
      }
    }
  }

  private static List<LocalDeclarationSnapshot> decodeDeclarations(
      byte[] payload,
      Section section,
      List<LocalSnapshotState.CatalogEntry> catalog,
      Duration maximumPlanningAge) throws LocalSnapshotException {
    Map<Integer, LocalSnapshotState.CatalogEntry> catalogById =
        new HashMap<Integer, LocalSnapshotState.CatalogEntry>();
    for (LocalSnapshotState.CatalogEntry entry : catalog) {
      catalogById.put(entry.metricId(), entry);
    }

    DataInputStream in = sectionInput(payload, section);
    try {
      int count = in.readInt();
      List<LocalDeclarationSnapshot> declarations =
          new ArrayList<LocalDeclarationSnapshot>(count);
      LocalDeclarationSnapshot previous = null;
      for (int index = 0; index < count; index++) {
        MetricVersionId metric = readMetric(in);
        int dimensionCount = in.readInt();
        List<DimensionSpec> dimensions =
            new ArrayList<DimensionSpec>(dimensionCount);
        for (int slot = 0; slot < dimensionCount; slot++) {
          String name =
              readString(in, MAX_DIMENSION_NAME_BYTES, "dimension name");
          DimValue.Kind kind = readKind(in);
          try {
            dimensions.add(new DimensionSpec(name, kind));
          } catch (RuntimeException invalid) {
            throw failure(
                LocalSnapshotException.Reason.FORMAT,
                "snapshot dimension declaration is invalid",
                invalid);
          }
        }

        Retention recommended = readRetention(
            in,
            LocalSnapshotException.Reason.SCHEMA_CONFLICT,
            "schema-recommended retention is invalid");
        Retention effective = readRetention(
            in,
            LocalSnapshotException.Reason.POLICY_CONFLICT,
            "effective retention is invalid");
        if (effective.planningMaxAge().compareTo(maximumPlanningAge) > 0) {
          throw failure(
              LocalSnapshotException.Reason.POLICY_CONFLICT,
              "effective planning age exceeds the supplied provider envelope");
        }
        if (!catalogById.containsKey(metric.metricId())) {
          throw failure(
              LocalSnapshotException.Reason.SCHEMA_CONFLICT,
              "snapshot declaration is absent from the catalog");
        }

        MetricSchema schema;
        try {
          schema = new MetricSchema(metric, dimensions, recommended);
        } catch (RuntimeException invalid) {
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "snapshot schema structure is invalid",
              invalid);
        }
        LocalDeclarationSnapshot current =
            new LocalDeclarationSnapshot(schema, effective);
        if (previous != null) {
          long previousKey = previous.schema().metric().packedKey();
          long currentKey = metric.packedKey();
          if (currentKey < previousKey) {
            throw failure(
                LocalSnapshotException.Reason.FORMAT,
                "snapshot declarations are not metric-key ordered");
          }
          if (currentKey == previousKey) {
            classifyDuplicateDeclaration(previous, current);
          }
        }
        declarations.add(current);
        previous = current;
      }
      requireConsumed(in, "snapshot declaration section has trailing bytes");
      return Collections.unmodifiableList(declarations);
    } catch (LocalSnapshotException expected) {
      throw expected;
    } catch (IOException truncated) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot declaration section is truncated",
          truncated);
    }
  }

  private static void classifyDuplicateDeclaration(
      LocalDeclarationSnapshot first,
      LocalDeclarationSnapshot second) throws LocalSnapshotException {
    boolean canonicalSame = first.schema().equals(second.schema());
    boolean recommendedSame = first.schema().recommendedRetention().equals(
        second.schema().recommendedRetention());
    boolean policySame = first.effectiveRetention().equals(
        second.effectiveRetention());
    if (!canonicalSame || !recommendedSame) {
      throw failure(
          LocalSnapshotException.Reason.SCHEMA_CONFLICT,
          "duplicate snapshot declarations contradict schema");
    }
    if (!policySame) {
      throw failure(
          LocalSnapshotException.Reason.POLICY_CONFLICT,
          "duplicate snapshot declarations contradict effective policy");
    }
    throw failure(
        LocalSnapshotException.Reason.FORMAT,
        "snapshot contains an identical duplicate declaration");
  }

  private static List<LocalObservationSnapshot> decodeObservations(
      byte[] payload,
      Section section,
      List<LocalDeclarationSnapshot> declarations)
      throws LocalSnapshotException {
    Map<MetricVersionId, MetricSchema> schemas = new HashMap<MetricVersionId, MetricSchema>();
    for (LocalDeclarationSnapshot declaration : declarations) {
      schemas.put(declaration.schema().metric(), declaration.schema());
    }

    DataInputStream in = sectionInput(payload, section);
    try {
      int count = in.readInt();
      List<LocalObservationSnapshot> observations =
          new ArrayList<LocalObservationSnapshot>(count);
      long previousOrdinal = 0L;
      for (int index = 0; index < count; index++) {
        MetricVersionId metric = readMetric(in);
        MetricSchema schema = schemas.get(metric);
        int dimensionCount = in.readInt();
        if (schema == null || dimensionCount != schema.dimensions().size()) {
          throw failure(
              LocalSnapshotException.Reason.SCHEMA_CONFLICT,
              "snapshot observation does not match a declaration");
        }

        Map<String, DimValue> dimensions =
            new LinkedHashMap<String, DimValue>(dimensionCount);
        for (int slot = 0; slot < dimensionCount; slot++) {
          DimValue value = readDimValue(in);
          DimensionSpec dimension = schema.dimensions().get(slot);
          if (value.kind() != dimension.kind()) {
            throw failure(
                LocalSnapshotException.Reason.SCHEMA_CONFLICT,
                "snapshot observation dimension kind contradicts its declaration");
          }
          dimensions.put(dimension.name(), value);
        }

        double value = in.readDouble();
        if (!Double.isFinite(value)) {
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "snapshot observation value is not finite");
        }
        long timestampMs = in.readLong();
        String app = readString(in, MAX_APP_BYTES, "provenance app");
        int attemptPresent = in.readUnsignedByte();
        String attempt;
        if (attemptPresent == 0) {
          attempt = null;
        } else if (attemptPresent == 1) {
          attempt =
              readString(in, MAX_ATTEMPT_BYTES, "provenance attempt");
        } else {
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "snapshot provenance null marker is invalid");
        }
        String pluginVersion =
            readString(
                in,
                MAX_PLUGIN_VERSION_BYTES,
                "provenance plugin version");
        long writtenAtMs = in.readLong();
        long ordinal = in.readLong();
        if (ordinal <= previousOrdinal) {
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "snapshot observation ordinals are not strictly ordered");
        }

        try {
          observations.add(new LocalObservationSnapshot(
              metric,
              dimensions,
              value,
              timestampMs,
              new Provenance(app, attempt, pluginVersion, writtenAtMs),
              ordinal));
        } catch (RuntimeException invalid) {
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "snapshot observation or provenance is invalid",
              invalid);
        }
        previousOrdinal = ordinal;
      }
      requireConsumed(in, "snapshot observation section has trailing bytes");
      return Collections.unmodifiableList(observations);
    } catch (LocalSnapshotException expected) {
      throw expected;
    } catch (IOException truncated) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot observation section is truncated",
          truncated);
    }
  }

  private static long decodeAllocator(
      byte[] payload,
      Section section,
      List<LocalObservationSnapshot> observations)
      throws LocalSnapshotException {
    DataInputStream in = sectionInput(payload, section);
    try {
      long next = in.readLong();
      requireConsumed(in, "snapshot allocator section has trailing bytes");
      long previous = observations.isEmpty()
          ? 0L
          : observations.get(observations.size() - 1).acceptanceOrdinal();
      if (next <= previous || next <= 0L) {
        throw failure(
            LocalSnapshotException.Reason.FORMAT,
            "snapshot allocator would reuse an acceptance ordinal");
      }
      return next;
    } catch (LocalSnapshotException expected) {
      throw expected;
    } catch (IOException truncated) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot allocator section is truncated",
          truncated);
    }
  }

  private static Section[] frameSections(byte[] payload)
      throws LocalSnapshotException {
    Cursor cursor = new Cursor(payload, 0, payload.length);
    Section[] sections = new Section[SECTION_COUNT];
    for (int index = 0; index < SECTION_COUNT; index++) {
      int length = cursor.readInt("snapshot section length is truncated");
      if (length < 0 || length > cursor.remaining()) {
        throw failure(
            LocalSnapshotException.Reason.BOUNDS,
            "snapshot section length exceeds its remaining payload");
      }
      sections[index] = new Section(cursor.position(), length);
      cursor.skipFixed(length, "snapshot section is truncated");
    }
    if (cursor.remaining() != 0) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot payload contains trailing section data");
    }
    return sections;
  }

  private static void preflightBounds(byte[] payload, Section[] sections)
      throws LocalSnapshotException {
    preflightCatalog(payload, sections[0]);
    preflightDeclarations(payload, sections[1]);
    preflightObservations(payload, sections[2]);
    Cursor allocator =
        new Cursor(payload, sections[3].offset, sections[3].end());
    allocator.skipFixed(Long.BYTES, "snapshot allocator section is truncated");
    allocator.requireEnd("snapshot allocator section has trailing bytes");
  }

  private static void preflightCatalog(byte[] payload, Section section)
      throws LocalSnapshotException {
    Cursor cursor = new Cursor(payload, section.offset, section.end());
    int count = cursor.readCount(
        MAX_CATALOG_ENTRIES, "snapshot catalog count exceeds the current bound");
    for (int index = 0; index < count; index++) {
      cursor.skipFixed(Integer.BYTES, "snapshot catalog metric ID is truncated");
      cursor.skipLengthFramed(
          MAX_CATALOG_NAME_BYTES,
          "snapshot catalog name exceeds its remaining bound");
      cursor.skipFixed(1, "snapshot catalog state is truncated");
    }
    cursor.requireEnd("snapshot catalog section has trailing bytes");
  }

  private static void preflightDeclarations(byte[] payload, Section section)
      throws LocalSnapshotException {
    Cursor cursor = new Cursor(payload, section.offset, section.end());
    int count = cursor.readCount(
        MAX_DECLARATIONS,
        "snapshot declaration count exceeds the current bound");
    for (int index = 0; index < count; index++) {
      cursor.skipFixed(2 * Integer.BYTES, "snapshot declaration key is truncated");
      int dimensions = cursor.readCount(
          MetricSchema.MAX_DIMENSIONS,
          "snapshot dimension count exceeds the current bound");
      for (int slot = 0; slot < dimensions; slot++) {
        cursor.skipLengthFramed(
            MAX_DIMENSION_NAME_BYTES,
            "snapshot dimension name exceeds its remaining bound");
        cursor.skipFixed(1, "snapshot dimension kind is truncated");
      }
      cursor.skipFixed(
          4 * (Long.BYTES + Integer.BYTES),
          "snapshot declaration retention is truncated");
    }
    cursor.requireEnd("snapshot declaration section has trailing bytes");
  }

  private static void preflightObservations(byte[] payload, Section section)
      throws LocalSnapshotException {
    Cursor cursor = new Cursor(payload, section.offset, section.end());
    int count = cursor.readCount(
        MAX_OBSERVATIONS,
        "snapshot observation count exceeds the current bound");
    for (int index = 0; index < count; index++) {
      cursor.skipFixed(2 * Integer.BYTES, "snapshot observation key is truncated");
      int dimensions = cursor.readCount(
          MetricSchema.MAX_DIMENSIONS,
          "snapshot observation dimension count exceeds the current bound");
      for (int slot = 0; slot < dimensions; slot++) {
        cursor.skipFixed(1, "snapshot dimension kind is truncated");
        int payloadLength =
            cursor.readUnsignedShort("snapshot dimension length is truncated");
        if (payloadLength > DimValue.MAX_CANONICAL_BYTES - 3) {
          throw failure(
              LocalSnapshotException.Reason.BOUNDS,
              "canonical dimension exceeds the current bound");
        }
        cursor.skipBounded(
            payloadLength,
            "snapshot dimension length exceeds its remaining section");
      }
      cursor.skipFixed(
          Double.BYTES + Long.BYTES,
          "snapshot observation value or time is truncated");
      cursor.skipLengthFramed(
          MAX_APP_BYTES,
          "snapshot provenance app exceeds its remaining bound");
      int attemptPresent =
          cursor.readUnsignedByte("snapshot provenance null marker is truncated");
      if (attemptPresent == 1) {
        cursor.skipLengthFramed(
            MAX_ATTEMPT_BYTES,
            "snapshot provenance attempt exceeds its remaining bound");
      }
      cursor.skipLengthFramed(
          MAX_PLUGIN_VERSION_BYTES,
          "snapshot provenance plugin version exceeds its remaining bound");
      cursor.skipFixed(
          2 * Long.BYTES,
          "snapshot provenance time or ordinal is truncated");
    }
    cursor.requireEnd("snapshot observation section has trailing bytes");
  }

  private static MetricVersionId readMetric(DataInputStream in)
      throws IOException, LocalSnapshotException {
    int id = in.readInt();
    int version = in.readInt();
    try {
      return new MetricVersionId(id, version);
    } catch (RuntimeException invalid) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot metric identity is invalid",
          invalid);
    }
  }

  private static DimValue readDimValue(DataInputStream in)
      throws IOException, LocalSnapshotException {
    DimValue.Kind kind = readKind(in);
    int length = in.readUnsignedShort();
    if (length > DimValue.MAX_CANONICAL_BYTES - 3) {
      throw failure(
          LocalSnapshotException.Reason.BOUNDS,
          "canonical dimension exceeds the current bound");
    }
    if (length > in.available()) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot dimension payload is physically truncated");
    }
    byte[] value = new byte[length];
    in.readFully(value);
    try {
      switch (kind) {
        case STRING:
          return DimValue.of(decodeUtf8(value, "string dimension value"));
        case LONG:
          if (length != Long.BYTES) {
            throw failure(
                LocalSnapshotException.Reason.FORMAT,
                "snapshot long dimension has an invalid length");
          }
          return DimValue.of(ByteBuffer.wrap(value).getLong());
        case BYTES:
          return DimValue.of(value);
        default:
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "snapshot dimension kind is invalid");
      }
    } catch (LocalSnapshotException expected) {
      throw expected;
    } catch (RuntimeException invalid) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot dimension value is invalid",
          invalid);
    }
  }

  private static DimValue.Kind readKind(DataInputStream in)
      throws IOException, LocalSnapshotException {
    int tag = in.readUnsignedByte();
    switch (tag) {
      case 1:
        return DimValue.Kind.STRING;
      case 2:
        return DimValue.Kind.LONG;
      case 3:
        return DimValue.Kind.BYTES;
      default:
        throw failure(
            LocalSnapshotException.Reason.FORMAT,
            "snapshot dimension kind is invalid");
    }
  }

  private static int kindTag(DimValue.Kind kind) {
    switch (kind) {
      case STRING:
        return 1;
      case LONG:
        return 2;
      case BYTES:
        return 3;
      default:
        throw new IllegalArgumentException("unsupported dimension kind");
    }
  }

  private static Retention readRetention(
      DataInputStream in,
      LocalSnapshotException.Reason reason,
      String diagnostic) throws IOException, LocalSnapshotException {
    Duration planning = readDuration(in);
    Duration storage = readDuration(in);
    try {
      return new Retention(planning, storage);
    } catch (RuntimeException invalid) {
      throw failure(reason, diagnostic, invalid);
    }
  }

  private static Duration readDuration(DataInputStream in)
      throws IOException, LocalSnapshotException {
    long seconds = in.readLong();
    int nanos = in.readInt();
    if (nanos < 0 || nanos >= 1_000_000_000) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot duration nanoseconds are outside the canonical range");
    }
    return Duration.ofSeconds(seconds, nanos);
  }

  private static void writeRetention(DataOutputStream out, Retention retention)
      throws IOException {
    writeDuration(out, retention.planningMaxAge());
    writeDuration(out, retention.storageRetention());
  }

  private static void writeDuration(DataOutputStream out, Duration duration)
      throws IOException {
    out.writeLong(duration.getSeconds());
    out.writeInt(duration.getNano());
  }

  private static void validateRetention(
      Retention retention,
      LocalSnapshotException.Reason reason,
      String diagnostic) throws LocalSnapshotException {
    if (retention.planningMaxAge().isNegative() ||
        retention.storageRetention().isNegative() ||
        retention.storageRetention().compareTo(
            retention.planningMaxAge()) < 0) {
      throw failure(reason, diagnostic);
    }
  }

  private static void validateObservation(
      LocalObservationSnapshot observation,
      MetricSchema schema) throws LocalSnapshotException {
    if (!Double.isFinite(observation.value()) ||
        observation.dimensions().size() != schema.dimensions().size()) {
      throw failure(
          LocalSnapshotException.Reason.SCHEMA_CONFLICT,
          "snapshot observation does not match its declaration");
    }
    for (DimensionSpec dimension : schema.dimensions()) {
      DimValue value = observation.dimensions().get(dimension.name());
      if (value == null || value.kind() != dimension.kind()) {
        throw failure(
            LocalSnapshotException.Reason.SCHEMA_CONFLICT,
            "snapshot observation does not match its declaration");
      }
      if (value.canonicalBytes().length > DimValue.MAX_CANONICAL_BYTES) {
        throw failure(
            LocalSnapshotException.Reason.BOUNDS,
            "canonical dimension exceeds the current bound");
      }
    }
    Provenance provenance = observation.provenance();
    encodeString(provenance.app(), MAX_APP_BYTES, "provenance app");
    if (provenance.attempt() != null) {
      encodeString(
          provenance.attempt(), MAX_ATTEMPT_BYTES, "provenance attempt");
    }
    encodeString(
        provenance.pluginVersion(),
        MAX_PLUGIN_VERSION_BYTES,
        "provenance plugin version");
  }

  private static void validateMetricId(int metricId, String diagnostic)
      throws LocalSnapshotException {
    if (metricId <= 0 || metricId > 0xFFFF) {
      throw failure(LocalSnapshotException.Reason.FORMAT, diagnostic);
    }
  }

  private static void writeString(
      DataOutputStream out, String value, int maximum, String field)
      throws IOException, LocalSnapshotException {
    byte[] encoded = encodeString(value, maximum, field);
    out.writeInt(encoded.length);
    out.write(encoded);
  }

  private static byte[] encodeString(String value, int maximum, String field)
      throws LocalSnapshotException {
    Objects.requireNonNull(value, field);
    byte[] encoded;
    try {
      ByteBuffer bytes = StandardCharsets.UTF_8.newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .encode(CharBuffer.wrap(value));
      encoded = new byte[bytes.remaining()];
      bytes.get(encoded);
    } catch (CharacterCodingException malformed) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot " + field + " is not strict UTF-8",
          malformed);
    }
    int bound = Math.min(MAX_STRING_BYTES, maximum);
    if (encoded.length > bound) {
      throw failure(
          LocalSnapshotException.Reason.BOUNDS,
          "snapshot " + field + " exceeds its current bound");
    }
    return encoded;
  }

  private static String readString(
      DataInputStream in, int maximum, String field)
      throws IOException, LocalSnapshotException {
    int length = in.readInt();
    int bound = Math.min(MAX_STRING_BYTES, maximum);
    if (length < 0 || length > bound) {
      throw failure(
          LocalSnapshotException.Reason.BOUNDS,
          "snapshot " + field + " length exceeds its current bound");
    }
    if (length > in.available()) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot " + field + " bytes are physically truncated");
    }
    byte[] encoded = new byte[length];
    in.readFully(encoded);
    return decodeUtf8(encoded, field);
  }

  private static String decodeUtf8(byte[] encoded, String field)
      throws LocalSnapshotException {
    try {
      return StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(encoded))
          .toString();
    } catch (CharacterCodingException malformed) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT,
          "snapshot " + field + " is not strict UTF-8",
          malformed);
    }
  }

  private static void writeIntAndUpdate(
      DataOutputStream out, CRC32 crc, int value) throws IOException {
    out.writeInt(value);
    crc.update((value >>> 24) & 0xFF);
    crc.update((value >>> 16) & 0xFF);
    crc.update((value >>> 8) & 0xFF);
    crc.update(value & 0xFF);
  }

  private static DataInputStream sectionInput(byte[] payload, Section section) {
    return new DataInputStream(
        new ByteArrayInputStream(payload, section.offset, section.length));
  }

  private static void requireConsumed(DataInputStream in, String diagnostic)
      throws IOException, LocalSnapshotException {
    if (in.available() != 0) {
      throw failure(LocalSnapshotException.Reason.FORMAT, diagnostic);
    }
  }

  private static int readInt(DataInputStream in, String diagnostic)
      throws IOException, LocalSnapshotException {
    try {
      return in.readInt();
    } catch (EOFException truncated) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT, diagnostic, truncated);
    }
  }

  private static short readShort(DataInputStream in, String diagnostic)
      throws IOException, LocalSnapshotException {
    try {
      return in.readShort();
    } catch (EOFException truncated) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT, diagnostic, truncated);
    }
  }

  private static void readFully(
      DataInputStream in, byte[] target, String diagnostic)
      throws IOException, LocalSnapshotException {
    try {
      in.readFully(target);
    } catch (EOFException truncated) {
      throw failure(
          LocalSnapshotException.Reason.FORMAT, diagnostic, truncated);
    }
  }

  private static LocalSnapshotException failure(
      LocalSnapshotException.Reason reason, String message) {
    return new LocalSnapshotException(reason, message);
  }

  private static LocalSnapshotException failure(
      LocalSnapshotException.Reason reason, String message, Throwable cause) {
    return new LocalSnapshotException(reason, message, cause);
  }

  private static final class Section {
    private final int offset;
    private final int length;

    private Section(int offset, int length) {
      this.offset = offset;
      this.length = length;
    }

    private int end() {
      return offset + length;
    }
  }

  private static final class Cursor {
    private final byte[] bytes;
    private final int end;
    private int position;

    private Cursor(byte[] bytes, int offset, int end) {
      this.bytes = bytes;
      this.position = offset;
      this.end = end;
    }

    private int position() {
      return position;
    }

    private int remaining() {
      return end - position;
    }

    private int readCount(int maximum, String diagnostic)
        throws LocalSnapshotException {
      int count = readInt("snapshot count is truncated");
      if (count < 0 || count > maximum) {
        throw failure(LocalSnapshotException.Reason.BOUNDS, diagnostic);
      }
      return count;
    }

    private int readInt(String diagnostic) throws LocalSnapshotException {
      requireFixed(Integer.BYTES, diagnostic);
      int value = ((bytes[position] & 0xFF) << 24) |
          ((bytes[position + 1] & 0xFF) << 16) |
          ((bytes[position + 2] & 0xFF) << 8) |
          (bytes[position + 3] & 0xFF);
      position += Integer.BYTES;
      return value;
    }

    private int readUnsignedShort(String diagnostic)
        throws LocalSnapshotException {
      requireFixed(Short.BYTES, diagnostic);
      int value = ((bytes[position] & 0xFF) << 8) |
          (bytes[position + 1] & 0xFF);
      position += Short.BYTES;
      return value;
    }

    private int readUnsignedByte(String diagnostic)
        throws LocalSnapshotException {
      requireFixed(1, diagnostic);
      return bytes[position++] & 0xFF;
    }

    private void skipLengthFramed(int maximum, String diagnostic)
        throws LocalSnapshotException {
      int length = readInt("snapshot string length is truncated");
      if (length < 0 || length > Math.min(MAX_STRING_BYTES, maximum)) {
        throw failure(LocalSnapshotException.Reason.BOUNDS, diagnostic);
      }
      if (length > remaining()) {
        throw failure(
            LocalSnapshotException.Reason.FORMAT,
            "snapshot string bytes are physically truncated");
      }
      position += length;
    }

    private void skipBounded(int length, String diagnostic)
        throws LocalSnapshotException {
      if (length < 0) {
        throw failure(LocalSnapshotException.Reason.BOUNDS, diagnostic);
      }
      if (length > remaining()) {
        throw failure(
            LocalSnapshotException.Reason.FORMAT,
            "snapshot component bytes are physically truncated");
      }
      position += length;
    }

    private void skipFixed(int length, String diagnostic)
        throws LocalSnapshotException {
      requireFixed(length, diagnostic);
      position += length;
    }

    private void requireFixed(int length, String diagnostic)
        throws LocalSnapshotException {
      if (length < 0 || length > remaining()) {
        throw failure(LocalSnapshotException.Reason.FORMAT, diagnostic);
      }
    }

    private void requireEnd(String diagnostic)
        throws LocalSnapshotException {
      if (remaining() != 0) {
        throw failure(LocalSnapshotException.Reason.FORMAT, diagnostic);
      }
    }
  }

  private static final class BoundedByteArrayOutputStream
      extends ByteArrayOutputStream {
    private final int maximum;

    private BoundedByteArrayOutputStream(int maximum) {
      this.maximum = maximum;
    }

    @Override
    public synchronized void write(int value) {
      ensureCapacityFor(1);
      super.write(value);
    }

    @Override
    public synchronized void write(byte[] value, int offset, int length) {
      ensureCapacityFor(length);
      super.write(value, offset, length);
    }

    private void ensureCapacityFor(int length) {
      if (length < 0 || (long) count + length > maximum) {
        throw new SizeLimitException();
      }
    }
  }

  private static final class SizeLimitException extends RuntimeException {
  }
}
