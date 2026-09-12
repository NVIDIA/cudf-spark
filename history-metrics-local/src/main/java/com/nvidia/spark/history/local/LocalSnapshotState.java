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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.nvidia.spark.history.HistoryMetricCatalog;

/** Immutable durable state captured by the deterministic local snapshot codec. */
final class LocalSnapshotState {
  private static final Comparator<CatalogEntry> CATALOG_ORDER =
      new Comparator<CatalogEntry>() {
        @Override
        public int compare(CatalogEntry left, CatalogEntry right) {
          return Integer.compare(left.metricId(), right.metricId());
        }
      };
  private static final Comparator<LocalDeclarationSnapshot> DECLARATION_ORDER =
      new Comparator<LocalDeclarationSnapshot>() {
        @Override
        public int compare(
            LocalDeclarationSnapshot left, LocalDeclarationSnapshot right) {
          return Long.compare(
              left.schema().metric().packedKey(), right.schema().metric().packedKey());
        }
      };
  private static final Comparator<LocalObservationSnapshot> OBSERVATION_ORDER =
      new Comparator<LocalObservationSnapshot>() {
        @Override
        public int compare(
            LocalObservationSnapshot left, LocalObservationSnapshot right) {
          return Long.compare(left.acceptanceOrdinal(), right.acceptanceOrdinal());
        }
      };

  private final List<CatalogEntry> catalog;
  private final List<LocalDeclarationSnapshot> declarations;
  private final List<LocalObservationSnapshot> observations;
  private final long nextAcceptanceOrdinal;

  private LocalSnapshotState(
      List<CatalogEntry> catalog,
      List<LocalDeclarationSnapshot> declarations,
      List<LocalObservationSnapshot> observations,
      long nextAcceptanceOrdinal) {
    this.catalog = immutableSortedCopy(catalog, CATALOG_ORDER, "catalog");
    this.declarations =
        immutableSortedCopy(declarations, DECLARATION_ORDER, "declarations");
    this.observations =
        immutableSortedCopy(observations, OBSERVATION_ORDER, "observations");
    validateUniqueState(this.catalog, this.declarations, this.observations);
    if (nextAcceptanceOrdinal <= 0) {
      throw new IllegalArgumentException("next acceptance ordinal must be positive");
    }
    if (!this.observations.isEmpty() &&
        nextAcceptanceOrdinal <=
            this.observations.get(this.observations.size() - 1).acceptanceOrdinal()) {
      throw new IllegalArgumentException(
          "next acceptance ordinal must follow every stored observation");
    }
    this.nextAcceptanceOrdinal = nextAcceptanceOrdinal;
  }

  static LocalSnapshotState capture(
      HistoryMetricCatalog catalog,
      List<LocalDeclarationSnapshot> declarations,
      List<LocalObservationSnapshot> observations,
      long nextAcceptanceOrdinal) {
    Objects.requireNonNull(catalog, "catalog");
    List<CatalogEntry> entries =
        new ArrayList<CatalogEntry>(catalog.entries().size());
    for (HistoryMetricCatalog.MetricDefinition definition : catalog.entries()) {
      entries.add(new CatalogEntry(
          definition.metricId(), definition.name(), definition.retired()));
    }
    return new LocalSnapshotState(
        entries, declarations, observations, nextAcceptanceOrdinal);
  }

  static LocalSnapshotState decoded(
      List<CatalogEntry> catalog,
      List<LocalDeclarationSnapshot> declarations,
      List<LocalObservationSnapshot> observations,
      long nextAcceptanceOrdinal) {
    return new LocalSnapshotState(
        catalog, declarations, observations, nextAcceptanceOrdinal);
  }

  List<CatalogEntry> catalog() {
    return catalog;
  }

  List<LocalDeclarationSnapshot> declarations() {
    return declarations;
  }

  List<LocalObservationSnapshot> observations() {
    return observations;
  }

  long nextAcceptanceOrdinal() {
    return nextAcceptanceOrdinal;
  }

  private static void validateUniqueState(
      List<CatalogEntry> catalog,
      List<LocalDeclarationSnapshot> declarations,
      List<LocalObservationSnapshot> observations) {
    for (int index = 1; index < catalog.size(); index++) {
      if (catalog.get(index - 1).metricId() == catalog.get(index).metricId()) {
        throw new IllegalArgumentException("snapshot catalog contains a duplicate metric ID");
      }
    }
    for (int index = 1; index < declarations.size(); index++) {
      if (declarations.get(index - 1).schema().metric().equals(
          declarations.get(index).schema().metric())) {
        throw new IllegalArgumentException(
            "snapshot declarations contain a duplicate metric version");
      }
    }
    long previousOrdinal = 0L;
    for (LocalObservationSnapshot observation : observations) {
      if (observation.acceptanceOrdinal() <= previousOrdinal) {
        throw new IllegalArgumentException(
            "snapshot observations require unique positive ordinals");
      }
      previousOrdinal = observation.acceptanceOrdinal();
    }
  }

  private static <T> List<T> immutableSortedCopy(
      List<T> values, Comparator<T> comparator, String name) {
    Objects.requireNonNull(values, name);
    List<T> copied = new ArrayList<T>(values.size());
    for (T value : values) {
      copied.add(Objects.requireNonNull(value, name + " element"));
    }
    Collections.sort(copied, comparator);
    return Collections.unmodifiableList(copied);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LocalSnapshotState)) {
      return false;
    }
    LocalSnapshotState that = (LocalSnapshotState) other;
    return nextAcceptanceOrdinal == that.nextAcceptanceOrdinal &&
        catalog.equals(that.catalog) &&
        declarationsEqual(declarations, that.declarations) &&
        observations.equals(that.observations);
  }

  @Override
  public int hashCode() {
    int result = catalog.hashCode();
    for (LocalDeclarationSnapshot declaration : declarations) {
      result = 31 * result + declaration.schema().hashCode();
      result = 31 * result +
          declaration.schema().recommendedRetention().hashCode();
      result = 31 * result + declaration.effectiveRetention().hashCode();
    }
    result = 31 * result + observations.hashCode();
    return 31 * result +
        (int) (nextAcceptanceOrdinal ^ (nextAcceptanceOrdinal >>> 32));
  }

  private static boolean declarationsEqual(
      List<LocalDeclarationSnapshot> left,
      List<LocalDeclarationSnapshot> right) {
    if (left.size() != right.size()) {
      return false;
    }
    for (int index = 0; index < left.size(); index++) {
      LocalDeclarationSnapshot leftDeclaration = left.get(index);
      LocalDeclarationSnapshot rightDeclaration = right.get(index);
      if (!leftDeclaration.schema().equals(rightDeclaration.schema()) ||
          !leftDeclaration.schema().recommendedRetention().equals(
              rightDeclaration.schema().recommendedRetention()) ||
          !leftDeclaration.effectiveRetention().equals(
              rightDeclaration.effectiveRetention())) {
        return false;
      }
    }
    return true;
  }

  /** One exact catalog association carried in the snapshot. */
  static final class CatalogEntry {
    private final int metricId;
    private final String name;
    private final boolean retired;

    CatalogEntry(int metricId, String name, boolean retired) {
      this.metricId = metricId;
      this.name = Objects.requireNonNull(name, "name");
      this.retired = retired;
    }

    int metricId() {
      return metricId;
    }

    String name() {
      return name;
    }

    boolean retired() {
      return retired;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof CatalogEntry)) {
        return false;
      }
      CatalogEntry that = (CatalogEntry) other;
      return metricId == that.metricId &&
          retired == that.retired &&
          name.equals(that.name);
    }

    @Override
    public int hashCode() {
      int result = 31 * metricId + name.hashCode();
      return 31 * result + (retired ? 1 : 0);
    }
  }
}
