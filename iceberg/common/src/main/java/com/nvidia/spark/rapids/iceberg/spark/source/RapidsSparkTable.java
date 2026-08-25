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

package com.nvidia.spark.rapids.iceberg.spark.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkTable;

import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsDeleteV2;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import scala.Option;

/**
 * Wraps an iceberg {@link SparkTable} so that scan options can be augmented from
 * Spark session configuration entries. Session-conf overrides are recognized at
 * three scopes, with the following precedence (most specific wins):
 * <ol>
 *   <li>session-table: {@code spark.rapids.iceberg.table-setting.<catalog>.<namespace>.<table>.<suffix>}</li>
 *   <li>catalog:       {@code spark.rapids.iceberg.catalog-setting.<catalog>.<suffix>}</li>
 *   <li>global:        {@code spark.rapids.iceberg.global-setting.<suffix>}</li>
 * </ol>
 * When no session conf is set at any scope, iceberg falls back to the table's
 * own {@code TBLPROPERTIES} (e.g. {@code read.split.target-size}) and then to
 * iceberg's built-in defaults.
 *
 * <p>Supported suffixes (mapped to the corresponding Iceberg read option):
 * <ul>
 *   <li>{@code read-split-target-size}       -&gt; {@link SparkReadOptions#SPLIT_SIZE}</li>
 *   <li>{@code read-split-planning-lookback} -&gt; {@link SparkReadOptions#LOOKBACK}</li>
 *   <li>{@code read-split-open-file-cost}    -&gt; {@link SparkReadOptions#FILE_OPEN_COST}</li>
 * </ul>
 *
 * <p>Suffixes mirror iceberg's TableProperties keys ({@code read.split.target-size}, etc.)
 * with {@code -} instead of {@code .} so the dot-separated session-conf path stays
 * unambiguous.
 *
 * <p>Explicit DataFrame read options take precedence over all session-level overrides.
 */
public class RapidsSparkTable implements Table,
    SupportsRead,
    SupportsWrite,
    SupportsDeleteV2,
    SupportsRowLevelOperations,
    SupportsMetadataColumns {

  /**
   * Root prefix for all session-level scan option overrides. The scope of an
   * individual conf is encoded in what immediately follows this prefix — see
   * the class javadoc for the table-setting / catalog-setting / global-setting
   * key shapes.
   */
  public static final String CONF_PREFIX = "spark.rapids.iceberg.";

  /** Scope marker for {@code spark.rapids.iceberg.table-setting.<…>.<suffix>}. */
  static final String TABLE_SETTING_PREFIX = CONF_PREFIX + "table-setting.";

  /** Scope marker for {@code spark.rapids.iceberg.catalog-setting.<catalog>.<suffix>}. */
  static final String CATALOG_SETTING_PREFIX = CONF_PREFIX + "catalog-setting.";

  /** Scope marker for {@code spark.rapids.iceberg.global-setting.<suffix>}. */
  static final String GLOBAL_SETTING_PREFIX = CONF_PREFIX + "global-setting.";

  /**
   * Mapping from session-conf suffix to the iceberg read option key. Suffix names
   * mirror iceberg's TableProperties keys ({@code read.split.target-size},
   * {@code read.split.planning-lookback}, {@code read.split.open-file-cost}) with
   * {@code -} instead of {@code .} so the dot-separated session-conf key remains
   * unambiguous.
   */
  private static final Map<String, String> SUFFIX_TO_READ_OPTION;
  static {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("read-split-target-size", SparkReadOptions.SPLIT_SIZE);
    m.put("read-split-planning-lookback", SparkReadOptions.LOOKBACK);
    m.put("read-split-open-file-cost", SparkReadOptions.FILE_OPEN_COST);
    SUFFIX_TO_READ_OPTION = m;
  }

  private final SparkTable delegate;
  private final String catalogName;
  // Iceberg's Table.name() returns "<icebergCatalog>.<namespaceParts>.<table>",
  // so namespace and table are recovered from the delegate by dropping the
  // leading iceberg-catalog component. Computed eagerly so concurrent
  // newScanBuilder calls on a shared (catalog-cached) instance see a fully
  // initialized identifier under the JVM memory model.
  private final String[] namespace;
  private final String tableName;

  public RapidsSparkTable(SparkTable delegate, String catalogName) {
    this.delegate = delegate;
    this.catalogName = catalogName;
    String name = delegate.table().name();
    String[] parts = name.split("\\.");
    if (parts.length < 2) {
      throw new IllegalStateException(
          "Cannot derive namespace and table from iceberg table name '" + name +
          "': expected '<catalog>.<namespace>.<table>'.");
    }
    this.namespace = Arrays.copyOfRange(parts, 1, parts.length - 1);
    this.tableName = parts[parts.length - 1];
  }

  public SparkTable delegate() {
    return delegate;
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return delegate.schema();
  }

  @Override
  public Transform[] partitioning() {
    return delegate.partitioning();
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return delegate.capabilities();
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    return delegate.metadataColumns();
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    CaseInsensitiveStringMap merged =
        mergeSessionOptions(options, catalogName, namespace, tableName);
    return delegate.newScanBuilder(merged);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return delegate.newWriteBuilder(info);
  }

  @Override
  public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
    return delegate.newRowLevelOperationBuilder(info);
  }

  @Override
  public boolean canDeleteWhere(Predicate[] filters) {
    return delegate.canDeleteWhere(filters);
  }

  @Override
  public void deleteWhere(Predicate[] filters) {
    delegate.deleteWhere(filters);
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RapidsSparkTable) {
      RapidsSparkTable that = (RapidsSparkTable) other;
      // catalogName is part of identity: it drives the session-conf prefix used
      // to merge per-table scan overrides, so two wrappers with the same iceberg
      // delegate but different Spark catalog names produce different scans.
      return delegate.equals(that.delegate) &&
          Objects.equals(catalogName, that.catalogName);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(delegate, catalogName);
  }

  static CaseInsensitiveStringMap mergeSessionOptions(
      CaseInsensitiveStringMap options,
      String catalogName,
      String[] namespace,
      String tableName) {
    // getActiveSession returns the thread-local active SparkSession. Empty for
    // call sites that do not run inside a Spark session (unit tests, tooling) —
    // in that case we have no session conf to read, so just return the original
    // options unchanged.
    Option<SparkSession> sparkOpt = SparkSession.getActiveSession();
    if (sparkOpt.isEmpty()) {
      return options;
    }

    String[] components = new String[1 + namespace.length + 1];
    components[0] = catalogName;
    System.arraycopy(namespace, 0, components, 1, namespace.length);
    components[components.length - 1] = tableName;
    String tableKeyPrefix =
        TABLE_SETTING_PREFIX + String.join(".", components) + ".";
    String catalogKeyPrefix = CATALOG_SETTING_PREFIX + catalogName + ".";

    // Iterate the scala session conf once, classifying keys by scope. The
    // scope marker (table-setting / catalog-setting / global-setting) sits
    // immediately after CONF_PREFIX, so the three scopes occupy disjoint key
    // spaces. Per-scope overrides for a given table are 0-3 entries each, so
    // copying the entire SparkConf into a Java map would be wasteful.
    Map<String, String> tableConfs = new HashMap<>();
    Map<String, String> catalogConfs = new HashMap<>();
    Map<String, String> globalConfs = new HashMap<>();
    scala.collection.Iterator<scala.Tuple2<String, String>> it =
        sparkOpt.get().conf().getAll().iterator();
    while (it.hasNext()) {
      scala.Tuple2<String, String> entry = it.next();
      String key = entry._1();
      if (key.startsWith(tableKeyPrefix)) {
        tableConfs.put(key, entry._2());
      } else if (key.startsWith(catalogKeyPrefix)) {
        // Catalog-scoped: the part after catalogKeyPrefix must be a single
        // token (no '.'); otherwise the key isn't a well-formed catalog
        // setting for this table's catalog (and isn't a table-setting key
        // either, since those have their own scope marker), so skip it.
        if (key.indexOf('.', catalogKeyPrefix.length()) < 0) {
          catalogConfs.put(key, entry._2());
        }
      } else if (key.startsWith(GLOBAL_SETTING_PREFIX)) {
        // Global-scoped: same single-token rule applies.
        if (key.indexOf('.', GLOBAL_SETTING_PREFIX.length()) < 0) {
          globalConfs.put(key, entry._2());
        }
      }
    }

    // Pure pass-through when no conf is set at any scope — identifiers
    // containing '.' must not break scans for tables the user has not
    // explicitly tuned.
    if (tableConfs.isEmpty() && catalogConfs.isEmpty() && globalConfs.isEmpty()) {
      return options;
    }

    // Conf keys join catalog/namespace/table with '.', so a '.' in any of
    // those identifiers makes a table-scoped prefix ambiguous (catalog =
    // "hadoop.prod" + namespace=["ns"] and catalog="hadoop" +
    // namespace=["prod","ns"] both produce the same prefix). Catalog-scoped
    // and global-scoped confs don't span multiple identifier components, so
    // only fail when a table-scoped conf would actually apply.
    if (!tableConfs.isEmpty()) {
      String[] ambiguous = Arrays.stream(components)
          .filter(c -> c.indexOf('.') >= 0)
          .toArray(String[]::new);
      if (ambiguous.length > 0) {
        throw new IllegalArgumentException(
            "Cannot apply " + TABLE_SETTING_PREFIX + "* session overrides for " +
            catalogName + "." + String.join(".", namespace) + "." + tableName +
            ": identifier component(s) " + Arrays.toString(ambiguous) +
            " contain '.', which makes the conf prefix ambiguous. Rename the " +
            "identifier or set the iceberg read.split.* table property directly.");
      }
    }

    // Reject any conf whose suffix is not one of the recognized ones, so
    // misspelled / unsupported keys fail loudly instead of being a no-op.
    rejectUnrecognizedSuffixes(tableConfs, tableKeyPrefix);
    rejectUnrecognizedSuffixes(catalogConfs, catalogKeyPrefix);
    rejectUnrecognizedSuffixes(globalConfs, GLOBAL_SETTING_PREFIX);

    // Merge with precedence: table > catalog > global > unset (iceberg then
    // falls back to TBLPROPERTIES and its built-in default). Explicit
    // DataFrame options win over every scope.
    Map<String, String> merged = new HashMap<>(options.asCaseSensitiveMap());
    boolean changed = false;
    for (Map.Entry<String, String> entry : SUFFIX_TO_READ_OPTION.entrySet()) {
      String suffix = entry.getKey();
      String optionKey = entry.getValue();
      if (options.containsKey(optionKey)) {
        continue;
      }
      String value = tableConfs.get(tableKeyPrefix + suffix);
      if (value == null) {
        value = catalogConfs.get(catalogKeyPrefix + suffix);
      }
      if (value == null) {
        value = globalConfs.get(GLOBAL_SETTING_PREFIX + suffix);
      }
      if (value != null) {
        merged.put(optionKey, value);
        changed = true;
      }
    }
    return changed ? new CaseInsensitiveStringMap(merged) : options;
  }

  private static void rejectUnrecognizedSuffixes(
      Map<String, String> confs, String prefix) {
    for (String key : confs.keySet()) {
      String suffix = key.substring(prefix.length());
      if (!SUFFIX_TO_READ_OPTION.containsKey(suffix)) {
        throw new IllegalArgumentException(
            "Unrecognized suffix '" + suffix + "' in conf '" + key +
            "'. Supported suffixes: " + SUFFIX_TO_READ_OPTION.keySet() + ".");
      }
    }
  }
}
