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

package com.nvidia.spark.rapids.iceberg.spark;

import com.nvidia.spark.rapids.iceberg.spark.source.RapidsSparkTable;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SparkTable;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.ViewCatalog;

/**
 * A drop-in replacement for {@link SparkSessionCatalog} that wraps every iceberg
 * {@link SparkTable} returned by {@code loadTable} with a {@link RapidsSparkTable}.
 * The wrapper augments scan options with session-level overrides keyed by
 * {@code spark.rapids.iceberg.<catalog>.<namespace>.<table>.}.
 *
 * <p>Configure via:
 * <pre>{@code
 *   spark.sql.catalog.spark_catalog =
 *     com.nvidia.spark.rapids.iceberg.spark.RapidsSparkSessionCatalog
 * }</pre>
 */
public class RapidsSparkSessionCatalog<T extends TableCatalog & FunctionCatalog
    & SupportsNamespaces & ViewCatalog>
    extends SparkSessionCatalog<T> {

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return wrap(name(), super.loadTable(ident));
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    return wrap(name(), super.loadTable(ident, version));
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    return wrap(name(), super.loadTable(ident, timestamp));
  }

  public static Table wrap(String catalogName, Table table) {
    if (table instanceof SparkTable) {
      return new RapidsSparkTable((SparkTable) table, catalogName);
    }
    return table;
  }
}
