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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.iceberg.spark;

import org.apache.iceberg.spark.SparkCatalog;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;

/**
 * A drop-in replacement for {@link SparkCatalog} that wraps every iceberg
 * {@link org.apache.iceberg.spark.source.SparkTable} returned by {@code loadTable}
 * with a {@link com.nvidia.spark.rapids.iceberg.spark.source.RapidsSparkTable}. The
 * wrapper augments scan options with session-level overrides keyed by
 * {@code spark.rapids.iceberg.<catalog>.<namespace>.<table>.}.
 *
 * <p>Configure via:
 * <pre>{@code
 *   spark.sql.catalog.<catalog> =
 *     com.nvidia.spark.rapids.iceberg.spark.RapidsSparkCatalog
 * }</pre>
 */
public class RapidsSparkCatalog extends SparkCatalog {

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return RapidsSparkSessionCatalog.wrap(name(), super.loadTable(ident));
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    return RapidsSparkSessionCatalog.wrap(name(), super.loadTable(ident, version));
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    return RapidsSparkSessionCatalog.wrap(name(), super.loadTable(ident, timestamp));
  }
}
