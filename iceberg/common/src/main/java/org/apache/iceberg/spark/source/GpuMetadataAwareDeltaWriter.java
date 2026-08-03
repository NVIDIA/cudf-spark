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

package org.apache.iceberg.spark.source;

import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Bridges Spark 3.5 and Spark 4.0+ {@link DeltaWriter} reinsert APIs.
 *
 * <p>Spark 4.0+ adds {@code reinsert(metadata, row)}. Scala cannot both {@code override} that
 * method (required on Spark 4 / Scala 2.13) and omit {@code override} (required on Spark 3.5).
 * Declaring {@code reinsert} in Java without {@code @Override} compiles against both and still
 * overrides the Spark 4 default method at the JVM level.
 */
public abstract class GpuMetadataAwareDeltaWriter implements DeltaWriter<ColumnarBatch> {

  @Override
  public final void insert(ColumnarBatch row) {
    reinsert(null, row);
  }

  /**
   * Matches Spark 4.0+ {@code DeltaWriter.reinsert(metadata, row)}. On Spark 3.5 this is an extra
   * method unused by the Spark 3.5 write path.
   */
  public void reinsert(ColumnarBatch meta, ColumnarBatch row) {
    doReinsert(meta, row);
  }

  /** Implementation of metadata-aware reinsert. Takes ownership of {@code row}; closes {@code meta}. */
  protected abstract void doReinsert(ColumnarBatch meta, ColumnarBatch row);
}
