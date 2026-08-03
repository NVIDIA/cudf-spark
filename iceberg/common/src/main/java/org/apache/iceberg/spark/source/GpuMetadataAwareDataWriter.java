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

import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Bridges Spark 3.5 and Spark 4.0+ {@link DataWriter} metadata-write APIs.
 *
 * <p>Spark 4.0+ adds {@code write(metadata, record)} as a default interface method. Scala 2.13
 * requires {@code override} when implementing that method, but Scala 2.12 / Spark 3.5 rejects
 * {@code override} because the method does not exist yet. Declaring the two-arg {@code write} in
 * Java (without {@code @Override}) compiles against both Spark versions and still overrides the
 * Spark 4 default method at the JVM level.
 */
public abstract class GpuMetadataAwareDataWriter implements DataWriter<ColumnarBatch> {

  @Override
  public final void write(ColumnarBatch record) {
    write(null, record);
  }

  /**
   * Matches Spark 4.0+ {@code DataWriter.write(metadata, record)}. On Spark 3.5 this is an extra
   * method unused by the Spark 3.5 write path.
   */
  public void write(ColumnarBatch meta, ColumnarBatch record) {
    doWrite(meta, record);
  }

  /** Implementation of metadata-aware write. Takes ownership of {@code record}; closes {@code meta}. */
  protected abstract void doWrite(ColumnarBatch meta, ColumnarBatch record);
}
