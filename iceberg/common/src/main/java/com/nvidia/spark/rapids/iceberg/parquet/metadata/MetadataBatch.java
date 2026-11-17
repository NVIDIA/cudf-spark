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

package com.nvidia.spark.rapids.iceberg.parquet.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.collection.JavaConverters;

import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM;

/** Pending work owns only metadata, so even a single file can be split on allocation failure. */
final class MetadataBatch implements AutoCloseable {
  final MetadataFileRows[] parts;
  final int rows;

  MetadataBatch(MetadataFileRows[] parts) {
    this.parts = parts;
    int total = 0;
    for (MetadataFileRows part : parts) {
      total += part.rows;
    }
    rows = total;
  }

  static scala.collection.immutable.List<MetadataBatch> split(MetadataBatch batch) {
    if (batch.rows <= 1) {
      throw new GpuSplitAndRetryOOM("Cannot split one metadata row");
    }
    int leftRows = batch.rows / 2;
    List<MetadataFileRows> left = new ArrayList<>();
    List<MetadataFileRows> right = new ArrayList<>();
    for (MetadataFileRows part : batch.parts) {
      int take = Math.min(leftRows, part.rows);
      if (take > 0) {
        left.add(new MetadataFileRows(part.file, take));
      }
      if (take < part.rows) {
        right.add(new MetadataFileRows(part.file, part.rows - take));
      }
      leftRows -= take;
    }
    return JavaConverters.asScalaBufferConverter(Arrays.asList(
        new MetadataBatch(left.toArray(new MetadataFileRows[0])),
        new MetadataBatch(right.toArray(new MetadataFileRows[0])))).asScala().toList();
  }

  @Override
  public void close() {
    // No GPU allocations are retained by a pending batch.
  }
}
