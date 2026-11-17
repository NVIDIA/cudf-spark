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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import scala.collection.AbstractIterator;

/** Combine file metadata into batches bounded by row, byte, and variable-width column limits. */
final class MetadataBatchIterator extends AbstractIterator<MetadataBatch>
    implements AutoCloseable {
  private final Iterator<FileMetadata> files;
  private final MetadataReadContext context;
  private MetadataFileState currentFile;
  private long remainingRows;
  private boolean closed;

  MetadataBatchIterator(List<FileMetadata> files, MetadataReadContext context) {
    this.files = files.iterator();
    this.context = context;
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }
    while (currentFile == null && files.hasNext()) {
      FileMetadata file = files.next();
      if (file.rowCount() > 0) {
        currentFile = new MetadataFileState(file, context);
        remainingRows = file.rowCount();
      }
    }
    return currentFile != null;
  }

  @Override
  public MetadataBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more Iceberg metadata");
    }
    List<MetadataFileRows> parts = new ArrayList<>();
    int rows = 0;
    long bytes = 0;
    long[] leafBytes = new long[currentFile.leafSizes.length];
    while (rows < context.maxRows && hasNext()) {
      MetadataFileState file = currentFile;
      long byteRows = context.maxRows;
      if (file.rowBytes > 0) {
        byteRows = Math.max(0L, (context.maxBytes - bytes) / file.rowBytes);
        // Like the data readers, allow one row larger than the soft batch byte limit.
        if (rows == 0) {
          byteRows = Math.max(1L, byteRows);
        }
      }
      long columnRows = context.maxRows;
      for (int i = 0; i < leafBytes.length; i++) {
        if (file.leafSizes[i] > 0) {
          columnRows = Math.min(columnRows,
              (context.columnLimit - leafBytes[i]) / file.leafSizes[i]);
        }
      }
      int take = (int) Math.min(Math.min(remainingRows, context.maxRows - rows),
          Math.min(byteRows, columnRows));
      if (take == 0) {
        if (rows == 0) {
          throw new IllegalArgumentException(
              "Iceberg metadata value exceeds the GPU column size limit");
        }
        break;
      }
      // Identical projected constants share one materialization, including across files.
      int last = parts.size() - 1;
      if (last >= 0 && parts.get(last).file.sameOutput(file)) {
        MetadataFileRows previous = parts.get(last);
        parts.set(last, new MetadataFileRows(previous.file, previous.rows + take));
      } else {
        parts.add(new MetadataFileRows(file, take));
      }
      rows += take;
      bytes += file.rowBytes * take;
      for (int i = 0; i < leafBytes.length; i++) {
        leafBytes[i] += file.leafSizes[i] * take;
      }
      remainingRows -= take;
      if (remainingRows == 0) {
        currentFile = null;
      }
    }
    return new MetadataBatch(parts.toArray(new MetadataFileRows[0]));
  }

  @Override
  public void close() {
    closed = true;
    currentFile = null;
  }
}
