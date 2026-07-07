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

package com.nvidia.spark.rapids.iceberg.parquet.staged;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import ai.rapids.cudf.HostMemoryBuffer;

/**
 * Executor-local-file implementation of {@link StagedParquetOutput}.
 *
 * <p>This is selected only when the exact host allocation fails. The task-owned file is exposed
 * through a writable memory mapping, so source workers can write disjoint ranges concurrently and
 * vectored reads can target their final offsets directly. This object owns only the mapping and
 * path; the file handle used to establish the exact length is closed during construction.</p>
 */
final class FileStagedParquetOutput extends StagedParquetOutput {
  private final Path path;
  private HostMemoryBuffer mappedBuffer;

  FileStagedParquetOutput(Path path, long exactSizeBytes) throws IOException {
    super(exactSizeBytes, true);
    this.path = Objects.requireNonNull(path, "path");
    boolean succeeded = false;
    try {
      try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw")) {
        file.setLength(exactSizeBytes);
      }
      mappedBuffer = HostMemoryBuffer.mapFile(
          path.toFile(), FileChannel.MapMode.READ_WRITE, 0L, exactSizeBytes);
      succeeded = true;
    } finally {
      if (!succeeded) {
        Files.deleteIfExists(path);
      }
    }
  }

  @Override
  HostMemoryBuffer writableBuffer() {
    return mappedBuffer;
  }

  @Override
  void sealStorage() {
    // GPU decode consumes an owning slice of this live mapping. Forcing dirty pages to disk here
    // would add a full-subtask barrier without making the temporary file more useful.
  }

  @Override
  HostMemoryBuffer materializeStorage() {
    // The owning slice keeps the mapping valid independently. On EMR/Linux the task-owned path
    // can therefore be unlinked when this output closes before GPU decode starts.
    return mappedBuffer.slice(0L, exactSizeBytes());
  }

  @Override
  void closeStorage() {
    Throwable failure = null;
    try {
      if (mappedBuffer != null) {
        mappedBuffer.close();
      }
    } catch (Throwable error) {
      failure = error;
    }
    mappedBuffer = null;
    try {
      Files.deleteIfExists(path);
    } catch (IOException ignored) {
      // Spark executor cleanup will remove its local directory if immediate deletion fails.
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
  }
}
